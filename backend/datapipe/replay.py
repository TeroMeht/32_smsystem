"""
Replay mode.

User picks a past ET session date and a replay speed multiplier. For every
active monitored symbol we:

  1. Fetch that day's 1-min bars via REST (same shape the historian uses).
  2. Ensure partitions exist for the replay date and prior 5 sessions
     (for baseline continuity).
  3. Backfill the daily table for the replay date + prior 20 sessions so
     ATR14 for the replay date is realistic.
  4. Rebuild rvol_baseline from the days *before* the replay date so RVOL
     comparisons make sense (baseline can't include the replay day
     itself).
  5. Truncate ``livestream``.
  6. Merge everyone's bars into one time-ordered queue and feed them
     through ``bar_processor.process_bar`` one at a time -- so cross-symbol
     timing stays aligned. Between bars we sleep
     ``(next.ts - current.ts) / speed`` seconds; ``speed == 0`` means
     "as fast as the event loop allows".

Because process_bar is shared with livestream, everything downstream
(strategy layer, SSE events) sees a replay day exactly as it would see
a live day.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

import asyncpg

from backend.database import rvol_baseline as rvol_baseline_db
from backend.database.partitions import (
    drop_old_partitions,
    ensure_partitions_for_dates,
)
from backend.database.readers import (
    load_active_symbol_map,
    load_latest_atr_map,
    load_rvol_baseline_for_symbol,
)
from backend.database.writers import (
    bulk_insert_daily_bars,
    bulk_insert_intraday_bars,
    truncate_livestream,
)
from backend.datapipe.bar_processor import BarSink, process_bar
from backend.datapipe.historian import (
    _backfill_daily_for_symbol,
    _backfill_intraday_for_symbol,
)
from backend.datapipe.rest_client import RestClient
from backend.datapipe.schemas import Bar1m
from backend.datapipe.session_state import SessionStore

logger = logging.getLogger(__name__)


@dataclass
class ReplayConfig:
    day: date            # ET session date to replay
    speed: float = 1.0   # 1.0 = wall clock, 60.0 = 1 real sec per replay minute, 0 = fastest
    lookback_days: int = 5  # sessions to warm baseline before replay day


# ---------------------------------------------------------------------------
# preparation
# ---------------------------------------------------------------------------


async def _prepare_replay_data(
    pool: asyncpg.Pool,
    rest: RestClient,
    cfg: ReplayConfig,
    symbol_map: dict[str, int],
) -> dict[str, list[Bar1m]]:
    """
    Fetch replay-day bars for every symbol, and also warm the DB with the
    daily + intraday history the replay day needs. Returns ``{symbol: bars}``
    for the replay-day bars (in chronological order).
    """
    # 1. Retention prune + partition creation for the replay window
    await drop_old_partitions(pool, cfg.day)
    intraday_days = [cfg.day - timedelta(days=i) for i in range(cfg.lookback_days + 1)]
    daily_days = [cfg.day - timedelta(days=i) for i in range(20)]
    await ensure_partitions_for_dates(pool, intraday_days, daily_days)

    # 2. Backfill DAILY history up to (and including) the replay day so ATR14 is accurate
    #    but leave the replay day itself out of rvol_baseline (rebuild step below).
    async def _warm_daily(symbol: str, symbolid: int) -> None:
        daily = await _backfill_daily_for_symbol(rest, symbol, symbolid, cfg.day)
        if daily:
            await bulk_insert_daily_bars(pool, daily)

    async def _warm_intraday_prev(symbol: str, symbolid: int) -> None:
        # prior 5 sessions ONLY -- replay-day bars are fetched separately
        prior_end = cfg.day - timedelta(days=1)
        prior_start = cfg.day - timedelta(days=cfg.lookback_days)
        prev = await _backfill_intraday_for_symbol(
            rest, symbol, symbolid, prior_start, prior_end,
        )
        if prev:
            await bulk_insert_intraday_bars(pool, prev)

    sem = asyncio.Semaphore(10)

    async def _work(symbol: str, symbolid: int) -> tuple[str, list[Bar1m]]:
        async with sem:
            await asyncio.gather(
                _warm_daily(symbol, symbolid),
                _warm_intraday_prev(symbol, symbolid),
            )
            # Now fetch the replay-day itself
            replay_bars = await _backfill_intraday_for_symbol(
                rest, symbol, symbolid, cfg.day, cfg.day,
            )
            return symbol, replay_bars

    results = await asyncio.gather(*[
        _work(s, sid) for s, sid in symbol_map.items()
    ])

    # 3. rvol_baseline from the prior sessions ONLY (replay day excluded)
    await rvol_baseline_db.rebuild(
        pool, end_day=cfg.day,
        lookback_days=cfg.lookback_days,
        sample_sessions=5,
    )

    # 4. Fresh livestream for the replay
    await truncate_livestream(pool)

    return {sym: bars for sym, bars in results if bars}


# ---------------------------------------------------------------------------
# step-through engine
# ---------------------------------------------------------------------------


def _merge_timeline(
    bars_per_symbol: dict[str, list[Bar1m]],
) -> list[Bar1m]:
    """Flatten to one chronologically-sorted list across all symbols."""
    everything: list[Bar1m] = []
    for bars in bars_per_symbol.values():
        everything.extend(bars)
    everything.sort(key=lambda b: b.ts)
    return everything


async def _prime_state_from_db(
    pool: asyncpg.Pool,
    store: SessionStore,
    cfg: ReplayConfig,
    symbol_map: dict[str, int],
) -> None:
    """Load ATR + rvol_baseline into per-symbol state before we start feeding bars."""
    atr_map = await load_latest_atr_map(pool)
    for sym, sid in symbol_map.items():
        baseline = await load_rvol_baseline_for_symbol(pool, sid)
        st = store.get_or_init(sym, sid, cfg.day)
        st.atr = atr_map.get(sid)
        st.rvol_baseline = baseline


async def run_replay(
    pool: asyncpg.Pool,
    rest: RestClient,
    cfg: ReplayConfig,
    sink: Optional[BarSink] = None,
) -> None:
    """
    End-to-end replay:  prepare data -> merge timeline -> feed bars ->
    step-sleep between bars scaled by speed.

    Cancelling this coroutine mid-replay is safe -- ``process_bar`` writes
    are atomic per bar and any consumed bars stay in the DB.
    """
    logger.info("[replay] ==> starting replay day=%s speed=%.2f lookback=%dd",
                cfg.day, cfg.speed, cfg.lookback_days)

    symbol_map = await load_active_symbol_map(pool)
    if not symbol_map:
        logger.warning("[replay] no active monitored symbols -- aborting")
        return

    logger.info("[replay] preparing data for %d symbols", len(symbol_map))
    bars_per_symbol = await _prepare_replay_data(pool, rest, cfg, symbol_map)
    if not bars_per_symbol:
        logger.warning("[replay] no bars returned for day=%s -- nothing to play", cfg.day)
        return
    logger.info("[replay] data prepared -- %d symbols have replay bars", len(bars_per_symbol))

    store = SessionStore()
    await _prime_state_from_db(pool, store, cfg, symbol_map)
    logger.info("[replay] session state primed")

    timeline = _merge_timeline(bars_per_symbol)
    total = len(timeline)
    logger.info("[replay] timeline built: %d bars across %d symbols", total, len(bars_per_symbol))

    progress_step = max(1, total // 20)  # every ~5%
    errors = 0
    prev_ts = None
    for i, bar in enumerate(timeline):
        if prev_ts is not None and cfg.speed > 0:
            gap = (bar.ts - prev_ts).total_seconds() / cfg.speed
            if gap > 0:
                await asyncio.sleep(gap)
        prev_ts = bar.ts
        try:
            await process_bar(pool, store, bar, sink=sink)
        except Exception:
            errors += 1
            logger.exception("[replay] process_bar failed for %s @ %s", bar.symbol, bar.ts)

        if i and i % progress_step == 0:
            pct = 100 * i / total
            logger.info("[replay] progress: %d / %d bars (%.1f%%) errors=%d ts=%s",
                        i, total, pct, errors, bar.ts.isoformat())

    logger.info("[replay] ==> complete (%d bars, %d errors)", total, errors)
