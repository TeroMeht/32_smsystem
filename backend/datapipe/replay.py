"""
Replay mode.

Consumes ``intraday_bars`` rows already on disk (from the historian
backfill) and streams them through ``bar_processor.process_bar`` at a
user-controlled speed. Because ``process_bar`` is the same entry point
the livestream uses, replay produces byte-identical downstream state to
a live session for the chosen day.

Flow (all DB, no REST):

  1. Load the active monitored-symbol set.
  2. Rebuild ``rvol_baseline`` from the days STRICTLY BEFORE the replay
     day. The baseline can't include the replay day itself -- that would
     leak future information into RVOL.
  3. Truncate ``livestream`` (fresh session).
  4. Prime per-symbol state (ATR + rvol baseline).
  5. Load the chosen day's 1-min bars in one batched query, ordered by
     timestamp -- so multi-symbol replays stay cross-symbol time-aligned
     without any Python-side merging.
  6. For each bar, sleep ``(next.ts - current.ts) / speed`` seconds and
     dispatch through ``process_bar``. ``speed = 0`` means "as fast as
     possible"; ``speed = 60`` compresses one minute of trading into one
     wall-clock second.

Because the pipeline only reads from the DB, replay works offline (nights,
weekends) as long as the historian has previously loaded that day's bars.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import date
from typing import Optional

import asyncpg

from backend.database import rvol_baseline as rvol_baseline_db
from backend.database.readers import (
    load_active_symbol_map,
    load_intraday_bars_for_day,
    load_latest_atr_map,
    load_rvol_baseline_for_symbol,
)
from backend.database.writers import truncate_livestream
from backend.datapipe.bar_processor import BarSink, process_bar
from backend.datapipe.rest_client import RestClient  # kept for pipeline signature compat
from backend.datapipe.session_state import SessionStore

logger = logging.getLogger(__name__)


@dataclass
class ReplayConfig:
    day: date            # ET session date to replay
    speed: float = 1.0   # 1.0 = wall clock, 60.0 = 1 real sec per replay minute, 0 = fastest
    lookback_days: int = 8  # calendar-day window used to rebuild the baseline
    sample_sessions: int = 5  # trading sessions averaged into the baseline


# ---------------------------------------------------------------------------
# state priming
# ---------------------------------------------------------------------------


async def _prime_state_from_db(
    pool: asyncpg.Pool,
    store: SessionStore,
    cfg: ReplayConfig,
    symbol_map: dict[str, int],
) -> None:
    """Load ATR + rvol_baseline into per-symbol state before we feed bars."""
    atr_map = await load_latest_atr_map(pool)
    missing_atr = 0
    missing_baseline = 0
    for sym, sid in symbol_map.items():
        baseline = await load_rvol_baseline_for_symbol(pool, sid)
        st = store.get_or_init(sym, sid, cfg.day)
        st.atr = atr_map.get(sid)
        st.rvol_baseline = baseline
        if st.atr is None:
            missing_atr += 1
        if not baseline:
            missing_baseline += 1
    logger.info(
        "[replay] state primed -- %d symbols missing ATR, %d missing rvol baseline",
        missing_atr, missing_baseline,
    )


# ---------------------------------------------------------------------------
# main loop
# ---------------------------------------------------------------------------


async def run_replay(
    pool: asyncpg.Pool,
    rest: RestClient,  # unused now; kept so pipeline.startup_replay signature holds
    cfg: ReplayConfig,
    sink: Optional[BarSink] = None,
) -> None:
    """
    Read replay-day bars from ``intraday_bars`` and stream them through
    ``process_bar``. All work happens against the DB -- no REST calls.

    Cancelling this coroutine mid-replay is safe: each ``process_bar`` call
    is atomic and any bars already emitted stay in ``livestream``.
    """
    _ = rest  # deliberately unused

    logger.info(
        "[replay] ==> starting replay day=%s speed=%.2f lookback=%dd sample_sessions=%d",
        cfg.day, cfg.speed, cfg.lookback_days, cfg.sample_sessions,
    )

    symbol_map = await load_active_symbol_map(pool)
    if not symbol_map:
        logger.warning("[replay] no active monitored symbols -- aborting")
        return
    logger.info("[replay] %d active symbols", len(symbol_map))

    # 1. Rebuild baseline from sessions STRICTLY BEFORE the replay day.
    #    end_day is exclusive in rebuild(), so passing cfg.day is correct.
    logger.info("[replay] rebuilding rvol_baseline excluding replay day")
    await rvol_baseline_db.rebuild(
        pool, end_day=cfg.day,
        lookback_days=cfg.lookback_days,
        sample_sessions=cfg.sample_sessions,
    )

    # 2. Fresh livestream for this replay
    await truncate_livestream(pool)

    # 3. Prime in-memory state
    store = SessionStore()
    await _prime_state_from_db(pool, store, cfg, symbol_map)

    # 4. Load the day's bars in one shot, already sorted by ts across all symbols
    logger.info("[replay] loading intraday_bars for %s", cfg.day)
    timeline = await load_intraday_bars_for_day(pool, cfg.day, symbol_map.values())
    total = len(timeline)
    if total == 0:
        logger.warning(
            "[replay] no rows in intraday_bars for day=%s -- did the historian run for that day?",
            cfg.day,
        )
        return
    distinct_syms = len({b.symbolid for b in timeline})
    logger.info("[replay] timeline: %d bars across %d symbols (from ts=%s to %s)",
                total, distinct_syms, timeline[0].ts.isoformat(), timeline[-1].ts.isoformat())

    # 5. Step through
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
