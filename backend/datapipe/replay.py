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
  3. Initialize per-symbol state (ATR + rvol baseline). livestream table
     itself is emptied by pipeline.startup before we spawn.
  4. Load the chosen day's 1-min bars in one batched query, ordered by
     timestamp -- so multi-symbol replays stay cross-symbol time-aligned
     without any Python-side merging.
  5. For each bar, sleep ``(next.ts - current.ts) / speed`` seconds and
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
from datetime import date, datetime
from typing import Optional

import asyncpg

from backend.core.config import settings
from backend.database import rvol_baseline as rvol_baseline_db
from backend.database.readers import (
    load_intraday_bars_for_day,
    load_latest_atr_map,
    load_rvol_baseline_for_symbol,
)
from backend.datapipe.bar_processor import BarSink, process_bar
from backend.datapipe.rest_client import RestClient  # kept for pipeline signature compat
from backend.datapipe.schemas import MonitoredSymbols
from backend.datapipe.session_state import SessionStore

logger = logging.getLogger(__name__)


@dataclass
class ReplayConfig:
    day: date            # ET session date to replay
    speed: float         # 1.0 = wall clock, 60.0 = 1 real sec per replay minute, 0 = fastest
    # Optional -- when set, bars earlier than this UTC instant are skipped.
    # VWAP/EMA/RVOL start accumulating from this point, as if a trader
    # turned on the system at that moment.
    start_utc: Optional[datetime] = None

    # Baseline lookback / sample sessions come from ``settings`` (same
    # values the historian uses), so replay and historian can't drift.


# ---------------------------------------------------------------------------
# state initialization
# ---------------------------------------------------------------------------


async def _initialize_state_from_db(
    pool: asyncpg.Pool,
    store: SessionStore,
    cfg: ReplayConfig,
    symbol_map: MonitoredSymbols,
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
        "state initialized -- %d symbols missing ATR, %d missing rvol baseline",
        missing_atr, missing_baseline,
    )


# ---------------------------------------------------------------------------
# main loop
# ---------------------------------------------------------------------------


async def _consume(
    pool: asyncpg.Pool,
    store: SessionStore,
    timeline: list,
    speed: float,
    sink: Optional[BarSink],
) -> None:
    """
    Drain the timeline one bar at a time through ``process_bar``. Between
    bars, sleep ``(next.ts - current.ts) / speed`` seconds so multi-symbol
    replays stay cross-symbol time-aligned; ``speed = 0`` skips the sleep.
    Per-bar errors are logged and the loop continues.
    """
    prev_ts = None
    for bar in timeline:
        if prev_ts is not None and speed > 0:
            gap = (bar.ts - prev_ts).total_seconds() / speed
            if gap > 0:
                await asyncio.sleep(gap)
        prev_ts = bar.ts
        try:
            await process_bar(pool, store, bar, sink=sink)
        except Exception:
            logger.exception("process_bar failed for %s @ %s", bar.symbol, bar.ts)


async def run_replay(
    pool: asyncpg.Pool,
    rest: RestClient,  # unused here; kept for signature symmetry with run_livestream
    cfg: ReplayConfig,
    symbol_map: MonitoredSymbols,
    sink: Optional[BarSink] = None,
) -> None:
    """
    Rebuild baseline, initialize state, load today's timeline, then dispatch
    to ``_consume`` for the step-through. NO reconnect: any failure
    propagates to the caller.

    Caller (pipeline.startup) supplies ``symbol_map`` -- already validated
    non-empty there, so we don't re-check.
    """
    store = SessionStore()

    logger.info(
        "task started day=%s speed=%.2f lookback=%dd sample_sessions=%d",
        cfg.day, cfg.speed,
        settings.INTRADAY_BACKFILL_DAYS, settings.RVOL_SAMPLE_SESSIONS,
    )

    try:

        await rvol_baseline_db.rebuild(
            pool, end_day=cfg.day,
            lookback_days=settings.INTRADAY_BACKFILL_DAYS,
            sample_sessions=settings.RVOL_SAMPLE_SESSIONS,
        )
        await _initialize_state_from_db(pool, store, cfg, symbol_map)

        timeline = await load_intraday_bars_for_day(pool, cfg.day, symbol_map.values())
        if not timeline:
            logger.warning(
                "No rows in intraday_bars for day = %s",
                cfg.day,
            )
            return
        if cfg.start_utc is not None:
            timeline = [b for b in timeline if b.ts >= cfg.start_utc]
            if not timeline:
                logger.warning("No bars at or after start_utc -- nothing to play")
                return

        await _consume(pool, store, timeline, cfg.speed, sink)
        logger.info("replay complete")

    except asyncio.CancelledError:
        logger.info("Task cancelled -- exiting")
        raise
    except Exception:
        logger.exception("Replay failed -- task will exit")
        raise
