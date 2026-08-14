"""
Replay mode.

Consumes ``intraday_bars`` rows already on disk (from the historian
backfill) and streams them through ``bar_processor.process_bar`` at a
user-controlled speed. Because ``process_bar`` is the same entry point
the livestream uses, replay produces byte-identical downstream state to
a live session for the chosen day.

Flow (all DB, no REST):

  1. Load the active monitored-symbol set (from pipeline.startup).
  2. Initialize per-symbol state (ATR + rvol baseline) from DB.
  3. Load the chosen day's bars in one batched query, ordered by
     timestamp -- so multi-symbol replays stay cross-symbol time-aligned
     without any Python-side merging.
  4. If REPLAY_START_TIME is set, split at that instant: bars before
     prime livestream + state (mirrors live's REST prime), bars at/after
     are streamed through the consumer.
  5. For each streamed bar, sleep ``(next.ts - current.ts) / speed``
     seconds and dispatch through ``process_bar``. ``speed = 0`` means
     "as fast as possible"; ``speed = 60`` compresses one minute of
     trading into one wall-clock second.

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

from backend.database.readers import (
    load_intraday_bars_for_day,
    load_latest_atr_map,
    load_rvol_baseline_for_symbol,
)
from backend.database.writers import bulk_insert_livestream_bars
from backend.datapipe.runtime.bar_processor import BarSink, process_bar,enrich_bar
from backend.datapipe.schemas import Bar1m, MonitoredSymbols
from backend.datapipe.runtime.session_state import SessionStore
from backend.dependencies import RestClient
from backend.datapipe.time_utils import helsinki_time_slot, to_helsinki

logger = logging.getLogger(__name__)


@dataclass
class ReplayConfig:
    day: date            
    speed: float        
    start_utc: Optional[datetime] = None


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


async def _initialize_livestream_from_prefix(
    pool: asyncpg.Pool,
    store: SessionStore,
    prefix: list[Bar1m],
    session_date: date,
) -> None:
    """
    Enrich pre-cutoff bars in ts order, append to per-symbol session
    state, and bulk-insert the enriched result into ``livestream``.

    Mirrors ``livestream._initialize_livestream`` semantics -- the only
    difference is the source (rows already on disk in ``intraday_bars``
    instead of a REST fetch). Result: /relatr sees a fully-populated
    livestream table the instant startup finishes, and per-symbol VWAP /
    EMA / RVOL accumulators are seeded with the whole session so far.
    """
    if not prefix:
        return
    all_enriched: list[Bar1m] = []
    for bar in prefix:
        st = store.get_or_init(bar.symbol, bar.symbolid, session_date)
        slot = helsinki_time_slot(bar.ts)
        slot_avg = st.baseline_for_slot(slot)
        enriched = enrich_bar(
            new_bar=bar,
            history=st.history,
            atr=st.atr,
            baseline_slot_avg=slot_avg,
            baseline_history_sum=st.baseline_history_sum,
        )
        st.history.append(enriched)
        st.baseline_history_sum += slot_avg
        all_enriched.append(enriched)

    await bulk_insert_livestream_bars(pool, all_enriched)
    logger.info(
        "Primed livestream with %d prefix bars (session=%s)",
        len(all_enriched), session_date.isoformat(),
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

    Log signals:
      * one info line per consumed bar -- OHLC + VWAP, same shape as the
        live consumer's first-bar line.
    """
    total = len(timeline)
    logger.info("Draining %d bars (speed= %.2f)", total, speed)

    prev_ts = None
    processed = 0

    for bar in timeline:
        if prev_ts is not None and speed > 0:
            gap = (bar.ts - prev_ts).total_seconds() / speed
            if gap > 0:
                await asyncio.sleep(gap)
        prev_ts = bar.ts

        try:
            await process_bar(pool, store, bar, sink=sink)
            processed += 1
            logger.info(
                "%s %s | O=%.4f H=%.4f L=%.4f C=%.4f V=%d",
                bar.symbol,
                to_helsinki(bar.ts).strftime("%H:%M"),
                bar.open, bar.high, bar.low, bar.close, bar.volume,
            )
        except Exception:
            logger.exception("process_bar failed for %s @ %s", bar.symbol, bar.ts)

    logger.info("Drain finished -- processed %d/%d bars", processed, total)


async def run_replay(
    pool: asyncpg.Pool,
    rest: RestClient,  # unused here; kept for signature symmetry with run_livestream
    cfg: ReplayConfig,
    symbol_map: MonitoredSymbols,
    sink: Optional[BarSink] = None,
) -> None:
    """
    Initialize state, load the day's timeline, then dispatch to ``_consume``
    for the step-through. NO reconnect: any failure propagates to the caller.

    Caller (pipeline.startup) supplies ``symbol_map`` -- already validated
    non-empty there, so we don't re-check.

    RVOL baseline is NOT rebuilt here -- the historian owns that as part of
    the daily backfill, so replay just uses whatever's on disk.
    """
    store = SessionStore()

    logger.info("Replay started day= %s speed= %.2f", cfg.day, cfg.speed)

    try:
        await _initialize_state_from_db(pool, store, cfg, symbol_map)

        timeline = await load_intraday_bars_for_day(pool, cfg.day, symbol_map.values())
        if not timeline:
            logger.warning("No rows in intraday_bars for day = %s", cfg.day)
            return

        # Split at start_utc: prefix primes livestream + state (so /relatr
        # is populated immediately), tail is streamed through the consumer.
        if cfg.start_utc is not None:
            prefix = [b for b in timeline if b.ts < cfg.start_utc]
            tail   = [b for b in timeline if b.ts >= cfg.start_utc]
        else:
            prefix, tail = [], timeline

        await _initialize_livestream_from_prefix(pool, store, prefix, cfg.day)

        if not tail:
            logger.warning(
                "No bars at or after start_utc -- livestream primed, nothing to stream",
            )
            return

        await _consume(pool, store, tail, cfg.speed, sink)
        logger.info("replay complete")

    except asyncio.CancelledError:
        logger.info("Task cancelled -- exiting")
        raise
    except Exception:
        logger.exception("Replay failed -- task will exit")
        raise
