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

from backend.database.readers import load_intraday_bars_for_day
from backend.datapipe.runtime.bar_processor import BarSink, process_bar
from backend.datapipe.runtime.priming import (
    bulk_persist_bars,
    enrich_prime_bars,
    seed_session_state,
)
from backend.datapipe.runtime.session_state import SessionStore
from backend.datapipe.schemas import Bar, MonitoredSymbols
from backend.datapipe.time_utils import to_helsinki
from backend.dependencies import RestClient

logger = logging.getLogger(__name__)


@dataclass
class ReplayConfig:
    day: date            
    speed: float        
    start_utc: Optional[datetime] = None


# ---------------------------------------------------------------------------
# state initialization
# ---------------------------------------------------------------------------


def _group_prefix_by_symbol(
    prefix: list[Bar],
) -> list[tuple[str, int, list[Bar]]]:
    """Reshape the flat prefix into what ``enrich_and_bulk_persist`` expects."""
    by_sym: dict[tuple[str, int], list[Bar]] = {}
    for bar in prefix:
        by_sym.setdefault((bar.symbol, bar.symbolid), []).append(bar)
    return [(sym, sid, bars) for (sym, sid), bars in by_sym.items()]


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
        await seed_session_state(pool, store, symbol_map, cfg.day)

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

        if prefix:
            enriched = enrich_prime_bars(store, _group_prefix_by_symbol(prefix))
            written = await bulk_persist_bars(pool, enriched)
            logger.info("Primed livestream with %d prefix bars (session=%s)",
                        written, cfg.day.isoformat())

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
