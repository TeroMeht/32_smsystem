"""
Async DB writes for the datapipe.

All persistence goes through this module so the datapipe layer has no
direct SQL. Three tables get written from the live/replay hot path:

  * ``livestream``      -- ephemeral today-only table with the four
                           indicator columns. Truncated at every session
                           boundary. Reader-friendly: strategies query the
                           last N rows here for state.
  * ``intraday_bars``   -- permanent partitioned history (5-day retention),
                           raw OHLCV only. Fed by both live and replay
                           paths so replay data is indistinguishable from
                           live once written.
  * ``daily``           -- historian only (never the live path).
"""

from __future__ import annotations

import logging
from typing import Iterable, Sequence

import asyncpg

from backend.datapipe.schemas import Bar1m, DailyBar

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# livestream (today only, ephemeral)
# ---------------------------------------------------------------------------


_INSERT_LIVESTREAM_SQL = """
    INSERT INTO livestream
        (symbolid, ts, open, high, low, close, volume, vwap, ema9, rvol_cum, relatr)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    ON CONFLICT (symbolid, ts) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        vwap = EXCLUDED.vwap,
        ema9 = EXCLUDED.ema9,
        rvol_cum = EXCLUDED.rvol_cum,
        relatr = EXCLUDED.relatr;
"""


async def insert_livestream_bar(pool: asyncpg.Pool, bar: Bar1m) -> None:
    """Upsert one enriched bar into livestream (indicators required)."""
    async with pool.acquire() as conn:
        await conn.execute(
            _INSERT_LIVESTREAM_SQL,
            bar.symbolid, bar.ts,
            bar.open, bar.high, bar.low, bar.close, bar.volume,
            bar.vwap, bar.ema9, bar.rvol_cum, bar.relatr,
        )


async def truncate_livestream(pool: asyncpg.Pool) -> None:
    """Called at session boundary / process start so livestream = today only."""
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE TABLE livestream;")
    logger.info("truncated livestream")


# ---------------------------------------------------------------------------
# intraday_bars (permanent, partitioned)
# ---------------------------------------------------------------------------


_INSERT_INTRADAY_SQL = """
    INSERT INTO intraday_bars
        (symbolid, ts, open, high, low, close, volume)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (symbolid, ts) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume;
"""


async def insert_intraday_bar(pool: asyncpg.Pool, bar: Bar1m) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            _INSERT_INTRADAY_SQL,
            bar.symbolid, bar.ts,
            bar.open, bar.high, bar.low, bar.close, bar.volume,
        )


async def bulk_insert_intraday_bars(
    pool: asyncpg.Pool,
    bars: Sequence[Bar1m],
) -> None:
    """
    Historian and replay-warmup path. copy_records_to_table would be faster
    but partitioned tables historically had quirks with it; executemany is
    plenty fast for backfill volumes we're dealing with (single-day bars
    per symbol = a few hundred rows).
    """
    if not bars:
        return
    rows = [
        (b.symbolid, b.ts, b.open, b.high, b.low, b.close, b.volume)
        for b in bars
    ]
    async with pool.acquire() as conn:
        await conn.executemany(_INSERT_INTRADAY_SQL, rows)


# ---------------------------------------------------------------------------
# daily (historian only)
# ---------------------------------------------------------------------------


_INSERT_DAILY_SQL = """
    INSERT INTO daily
        (symbolid, date, open, high, low, close, volume, atr)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    ON CONFLICT (symbolid, date) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        atr = EXCLUDED.atr;
"""


async def bulk_insert_daily_bars(
    pool: asyncpg.Pool,
    bars: Iterable[DailyBar],
) -> None:
    rows = [
        (b.symbolid, b.d, b.open, b.high, b.low, b.close, b.volume, b.atr)
        for b in bars
    ]
    if not rows:
        return
    async with pool.acquire() as conn:
        await conn.executemany(_INSERT_DAILY_SQL, rows)
