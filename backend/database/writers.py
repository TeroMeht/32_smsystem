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


async def bulk_insert_livestream_bars(
    pool: asyncpg.Pool,
    bars: Sequence[Bar1m],
) -> None:
    """
    Bulk-insert already-enriched bars into livestream. Used by the
    livestream priming step at boot: today's already-occurred REST bars
    are enriched (VWAP/EMA9/RelATR/RVOL) in one pass and dropped in via
    COPY -> temp -> INSERT ON CONFLICT for speed.

    Same COPY-into-temp pattern as bulk_insert_intraday_bars. Duplicate
    (symbolid, ts) rows update indicator columns, matching the live
    single-row upsert.
    """
    if not bars:
        return
    rows = [
        (b.symbolid, b.ts,
         b.open, b.high, b.low, b.close, b.volume,
         b.vwap, b.ema9, b.rvol_cum, b.relatr)
        for b in bars
    ]
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                CREATE TEMP TABLE _stage_livestream (
                    symbolid  integer,
                    ts        timestamptz,
                    open      numeric(12,4),
                    high      numeric(12,4),
                    low       numeric(12,4),
                    close     numeric(12,4),
                    volume    bigint,
                    vwap      numeric(12,4),
                    ema9      numeric(12,4),
                    rvol_cum  numeric(8,4),
                    relatr    numeric(8,4)
                ) ON COMMIT DROP;
                """
            )
            await conn.copy_records_to_table(
                "_stage_livestream",
                records=rows,
                columns=["symbolid", "ts", "open", "high", "low", "close", "volume",
                         "vwap", "ema9", "rvol_cum", "relatr"],
            )
            await conn.execute(
                """
                INSERT INTO livestream
                    (symbolid, ts, open, high, low, close, volume,
                     vwap, ema9, rvol_cum, relatr)
                SELECT symbolid, ts, open, high, low, close, volume,
                       vwap, ema9, rvol_cum, relatr
                  FROM _stage_livestream
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
            )


async def empty_livestream_table(pool: asyncpg.Pool) -> None:
    """
    Wipe the livestream table so priming re-populates it from a clean slate.
    Called at session boundary and on every process start.
    """
    async with pool.acquire() as conn:
        before = await conn.fetchval("SELECT COUNT(*) FROM livestream;")
        await conn.execute("TRUNCATE TABLE livestream;")
    logger.debug("emptied livestream (was %d rows)", before)


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
    Backfill/replay bulk insert into intraday_bars.

    Uses COPY into a per-connection TEMP staging table, then a single
    ``INSERT ... SELECT ... ON CONFLICT DO NOTHING`` from staging into the
    partitioned target. Rationale:

      * ``executemany`` with ``ON CONFLICT DO UPDATE`` serializes on
        partition-level locks when many connections write concurrently --
        that's what pinned all 10 historian workers on ``intraday_insert``
        for 90+s in earlier runs.
      * COPY moves rows in a single protocol frame; the follow-up INSERT
        does one plan + one execution.
      * DO NOTHING is safe here because Massive's historical bars are
        immutable -- a duplicate (symbolid, ts) means we already have the
        canonical values. The live path (insert_intraday_bar) still uses
        DO UPDATE for late corrections.
    """
    if not bars:
        return
    rows = [
        (b.symbolid, b.ts, b.open, b.high, b.low, b.close, b.volume)
        for b in bars
    ]
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                CREATE TEMP TABLE _stage_intraday (
                    symbolid  integer,
                    ts        timestamptz,
                    open      numeric(12,4),
                    high      numeric(12,4),
                    low       numeric(12,4),
                    close     numeric(12,4),
                    volume    bigint
                ) ON COMMIT DROP;
                """
            )
            await conn.copy_records_to_table(
                "_stage_intraday",
                records=rows,
                columns=["symbolid", "ts", "open", "high", "low", "close", "volume"],
            )
            await conn.execute(
                """
                INSERT INTO intraday_bars (symbolid, ts, open, high, low, close, volume)
                SELECT symbolid, ts, open, high, low, close, volume FROM _stage_intraday
                ON CONFLICT (symbolid, ts) DO NOTHING;
                """
            )


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
    """
    Backfill bulk insert into daily. Same COPY-into-temp pattern as
    bulk_insert_intraday_bars -- see that docstring for rationale.

    ATR is included in the DO UPDATE branch because a fresh ATR14
    computation from a wider window can legitimately produce a different
    value for the same date, and we want the newest one.
    """
    rows = [
        (b.symbolid, b.d, b.open, b.high, b.low, b.close, b.volume, b.atr)
        for b in bars
    ]
    if not rows:
        return
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                CREATE TEMP TABLE _stage_daily (
                    symbolid  integer,
                    date      date,
                    open      numeric(12,4),
                    high      numeric(12,4),
                    low       numeric(12,4),
                    close     numeric(12,4),
                    volume    bigint,
                    atr       numeric(12,4)
                ) ON COMMIT DROP;
                """
            )
            await conn.copy_records_to_table(
                "_stage_daily",
                records=rows,
                columns=["symbolid", "date", "open", "high", "low", "close", "volume", "atr"],
            )
            await conn.execute(
                """
                INSERT INTO daily (symbolid, date, open, high, low, close, volume, atr)
                SELECT symbolid, date, open, high, low, close, volume, atr FROM _stage_daily
                ON CONFLICT (symbolid, date) DO UPDATE SET
                    atr = EXCLUDED.atr;
                """
            )
