"""
Async DB writes for the datapipe.

All persistence goes through this module so the datapipe layer has no
direct SQL. Three tables get written from the live hot path:

  * ``livestream``      -- ephemeral today-only table with the four
                           indicator columns. Truncated at every session
                           boundary. Reader-friendly: strategies query the
                           last N rows here for state.
  * ``intraday_bars``   -- permanent partitioned history (5-day retention),
                           raw OHLCV only. Fed by the historian backfill.
  * ``daily``           -- historian only (never the live path).
"""

from __future__ import annotations

import logging
from collections import Counter
from datetime import date, datetime, time
from typing import Iterable, Optional, Sequence

import asyncpg

from backend.datapipe.schemas import Bar, DailyBar
from backend.datapipe.time_utils import to_helsinki

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


async def insert_livestream_bar(pool: asyncpg.Pool, bar: Bar) -> None:
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
    bars: Sequence[Bar],
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

    earliest = min(b.ts for b in bars)
    # Top-5 symbols by bar count -- quick visual on which tickers had the
    # longest already-occurred sessions when priming.
    counts = Counter(b.symbol for b in bars)
    top_str = ", ".join(f"{sym}={n}" for sym, n in counts.most_common(20))
    logger.info(
        "Livestream table initialized -- %d bars written, earliest ts= %s, top: %s",
        len(bars),
        to_helsinki(earliest).strftime("%Y-%m-%d %H:%M"),
        top_str,
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


async def bulk_insert_intraday_bars(
    pool: asyncpg.Pool,
    bars: Sequence[Bar],
) -> None:
    """
    Backfill bulk insert into intraday_bars.

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
        canonical values.
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
# daily (raw ingestion; historian only)
# ---------------------------------------------------------------------------


_INSERT_DAILY_SQL = """
    INSERT INTO daily
        (symbolid, date, open, high, low, close, volume)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (symbolid, date) DO UPDATE SET
        open   = EXCLUDED.open,
        high   = EXCLUDED.high,
        low    = EXCLUDED.low,
        close  = EXCLUDED.close,
        volume = EXCLUDED.volume;
"""


async def bulk_insert_daily_bars(
    pool: asyncpg.Pool,
    bars: Iterable[DailyBar],
) -> None:
    """
    Backfill bulk insert into ``daily`` (raw OHLCV only).

    Same COPY-into-temp pattern as bulk_insert_intraday_bars -- see that
    docstring for rationale. Derived fields (ATR, etc.) are written by
    ``bulk_insert_daily_indicators`` in a separate phase.
    """
    rows = [
        (b.symbolid, b.d, b.open, b.high, b.low, b.close, b.volume)
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
                    volume    bigint
                ) ON COMMIT DROP;
                """
            )
            await conn.copy_records_to_table(
                "_stage_daily",
                records=rows,
                columns=["symbolid", "date", "open", "high", "low", "close", "volume"],
            )
            await conn.execute(
                """
                INSERT INTO daily (symbolid, date, open, high, low, close, volume)
                SELECT symbolid, date, open, high, low, close, volume FROM _stage_daily
                ON CONFLICT (symbolid, date) DO UPDATE SET
                    open   = EXCLUDED.open,
                    high   = EXCLUDED.high,
                    low    = EXCLUDED.low,
                    close  = EXCLUDED.close,
                    volume = EXCLUDED.volume;
                """
            )


# ---------------------------------------------------------------------------
# daily_indicators (calculated; historian rewrites after raw daily inserts)
# ---------------------------------------------------------------------------


async def bulk_insert_daily_indicators(
    pool: asyncpg.Pool,
    rows: Sequence[tuple[int, date, float]],
) -> None:
    """
    Upsert calculated per-day indicators (currently just ATR14).

    ``rows`` is a sequence of ``(symbolid, date, atr)`` tuples. A fresh
    ATR14 computation from a wider window can legitimately produce a
    different value for the same date, so the DO UPDATE branch overwrites
    the previous value.
    """
    if not rows:
        return
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                CREATE TEMP TABLE _stage_daily_ind (
                    symbolid  integer,
                    date      date,
                    atr       numeric(12,4)
                ) ON COMMIT DROP;
                """
            )
            await conn.copy_records_to_table(
                "_stage_daily_ind",
                records=rows,
                columns=["symbolid", "date", "atr"],
            )
            await conn.execute(
                """
                INSERT INTO daily_indicators (symbolid, date, atr, updated)
                SELECT symbolid, date, atr, now() FROM _stage_daily_ind
                ON CONFLICT (symbolid, date) DO UPDATE SET
                    atr     = EXCLUDED.atr,
                    updated = EXCLUDED.updated;
                """
            )


# ---------------------------------------------------------------------------
# backfill_status  (persistent freshness ledger; one INSERT per historian run)
# ---------------------------------------------------------------------------


async def record_backfill_run(
    pool: asyncpg.Pool,
    *,
    daily_last_run:       Optional[datetime],
    intraday_last_run:    Optional[datetime]
) -> None:
    """
    Append one row to ``backfill_status`` describing this run.

    Pass ``None`` for whichever side was skipped in this run; row counts
    default to 0. Historian reads back ``MAX(daily_last_run)`` /
    ``MAX(intraday_last_run)`` to decide freshness on the next startup.
    """
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO backfill_status
                (daily_last_run, intraday_last_run)
            VALUES ($1, $2);
            """,
            daily_last_run, intraday_last_run
        )


# ---------------------------------------------------------------------------
# rvol_baseline (full rebuild -- wipe + bulk insert)
# ---------------------------------------------------------------------------


async def bulk_replace_rvol_baseline(
    pool: asyncpg.Pool,
    rows: Sequence[tuple[int, time, float, int]],
) -> None:
    """
    Wipe ``rvol_baseline`` and repopulate from ``rows``, atomically.

    ``rows`` items are ``(symbolid, bar_time, avg_volume, sample_days)``
    where ``bar_time`` is a naive ``datetime.time`` in Helsinki (Postgres
    stores it as ``timetz`` -- the offset is stamped server-side under a
    ``SET LOCAL TIME ZONE 'Europe/Helsinki'`` so the value reads back as
    e.g. ``09:30:00+03``).

    Called by ``datapipe.rvol_baseline.rebuild_rvol_model`` after
    ``calculations.compute_rvol_baseline`` has produced the full grid.
    """
    if not rows:
        return
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL TIME ZONE 'Europe/Helsinki';")
            await conn.execute("DELETE FROM rvol_baseline;")
            await conn.execute(
                """
                CREATE TEMP TABLE _stage_rvol (
                    symbolid    integer,
                    bar_time    time,
                    avg_volume  numeric(16,2),
                    sample_days smallint
                ) ON COMMIT DROP;
                """
            )
            await conn.copy_records_to_table(
                "_stage_rvol",
                records=rows,
                columns=["symbolid", "bar_time", "avg_volume", "sample_days"],
            )
            await conn.execute(
                """
                INSERT INTO rvol_baseline
                    (symbolid, bar_time, avg_volume, sample_days, updated)
                SELECT symbolid, bar_time::timetz, avg_volume, sample_days, now()
                  FROM _stage_rvol;
                """
            )
