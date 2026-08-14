"""
Daily-partition management for ``intraday_bars``, ``daily``, and
``daily_indicators``.

Schema declares all three tables PARTITION BY RANGE (ts / date). Every
session-day needs its own partition; retention is 5 trading days for
intraday and 14 for daily / daily_indicators. This module creates
today's partitions on startup and drops the ones older than the
retention window.

Design note: partition names use the ``YYYYMMDD`` suffix already used in
``schema.sql`` (e.g. ``intraday_bars_20260807``). We rebuild names from
``date`` values so the caller only ever passes a ``date``.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Iterable

import asyncpg

from backend.core.config import settings

logger = logging.getLogger(__name__)


def _part_name(base: str, d: date) -> str:
    return f"{base}_{d.strftime('%Y%m%d')}"


async def ensure_partition_intraday(pool: asyncpg.Pool, d: date) -> None:
    """CREATE the intraday_bars partition for ``d`` if missing."""
    name = _part_name("intraday_bars", d)
    end = d + timedelta(days=1)
    sql = (
        f"CREATE TABLE IF NOT EXISTS {name} PARTITION OF intraday_bars "
        f"FOR VALUES FROM ('{d.isoformat()}') TO ('{end.isoformat()}');"
    )
    async with pool.acquire() as conn:
        await conn.execute(sql)
    logger.debug("ensured %s", name)


async def ensure_partition_daily(pool: asyncpg.Pool, d: date) -> None:
    name = _part_name("daily", d)
    end = d + timedelta(days=1)
    sql = (
        f"CREATE TABLE IF NOT EXISTS {name} PARTITION OF daily "
        f"FOR VALUES FROM ('{d.isoformat()}') TO ('{end.isoformat()}');"
    )
    async with pool.acquire() as conn:
        await conn.execute(sql)
    logger.debug("ensured %s", name)


async def ensure_partition_daily_indicators(pool: asyncpg.Pool, d: date) -> None:
    """CREATE the daily_indicators partition for ``d`` if missing."""
    name = _part_name("daily_indicators", d)
    end = d + timedelta(days=1)
    sql = (
        f"CREATE TABLE IF NOT EXISTS {name} PARTITION OF daily_indicators "
        f"FOR VALUES FROM ('{d.isoformat()}') TO ('{end.isoformat()}');"
    )
    async with pool.acquire() as conn:
        await conn.execute(sql)
    logger.debug("ensured %s", name)


async def ensure_partitions_for_dates(
    pool: asyncpg.Pool,
    intraday_dates: Iterable[date],
    daily_dates: Iterable[date],
) -> None:
    """
    Batch helper called by the historian before bulk-inserting bars.
    ``daily_dates`` covers both the raw ``daily`` and the derived
    ``daily_indicators`` partitions (same partition grid).
    """
    intra = sorted(set(intraday_dates))
    daily = sorted(set(daily_dates))
    for d in intra:
        await ensure_partition_intraday(pool, d)
    for d in daily:
        await ensure_partition_daily(pool, d)
        await ensure_partition_daily_indicators(pool, d)
    logger.debug("ensured %d intraday_bars + %d daily/daily_indicators partitions",
                len(intra), len(daily))


async def data_cleanup(pool: asyncpg.Pool, today: date) -> None:
    """
    Drop intraday data older than ``today - settings.INTRADAY_BACKFILL_DAYS``
    and daily / daily_indicators data older than
    ``today - settings.DAILY_BACKFILL_DAYS``.

    Retention window equals the backfill window -- whatever we fetch is
    kept, and everything older gets purged. Implemented under the hood
    by dropping the day-partition tables (fast, no row-by-row delete);
    safe to run on every startup, missing tables are ignored.
    """
    # Retention semantics: "N-day window" means we keep N dates inclusive
    # of today -- so the earliest kept date is today - (N - 1), and
    # anything strictly before that gets dropped.
    intraday_cutoff = today - timedelta(days=settings.INTRADAY_BACKFILL_DAYS - 1)
    daily_cutoff    = today - timedelta(days=settings.DAILY_BACKFILL_DAYS    - 1)
    dropped: list[str] = []

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT c.relname
              FROM pg_inherits i
              JOIN pg_class c ON c.oid = i.inhrelid
              JOIN pg_class p ON p.oid = i.inhparent
             WHERE p.relname IN ('intraday_bars', 'daily', 'daily_indicators')
            """
        )
        for r in rows:
            name = r["relname"]
            try:
                suffix = name.rsplit("_", 1)[-1]
                part_date = date(int(suffix[0:4]), int(suffix[4:6]), int(suffix[6:8]))
            except (ValueError, IndexError):
                logger.warning("Skipping table with unparseable date suffix: %s", name)
                continue

            # intraday_bars keeps the tighter window; daily + daily_indicators
            # share the daily-backfill retention.
            cutoff = intraday_cutoff if name.startswith("intraday_bars") else daily_cutoff
            if part_date < cutoff:
                await conn.execute(f"DROP TABLE IF EXISTS {name};")
                dropped.append(name)
                logger.debug("Cleaned up %s (before %s)", name, cutoff)

    if dropped:
        logger.info(
            "Data cleanup: purged %d day(s): %s",
            len(dropped), ", ".join(sorted(dropped)),
        )
    else:
        logger.info("Data cleanup: no old data dropped")
