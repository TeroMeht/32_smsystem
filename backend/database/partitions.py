"""
Daily-partition management for ``intraday_bars`` and ``daily``.

Schema declares both tables PARTITION BY RANGE (ts) / (date). Every
session-day needs its own partition; retention is 5 days for intraday and
14 days for daily. This module creates today's partitions on startup and
drops the ones older than the retention window.

Design note: partition names use the ``YYYYMMDD`` suffix already used in
``schema.sql`` (e.g. ``intraday_bars_20260807``). We rebuild names from
``date`` values so the caller only ever passes a ``date``.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Iterable

import asyncpg

logger = logging.getLogger(__name__)


INTRADAY_RETENTION_DAYS = 8   # 8 calendar days guarantees 5 trading sessions
DAILY_RETENTION_DAYS = 14


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


async def ensure_partitions_for_dates(
    pool: asyncpg.Pool,
    intraday_dates: Iterable[date],
    daily_dates: Iterable[date],
) -> None:
    """Batch helper called by the historian before bulk-inserting bars."""
    intra = sorted(set(intraday_dates))
    daily = sorted(set(daily_dates))
    for d in intra:
        await ensure_partition_intraday(pool, d)
    for d in daily:
        await ensure_partition_daily(pool, d)
    logger.info("ensured %d intraday_bars + %d daily partitions",
                len(intra), len(daily))


async def drop_old_partitions(pool: asyncpg.Pool, today: date) -> None:
    """
    Drop intraday partitions older than ``today - INTRADAY_RETENTION_DAYS``
    and daily partitions older than ``today - DAILY_RETENTION_DAYS``.

    Retention is enforced by dropping the partition table entirely (fast --
    no row-by-row delete). Safe to run on every startup; missing tables are
    simply ignored (IF EXISTS).
    """
    # Retention semantics: "N-day retention" means we keep N dates
    # inclusive of today -- so the earliest kept partition is
    # today - (N - 1), and anything strictly before that gets dropped.
    intraday_cutoff = today - timedelta(days=INTRADAY_RETENTION_DAYS - 1)
    daily_cutoff = today - timedelta(days=DAILY_RETENTION_DAYS - 1)
    dropped: list[str] = []

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT c.relname
              FROM pg_inherits i
              JOIN pg_class c ON c.oid = i.inhrelid
              JOIN pg_class p ON p.oid = i.inhparent
             WHERE p.relname IN ('intraday_bars', 'daily')
            """
        )
        for r in rows:
            name = r["relname"]
            try:
                suffix = name.rsplit("_", 1)[-1]
                part_date = date(int(suffix[0:4]), int(suffix[4:6]), int(suffix[6:8]))
            except (ValueError, IndexError):
                logger.warning("Skipping partition with unparseable name: %s", name)
                continue

            cutoff = intraday_cutoff if name.startswith("intraday_bars") else daily_cutoff
            if part_date < cutoff:
                await conn.execute(f"DROP TABLE IF EXISTS {name};")
                dropped.append(name)
                logger.debug("Dropped %s (before %s)", name, cutoff)

    if dropped:
        logger.info("Dropped %d partitions past retention", len(dropped))
    else:
        logger.info("No partitions past retention (nothing to drop)")
