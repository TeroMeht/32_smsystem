"""
Reads and writes for the ``rvol_baseline`` table.

The baseline stores avg cumulative volume per (symbolid, ET bar_time) --
the denominator for cumulative RVOL. It's rebuilt from ``intraday_bars``
at startup (and again before every replay), so this module has one write
op (``rebuild``) and one read op (see ``readers.load_rvol_baseline_for_symbol``
for the per-symbol lookup used in the live path).

The rebuild is a single SQL statement:
  1. For each intraday bar, project (symbolid, session_date, ET bar_time,
     cum_vol) using an ET-localized window function.
  2. Average cum_vol across sessions for each (symbolid, bar_time).
  3. UPSERT into ``rvol_baseline``.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

import asyncpg

logger = logging.getLogger(__name__)


_REBUILD_SQL = """
    INSERT INTO rvol_baseline (symbolid, bar_time, avg_cum_volume, sample_days, updated)
    SELECT symbolid,
           bar_time,
           AVG(cum_vol)::numeric(16,2) AS avg_cum_volume,
           COUNT(DISTINCT session_date)::smallint AS sample_days,
           now() AS updated
      FROM (
            SELECT symbolid,
                   (ts AT TIME ZONE 'America/New_York')::date  AS session_date,
                   make_time(
                       EXTRACT(HOUR   FROM (ts AT TIME ZONE 'America/New_York'))::int,
                       EXTRACT(MINUTE FROM (ts AT TIME ZONE 'America/New_York'))::int,
                       0
                   ) AS bar_time,
                   SUM(volume) OVER (
                       PARTITION BY symbolid, (ts AT TIME ZONE 'America/New_York')::date
                       ORDER BY ts
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) AS cum_vol
              FROM intraday_bars
             WHERE ts >= $1
               AND ts <  $2
           ) s
     GROUP BY symbolid, bar_time
    ON CONFLICT (symbolid, bar_time) DO UPDATE SET
        avg_cum_volume = EXCLUDED.avg_cum_volume,
        sample_days    = EXCLUDED.sample_days,
        updated        = EXCLUDED.updated;
"""


async def rebuild(
    pool: asyncpg.Pool,
    end_day: date,
    sample_days: int = 5,
) -> None:
    """
    Recompute avg cumulative volume per (symbolid, ET bar_time) from the
    ``sample_days`` sessions strictly before ``end_day`` and UPSERT into
    ``rvol_baseline``.

    ``end_day`` is exclusive (matches replay semantics: baseline must not
    include the replay day itself). For live use, pass ``today``.
    """
    start_day = end_day - timedelta(days=sample_days)
    async with pool.acquire() as conn:
        await conn.execute(
            _REBUILD_SQL,
            datetime.combine(start_day, datetime.min.time()),
            datetime.combine(end_day, datetime.min.time()),
        )
    logger.info("rvol_baseline rebuilt from %s to %s", start_day, end_day)
