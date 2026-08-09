"""
Reads and writes for the ``rvol_baseline`` table.

The baseline stores avg cumulative volume per (symbolid, ET bar_time) --
the denominator for cumulative RVOL. It's rebuilt from ``intraday_bars``
at startup (and again before every replay).

The rebuild takes two knobs:

  * ``lookback_days``    -- how many CALENDAR days back to search for
                            intraday data (needs to be wide enough to
                            contain the target trading-session count even
                            after weekends + holidays).
  * ``sample_sessions``  -- how many TRADING sessions to actually average
                            per symbol. The SQL picks the N most recent
                            distinct session_dates per symbol from the
                            lookback window, so on a normal week with
                            lookback_days=8, sample_sessions=5 you get an
                            average over exactly the last 5 trading days.

Why the split: calendar-day windowing alone gives you 3-5 trading days
depending on the day of week and whether there's a US holiday. The user
wants a specific number of trading sessions (5), so we fetch/keep a wider
calendar window and let the SQL pick the freshest N sessions per symbol.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

import asyncpg

logger = logging.getLogger(__name__)


# SQL:
#   1. per_bar   -- for each intraday row, tag it with its ET session_date
#                   and ET bar_time slot, plus cum_vol for that session.
#   2. sessions_ranked -- rank the distinct session_dates per symbol
#                         freshest first, using DENSE_RANK so identical
#                         dates would share a rank (they can't here, but
#                         it's the semantically right function).
#   3. recent    -- keep only sessions where session_rank <= $3
#                   (i.e. the N most recent trading sessions per symbol).
#   4. Aggregate: average cum_vol across ONLY those recent sessions and
#      UPSERT into rvol_baseline. sample_days is now always exactly the
#      count of recent sessions we found for that symbol (typically N,
#      less if the symbol has fewer sessions on disk).
_REBUILD_SQL = """
    WITH per_bar AS (
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
    ),
    sessions_ranked AS (
        SELECT symbolid, session_date,
               DENSE_RANK() OVER (
                   PARTITION BY symbolid
                   ORDER BY session_date DESC
               ) AS session_rank
          FROM (SELECT DISTINCT symbolid, session_date FROM per_bar) s
    ),
    recent AS (
        SELECT symbolid, session_date
          FROM sessions_ranked
         WHERE session_rank <= $3
    )
    INSERT INTO rvol_baseline (symbolid, bar_time, avg_cum_volume, sample_days, updated)
    SELECT p.symbolid,
           p.bar_time,
           AVG(p.cum_vol)::numeric(16,2)          AS avg_cum_volume,
           COUNT(DISTINCT p.session_date)::smallint AS sample_days,
           now()                                   AS updated
      FROM per_bar p
      JOIN recent r
        ON r.symbolid = p.symbolid AND r.session_date = p.session_date
     GROUP BY p.symbolid, p.bar_time
    ON CONFLICT (symbolid, bar_time) DO UPDATE SET
        avg_cum_volume = EXCLUDED.avg_cum_volume,
        sample_days    = EXCLUDED.sample_days,
        updated        = EXCLUDED.updated;
"""


async def rebuild(
    pool: asyncpg.Pool,
    end_day: date,
    lookback_days: int = 8,
    sample_sessions: int = 5,
) -> None:
    """
    Recompute avg cumulative volume per (symbolid, ET bar_time) using the
    N most recent distinct trading sessions found in the calendar window
    ``[end_day - lookback_days, end_day)``, then UPSERT into
    ``rvol_baseline``.

    ``end_day`` is exclusive (matches replay semantics: baseline must not
    include the replay/target day itself). For live use, pass ``today``.
    """
    start_day = end_day - timedelta(days=lookback_days)
    logger.info(
        "[db.rvol_baseline] rebuilding: calendar window [%s, %s) "
        "-> take %d most recent trading sessions per symbol",
        start_day, end_day, sample_sessions,
    )
    async with pool.acquire() as conn:
        await conn.execute(
            _REBUILD_SQL,
            datetime.combine(start_day, datetime.min.time()),
            datetime.combine(end_day, datetime.min.time()),
            sample_sessions,
        )
        count = await conn.fetchval("SELECT COUNT(*) FROM rvol_baseline;")
        # Distribution of sample_days so operator can spot thin baselines
        dist_rows = await conn.fetch(
            "SELECT sample_days, COUNT(*) AS n "
            "FROM rvol_baseline GROUP BY sample_days ORDER BY sample_days DESC;"
        )
    logger.info("[db.rvol_baseline] rebuild complete -- %d rows now in table", count)
    if dist_rows:
        dist = {r["sample_days"]: r["n"] for r in dist_rows}
        logger.info("[db.rvol_baseline] sample_days distribution: %s", dist)
