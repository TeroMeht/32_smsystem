"""
Reads and writes for the ``rvol_baseline`` table.

The baseline stores the PER-BAR average volume per (symbolid, ET bar_time)
across the last N trading sessions. It's the input that the live path
uses to build a cumulative denominator on the fly: for each incoming bar
we add the current slot's per-bar average to a running sum, then divide
today's cum volume by that. A missing slot contributes 0 (its per-bar
avg was never populated) but the running sum keeps its prior value, so
RVOL stays defined for the rest of the session.

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
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

import asyncpg

logger = logging.getLogger(__name__)


# SQL:
#   1. per_bar   -- tag each intraday row with its ET session_date and
#                   ET bar_time slot. Just projection -- no windowing.
#   2. sessions_ranked -- rank the distinct session_dates per symbol
#                         freshest first.
#   3. recent    -- keep only sessions where session_rank <= $3
#                   (the N most recent trading sessions per symbol).
#   4. Aggregate: AVG raw volume across those recent sessions and UPSERT
#      per (symbolid, bar_time). The result is a per-bar average -- NOT
#      cumulative -- because the live path accumulates on the fly.
_REBUILD_SQL = """
    WITH per_bar AS (
        SELECT symbolid,
               (ts AT TIME ZONE 'America/New_York')::date  AS session_date,
               make_time(
                   EXTRACT(HOUR   FROM (ts AT TIME ZONE 'America/New_York'))::int,
                   EXTRACT(MINUTE FROM (ts AT TIME ZONE 'America/New_York'))::int,
                   0
               ) AS bar_time,
               volume
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
    INSERT INTO rvol_baseline (symbolid, bar_time, avg_volume, sample_days, updated)
    SELECT p.symbolid,
           p.bar_time,
           AVG(p.volume)::numeric(16,2)             AS avg_volume,
           COUNT(DISTINCT p.session_date)::smallint AS sample_days,
           now()                                    AS updated
      FROM per_bar p
      JOIN recent r
        ON r.symbolid = p.symbolid AND r.session_date = p.session_date
     GROUP BY p.symbolid, p.bar_time
    ON CONFLICT (symbolid, bar_time) DO UPDATE SET
        avg_volume  = EXCLUDED.avg_volume,
        sample_days = EXCLUDED.sample_days,
        updated     = EXCLUDED.updated;
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
