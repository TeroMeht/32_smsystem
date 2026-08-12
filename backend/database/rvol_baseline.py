"""
Reads and writes for the ``rvol_baseline`` table.

Shape:
  * One row per (symbolid, Helsinki bar_time) for EVERY 2-minute slot of
    the 24-hour day (720 rows/active symbol). Slots that don't trade in
    the recent sessions still get a row with avg_volume = 0.
  * ``avg_volume`` = sum(volume across the last N sessions at that slot)
    divided by N -- a FIXED denominator. Missing sessions count as zero
    volume (this is what makes the pre-market / after-hours baselines
    honest instead of averaging across only the days a slot fired).
  * ``sample_days`` = how many of those N sessions actually contributed
    a bar to this slot (0..N). Kept as an operator diagnostic so thin
    baselines are visible.

Live path usage: for each incoming bar we take its slot's per-bar
baseline and add it to a running sum, then divide today's cumulative
volume by that. A slot that never trades contributes 0 to the sum, so
RVOL stays defined.

The rebuild takes two knobs:

  * ``lookback_days``   -- how many CALENDAR days back to search for
                           intraday data (wide enough to contain the
                           target trading-session count even after
                           weekends + holidays).
  * ``sample_sessions`` -- N: how many TRADING sessions to average per
                           symbol AND the denominator. The SQL picks the
                           N most recent distinct session_dates per
                           symbol from the lookback window.
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
_WIPE_SQL = "DELETE FROM rvol_baseline;"

# Full rebuild: every active symbol gets one row per 2-min Helsinki slot
# of the 24-hour day (720 rows/symbol). Slots that never traded in the
# last N sessions still get a row with avg_volume = 0.
#
# Denominator is FIXED at $3 (RVOL_SAMPLE_SESSIONS): a slot that appeared
# in 3 of the 5 recent sessions computes as sum/5, not sum/3 -- missing
# days count as zero volume.
#
# sample_days is kept as a diagnostic: how many of those N sessions
# actually contributed a bar to this slot (0..$3).
#
# NB: kept as a separate statement from the DELETE above because asyncpg
# prepares each execute() as a single SQL command.
_REBUILD_SQL = """
    WITH slots AS (
        -- 24-hour 2-minute Helsinki grid: 00:00, 00:02, ..., 23:58.
        -- BAR_MINUTES is fixed at 2 for the app, so we generate every
        -- even minute of every hour.
        SELECT make_time(h::int, m::int, 0) AS bar_time
          FROM generate_series(0, 23) h
          CROSS JOIN generate_series(0, 58, 2) m
    ),
    active AS (
        SELECT symbolid FROM monitored_symbols WHERE active = true
    ),
    per_bar AS (
        SELECT symbolid,
               -- session_date STAYS in ET: US trading sessions are defined
               -- on the ET calendar (04:00-20:00 ET), so grouping by any
               -- other tz would split a single session across two dates.
               (ts AT TIME ZONE 'America/New_York')::date  AS session_date,
               -- bar_time in Helsinki so DB values match the display used
               -- everywhere else. See helsinki_time_slot() for DST caveat.
               make_time(
                   EXTRACT(HOUR   FROM (ts AT TIME ZONE 'Europe/Helsinki'))::int,
                   EXTRACT(MINUTE FROM (ts AT TIME ZONE 'Europe/Helsinki'))::int,
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
    ),
    agg AS (
        -- Per (symbolid, bar_time): sum of volumes across the recent
        -- sessions that actually had that slot, plus a diagnostic count
        -- of how many distinct sessions contributed.
        SELECT p.symbolid,
               p.bar_time,
               SUM(p.volume)                          AS vol_sum,
               COUNT(DISTINCT p.session_date)::smallint AS sample_days
          FROM per_bar p
          JOIN recent r
            ON r.symbolid = p.symbolid AND r.session_date = p.session_date
         GROUP BY p.symbolid, p.bar_time
    )
    INSERT INTO rvol_baseline (symbolid, bar_time, avg_volume, sample_days, updated)
    SELECT a.symbolid,
           -- Convert the naive Helsinki HH:MM to a Helsinki-offset timetz.
           -- Anchor to current_date, apply AT TIME ZONE, then cast: the
           -- resulting timetz carries the current Helsinki offset (+03
           -- during EEST, +02 during EET). Rebuild is daily so the value
           -- always reflects the current-period offset.
           ((current_date + s.bar_time) AT TIME ZONE 'Europe/Helsinki')::timetz
             AS bar_time,
           COALESCE(agg.vol_sum::numeric / $3::numeric, 0)::numeric(16,2) AS avg_volume,
           COALESCE(agg.sample_days, 0)::smallint                          AS sample_days,
           now()                                                           AS updated
      FROM active a
      CROSS JOIN slots s
      LEFT JOIN agg
        ON agg.symbolid = a.symbolid AND agg.bar_time = s.bar_time;
"""


async def rebuild(
    pool: asyncpg.Pool,
    end_day: date,
    lookback_days: int,
    sample_sessions: int,
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
    window_start = datetime.combine(start_day, datetime.min.time())
    window_end   = datetime.combine(end_day,   datetime.min.time())

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(_WIPE_SQL)
            await conn.execute(
                _REBUILD_SQL,
                window_start,
                window_end,
                sample_sessions,
            )

        # Earliest / latest source ts that actually fed the rebuild.
        # Cast to text server-side after setting the session tz to Helsinki
        # so the log shows values like "2026-08-05 00:00:00+03" -- matching
        # how the DB itself renders timestamptz for a Helsinki client.
        # SET LOCAL scopes the tz change to this transaction only.
        async with conn.transaction():
            await conn.execute("SET LOCAL TIME ZONE 'Europe/Helsinki';")
            src = await conn.fetchrow(
                """
                SELECT MIN(ts)::text AS earliest, MAX(ts)::text AS latest
                  FROM intraday_bars
                 WHERE ts >= $1 AND ts < $2;
                """,
                window_start, window_end,
            )

    logger.info("Rvol basemodel completed -- ts: earliest= %s, latest= %s", src["earliest"], src["latest"])



