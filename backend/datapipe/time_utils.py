"""
Timezone + session-grid utilities.

Massive/Polygon emits all timestamps as Unix ms in UTC. The trading
session grid, however, is defined in ET (America/New_York). This module
is the single place we convert between the two and derive session-day
buckets so callers never have to think about DST or midnight-in-UTC
splitting a session.

Everything the datapipe writes to Postgres uses ``timestamptz`` -- we
persist UTC and let the DB store the offset.
"""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo

from backend.core.config import settings

ET = ZoneInfo("America/New_York")
HELSINKI = ZoneInfo("Europe/Helsinki")
UTC = timezone.utc


def to_helsinki(dt: datetime) -> datetime:
    """Return dt in Europe/Helsinki (DST-aware). Naive dt is assumed UTC."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(HELSINKI)


def helsinki_hhmm_to_utc(day: date, hhmm: str) -> datetime:
    """
    Take a strict 24-hour "HH:MM" string interpreted as Helsinki local
    time on ``day`` and return the corresponding UTC datetime (tz-aware).
    DST handled automatically via ZoneInfo("Europe/Helsinki").
    """
    h_str, m_str = hhmm.split(":")
    hki_dt = datetime(day.year, day.month, day.day, int(h_str), int(m_str), 0, tzinfo=HELSINKI)
    return hki_dt.astimezone(UTC)


def effective_today() -> date:
    """
    The date the pipeline should treat as "today".

      * MODE=live   -> wall-clock date.today()
      * MODE=replay -> settings.REPLAY_DAY

    Historian, partition planning, and rvol_baseline rebuild all key off
    this. Replay mode also feeds REPLAY_DAY into the replay driver.
    """
    if settings.MODE == "replay":
        return settings.REPLAY_DAY
    return date.today()


def to_utc(dt: datetime) -> datetime:
    """Return a UTC tz-aware copy of ``dt`` (naive is assumed UTC)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def to_et(dt: datetime) -> datetime:
    return to_utc(dt).astimezone(ET)


def session_date_et(ts_utc: datetime) -> date:
    """
    The trading-session date the given UTC timestamp belongs to (ET calendar).

    Used to key partition inserts, split intraday vs today buckets, and to
    look up the rvol baseline slot.
    """
    return to_et(ts_utc).date()


def et_time_slot(ts_utc: datetime) -> time:
    """
    The ET time-of-day of the bar's *start*, minutes rounded (seconds
    zeroed). This is the join key into ``rvol_baseline.bar_time``.
    """
    et_dt = to_et(ts_utc)
    return time(et_dt.hour, et_dt.minute)


def unix_ms_to_utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


def date_to_iso(d: date) -> str:
    """YYYY-MM-DD for REST 'from'/'to' params."""
    return d.isoformat()
