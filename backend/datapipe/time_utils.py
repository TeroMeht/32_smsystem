"""
Timezone + session-grid utilities.

Polygon emits all timestamps in UTC. The trading session grid is defined
in ET (America/New_York); the UI + logs display in Helsinki. This module
is the single place we convert between those, derive session-day
buckets, and generate the bar_time grid so callers never have to think
about DST or midnight-in-UTC splitting a session.

Everything the datapipe writes to Postgres uses ``timestamptz`` -- we
persist UTC and let the DB store the offset.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from backend.datapipe.schemas import BAR_MINUTES


# ---------------------------------------------------------------------------
# constants
# ---------------------------------------------------------------------------


ET = ZoneInfo("America/New_York")
HELSINKI = ZoneInfo("Europe/Helsinki")
UTC = timezone.utc


# ---------------------------------------------------------------------------
# normalize (naive -> UTC-aware) -- private, used by the tz converters
# ---------------------------------------------------------------------------


def _ensure_utc(dt: datetime) -> datetime:
    """Return a UTC tz-aware copy of ``dt``. Naive input is assumed UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


# ---------------------------------------------------------------------------
# tz converters
# ---------------------------------------------------------------------------


def to_utc(dt: datetime) -> datetime:
    """Return a UTC tz-aware copy of ``dt`` (naive is assumed UTC)."""
    return _ensure_utc(dt)


def to_helsinki(dt: datetime) -> datetime:
    """Return dt in Europe/Helsinki (DST-aware). Naive dt is assumed UTC."""
    return _ensure_utc(dt).astimezone(HELSINKI)


# ---------------------------------------------------------------------------
# session grid (ET-based)
# ---------------------------------------------------------------------------


def session_date_et(ts_utc: datetime) -> date:
    """
    The trading-session date the given UTC timestamp belongs to (ET calendar).

    Used to key partition inserts, split intraday vs today buckets, and
    to group bars into ET sessions during the rvol baseline rebuild.
    """
    return _ensure_utc(ts_utc).astimezone(ET).date()


def previous_trading_day(d: date) -> date:
    """
    The most recent US-market trading day strictly before ``d``.

    Weekend-aware only: Mon -> Fri, Tue -> Mon, ..., Sun -> Fri.

    Used by the historian as the freshness bar -- "we should already have
    data through this date; if we don't, refetch". Deliberately does NOT
    know about market holidays (Thanksgiving, Christmas, etc.). Worst case
    on a post-holiday morning is one wasted REST call that returns empty.
    """
    prev = d - timedelta(days=1)
    while prev.weekday() >= 5:  # 5 = Sat, 6 = Sun
        prev -= timedelta(days=1)
    return prev


# ---------------------------------------------------------------------------
# bar-time slots (Helsinki-based)
# ---------------------------------------------------------------------------


def helsinki_time_slot(ts_utc: datetime) -> time:
    """
    The Helsinki time-of-day of the bar's *start*, minutes rounded
    (seconds zeroed). Join key into ``rvol_baseline.bar_time``.

    Baseline slots are keyed in Helsinki so the DB values match the
    display everywhere else in the app. Note: US and Finland transition
    DST on different weekends, so for ~3 weeks a year the ET-equivalent
    of a given Helsinki HH:MM shifts by an hour. Baseline lookups during
    those weeks may return the neighbouring slot's average.
    """
    hki_dt = to_helsinki(ts_utc)
    return time(hki_dt.hour, hki_dt.minute)


def helsinki_2min_slots() -> list[time]:
    """
    Every 2-minute Helsinki slot in a 24-hour day: 00:00, 00:02, ..., 23:58.

    Values are naive ``datetime.time`` -- callers key them against the
    Helsinki-interpreted bar_time (see ``helsinki_time_slot``). Used by
    the rvol_baseline rebuild to generate the full 720-slot grid per
    active symbol.
    """
    return [
        time(h, m, 0)
        for h in range(24)
        for m in range(0, 60, BAR_MINUTES)
    ]


# ---------------------------------------------------------------------------
# misc formatting
# ---------------------------------------------------------------------------


def date_to_iso(d: date) -> str:
    """YYYY-MM-DD for REST 'from'/'to' params."""
    return d.isoformat()
