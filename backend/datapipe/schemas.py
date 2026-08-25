"""
Typed data models for the shapes crossing internal module boundaries.

Two shapes to be aware of:

  * ``Bar``       -- the *canonical* intraday bar the rest of the system
                     consumes. Explicit field names, tz-aware UTC
                     timestamp, symbolid resolved.
  * ``DailyBar``  -- one raw daily OHLCV row for the ``daily`` table.

Wire-format shapes (Polygon REST results / WS /stocks/AM payloads) live
inside ``data_sources.polygon`` -- consumers never see them. The polygon
adapter emits ``IncomingBar`` at the seam; the ``from_incoming``
classmethods below wrap those into ``Bar`` / ``DailyBar`` in one line at
the two callers that need them (livestream + historian).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from data_sources._bar import IncomingBar
from pydantic import BaseModel, ConfigDict


# ---------------------------------------------------------------------------
# System-wide bar cadence
# ---------------------------------------------------------------------------
# Single source of truth for the aggregation window (minutes). Read by:
#   * data_sources.polygon._source -- builds Polygon URL ``/range/N/minute/...``
#   * runtime.aggregation -- WS-side 1-min -> N-min bucketing
#   * rvol_baseline SQL   -- generates the 24h Helsinki slot grid
# Fixed at 2 for now; promote to settings if it ever needs to vary.
BAR_MINUTES = 2


# ---------------------------------------------------------------------------
# Canonical bar -- the ONLY bar shape used past the adapter boundary
# ---------------------------------------------------------------------------


class Bar(BaseModel):
    """
    A validated N-min OHLCV bar with indicator slots.

    Persisted directly to ``livestream`` (indicator columns filled) and
    to ``intraday_bars`` (indicator columns dropped -- history table
    only stores the raw OHLCV). ``ts`` is always UTC tz-aware, matching
    timestamptz.
    """

    model_config = ConfigDict(frozen=False)

    symbol: str
    symbolid: int
    ts: datetime  # UTC, tz-aware -- window start
    open: float
    high: float
    low: float
    close: float
    volume: int
    # Indicator slots populated by calculations.py
    vwap: Optional[float] = None          # session-cumulative VWAP
    ema9: Optional[float] = None
    rvol_cum: Optional[float] = None
    relatr: Optional[float] = None

    @classmethod
    def from_incoming(cls, ib: IncomingBar, symbol: str, symbolid: int) -> "Bar":
        """
        Wrap an ``IncomingBar`` from the data_sources seam into a
        canonical ``Bar``. ``IncomingBar.date`` is expected to be a
        tz-aware UTC datetime (both PolygonHistoricalSource and
        PolygonRealtimeSource emit it that way).
        """
        return cls(
            symbol   = symbol,
            symbolid = symbolid,
            ts       = ib.date,
            open     = float(ib.open),
            high     = float(ib.high),
            low      = float(ib.low),
            close    = float(ib.close),
            volume   = int(ib.volume),
        )


# ---------------------------------------------------------------------------
# Daily bar shape (for the `daily` table + ATR14 warmup)
# ---------------------------------------------------------------------------


class DailyBar(BaseModel):
    """
    One raw daily OHLCV row for the ``daily`` table.

    ATR (and any other derived indicator) is NOT part of this shape --
    the historian computes those in a separate pass and persists them
    to ``daily_indicators``. Keeping DailyBar raw preserves the
    "incoming vs calculated" boundary end-to-end.
    """

    symbol: str
    symbolid: int
    d: date
    open: float
    high: float
    low: float
    close: float
    volume: int

    @classmethod
    def from_incoming(cls, ib: IncomingBar, symbol: str, symbolid: int) -> "DailyBar":
        """
        Wrap an ``IncomingBar`` from the data_sources seam into a
        ``DailyBar``. ``IncomingBar.date`` is a tz-aware UTC datetime;
        we keep only the date portion (Polygon daily bars are
        session-dated at midnight UTC).
        """
        return cls(
            symbol   = symbol,
            symbolid = symbolid,
            d        = ib.date.date(),
            open     = float(ib.open),
            high     = float(ib.high),
            low      = float(ib.low),
            close    = float(ib.close),
            volume   = int(ib.volume),
        )


# ---------------------------------------------------------------------------
# Active-symbol universe
# ---------------------------------------------------------------------------
# Loaded once by ``load_active_symbol_map`` at pipeline startup and passed
# read-only through ``run_livestream``. Treated as read-only by
# convention -- if you need enforced immutability, wrap in
# ``types.MappingProxyType`` at the load site.
MonitoredSymbols = dict[str, int]  # symbol -> symbolid
