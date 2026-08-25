"""
Typed data models for the shapes crossing internal module boundaries.

Two shapes to be aware of:

  * ``CandleRow`` -- the *canonical* enriched intraday bar, defined in
                     the shared ``indicators`` package and re-exported
                     here for convenience. Explicit field names, tz-
                     aware UTC ``ts``, ``symbolid`` resolved.
  * ``DailyBar``  -- one raw daily OHLCV row for the ``daily`` table.
                     Not shared -- lives here because only the historian
                     ever touches it.

Wire-format shapes (Polygon REST results / WS /stocks/AM payloads) live
inside ``data_sources.polygon`` -- consumers never see them. The polygon
adapter emits ``IncomingBar`` at the seam; ``DailyBar.from_incoming``
(historian) and the CandleRow constructor calls in livestream/warmup
wrap those into typed rows.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from data_sources._bar import IncomingBar
from indicators.candle_row import CandleRow  # re-exported from shared package


def candle_row_from_incoming(incoming, symbol: str, symbolid: int) -> "CandleRow":
    """
    Build a ``CandleRow`` from an ``IncomingBar`` (or anything with the
    same six attributes) at the data_sources -> project seam.

    Kept in this module -- next to ``DailyBar.from_incoming`` -- so
    every adapter concern lives on the project side and the shared
    ``indicators`` package stays free of any implicit knowledge about
    what an ``IncomingBar`` is.

    ``incoming.date`` is a tz-aware UTC datetime; it lands on ``ts``.
    Local ``date`` / ``time`` fields are left ``None`` -- the DB
    writers don't need them on this table.
    """
    return CandleRow(
        symbol   = symbol,
        symbolid = symbolid,
        ts       = incoming.date,
        open     = float(incoming.open),
        high     = float(incoming.high),
        low      = float(incoming.low),
        close    = float(incoming.close),
        volume   = float(incoming.volume),
    )
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
