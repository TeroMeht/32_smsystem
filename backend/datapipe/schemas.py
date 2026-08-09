"""
Typed data models for every bar shape crossing a module boundary.

Three shapes to be aware of:

  * ``AggregateMinuteMessage``  -- raw WS /stocks/AM payload from Massive.
                                    Field names match the API exactly (short
                                    keys: ev, sym, o, c, h, l, v, s, e, ...).
  * ``RestAggregateBar``        -- one entry of ``results[]`` from the REST
                                    /v2/aggs/... endpoint. Same short keys
                                    (t, o, h, l, c, v, vw, n).
  * ``Bar1m``                   -- the *canonical* bar the rest of the
                                    system consumes. Explicit field names,
                                    tz-aware UTC timestamp, symbolid resolved.

Adapters (``AggregateMinuteMessage.to_bar1m`` / ``RestAggregateBar.to_bar1m``)
turn the raw shapes into the canonical shape. Everything downstream of the
adapter is Bar1m only -- calculations, DB writer, and strategies never touch
the raw API dicts.
"""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Canonical bar -- the ONLY bar shape used past the adapter boundary
# ---------------------------------------------------------------------------


class Bar1m(BaseModel):
    """
    A validated 1-min OHLCV bar with indicator slots.

    Persisted directly to ``livestream`` (indicator columns filled) and to
    ``intraday_bars`` (indicator columns dropped -- history table only stores
    the raw OHLCV). ``ts`` is always UTC tz-aware, matching timestamptz.
    """

    model_config = ConfigDict(frozen=False)

    symbol: str
    symbolid: int
    ts: datetime  # UTC, tz-aware -- window start (Polygon `s`)
    open: float
    high: float
    low: float
    close: float
    volume: int
    vwap_bar: Optional[float] = None  # per-bar vwap from source (Polygon `vw`)
    # Indicator slots populated by calculations.py
    vwap: Optional[float] = None          # session-cumulative VWAP
    ema9: Optional[float] = None
    rvol_cum: Optional[float] = None
    relatr: Optional[float] = None


# ---------------------------------------------------------------------------
# WebSocket payload -- Massive /stocks/AM
# ---------------------------------------------------------------------------


class AggregateMinuteMessage(BaseModel):
    """
    One incoming WS /stocks/AM message. Field names mirror the API exactly.

    We tolerate unknown fields (``extra="ignore"``) because Massive can add
    non-critical fields without breaking us.
    """

    model_config = ConfigDict(extra="ignore")

    ev: str = Field(..., description="Event type, always 'AM' here")
    sym: str
    o: float
    h: float
    l: float
    c: float
    v: int
    s: int = Field(..., description="Window start, unix ms")
    e: int = Field(..., description="Window end, unix ms")
    vw: Optional[float] = None
    av: Optional[int] = None
    op: Optional[float] = None
    a: Optional[float] = None
    z: Optional[int] = None
    otc: Optional[bool] = None

    def to_bar1m(self, symbolid: int) -> Bar1m:
        """Convert to canonical bar. Requires symbolid resolved by caller."""
        return Bar1m(
            symbol=self.sym,
            symbolid=symbolid,
            ts=datetime.fromtimestamp(self.s / 1000, tz=timezone.utc),
            open=self.o,
            high=self.h,
            low=self.l,
            close=self.c,
            volume=self.v,
            vwap_bar=self.vw,
        )


# ---------------------------------------------------------------------------
# REST payload -- one entry of results[] from /v2/aggs/.../range/1/minute/...
# ---------------------------------------------------------------------------


class RestAggregateBar(BaseModel):
    """One row from the REST aggregates endpoint. Field names mirror the API."""

    model_config = ConfigDict(extra="ignore")

    t: int = Field(..., description="Window start, unix ms")
    o: float
    h: float
    l: float
    c: float
    v: float  # REST returns numeric (can be fractional for OTC); we cast to int
    vw: Optional[float] = None
    n: Optional[int] = None

    def to_bar1m(self, symbol: str, symbolid: int) -> Bar1m:
        return Bar1m(
            symbol=symbol,
            symbolid=symbolid,
            ts=datetime.fromtimestamp(self.t / 1000, tz=timezone.utc),
            open=self.o,
            high=self.h,
            low=self.l,
            close=self.c,
            volume=int(self.v),
            vwap_bar=self.vw,
        )


class RestAggregateResponse(BaseModel):
    """Full response shape from /v2/aggs/.../range/... -- used for validation."""

    model_config = ConfigDict(extra="ignore")

    ticker: Optional[str] = None
    adjusted: Optional[bool] = None
    status: str
    results: list[RestAggregateBar] = Field(default_factory=list)
    resultsCount: int = 0
    next_url: Optional[str] = None


# ---------------------------------------------------------------------------
# Daily bar shape (for the `daily` table + ATR14 warmup)
# ---------------------------------------------------------------------------


class DailyBar(BaseModel):
    """
    One daily OHLCV row for the ``daily`` table. ATR is filled by the
    calculations layer, not the adapter.
    """

    symbol: str
    symbolid: int
    d: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    atr: Optional[float] = None
