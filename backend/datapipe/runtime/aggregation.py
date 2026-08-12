"""
Per-symbol N-minute streaming aggregator for the live WS path.

Polygon's REST endpoint supports ``/range/N/minute/...`` server-side, so
the historian and livestream REST-prime paths request N-min bars directly
and don't need any client-side aggregation. This module only exists for
the live WS consumer -- Polygon's AM socket only emits 1-min bars, so we
buffer them per symbol and emit one N-min aggregate every N-th minute.

Window alignment: buckets are aligned to the wall-clock N-min grid in
UTC (equivalent to Helsinki at the minute level -- both share the
minute-of-hour). For N=2: 09:30-09:32, 09:32-09:34, .... Aggregate ``ts``
is the window START (even minute).

A partial trailing window (single 1-min bar with no partner) is buffered
but never emitted alone -- if the session ends on an odd minute, that
last minute is dropped.

Cadence is set by ``BAR_MINUTES`` in ``datapipe.schemas`` -- the same
constant used by ``sources.rest_client`` to build the REST URL, so the
whole system agrees on the window size.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from backend.datapipe.schemas import BAR_MINUTES, Bar1m


_BUCKET_SECONDS = BAR_MINUTES * 60


def _bucket_of(bar: Bar1m) -> int:
    """Integer bucket ID = epoch-seconds floor-divided by window length."""
    return int(bar.ts.timestamp()) // _BUCKET_SECONDS


def _bucket_start_ts(bucket_id: int) -> datetime:
    """UTC start of the bucket window."""
    return datetime.fromtimestamp(bucket_id * _BUCKET_SECONDS, tz=timezone.utc)


def _merge(bars: list[Bar1m]) -> Bar1m:
    """Fold a full window of 1-min bars into one aggregated Bar1m."""
    first = bars[0]
    return Bar1m(
        symbol   = first.symbol,
        symbolid = first.symbolid,
        ts       = _bucket_start_ts(_bucket_of(first)),
        open     = first.open,
        high     = max(b.high for b in bars),
        low      = min(b.low  for b in bars),
        close    = bars[-1].close,
        volume   = sum(b.volume for b in bars),
    )


class BarAggregator:
    """
    Per-symbol streaming aggregator for the live WS path.

    Feed 1-min bars in via ``feed()``; get back ``None`` while a window is
    still filling, or the completed aggregated bar when it closes. Handles
    gaps (missed minute) by dropping the incomplete window and starting
    fresh on the next bar.

    Not thread-safe -- caller owns the per-symbol instance.
    """

    __slots__ = ("_bucket", "_buf")

    def __init__(self) -> None:
        self._bucket: Optional[int] = None
        self._buf: list[Bar1m] = []

    def feed(self, bar: Bar1m) -> Optional[Bar1m]:
        b = _bucket_of(bar)
        if self._bucket is None or b != self._bucket:
            # New bucket. Drop any partial window we had; start a fresh one.
            self._bucket = b
            self._buf = [bar]
            return None
        self._buf.append(bar)
        if len(self._buf) == BAR_MINUTES:
            out = _merge(self._buf)
            self._bucket = None
            self._buf = []
            return out
        return None
