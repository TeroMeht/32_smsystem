"""
Speed of the move -- EMA9-anchored %/bar, computed bar by bar.

Same rules as 35_backtesting (``a_scanner/_scan.py`` SpeedTracker,
``c_viewer/_speed.py``) and 30_ReviewApp ``helpers/speed_metrics.py``,
so a number on the live dashboard means the same thing as in the
scanner and the backtest viewer. Keep them in sync.

  * anchor = the bar whose close crosses BELOW EMA9 coming from above
             (prev close >= prev ema9 and close < ema9);
  * a close crossing back ABOVE EMA9 clears the anchor;
  * speed  = (C_anchor - C_now) / C_anchor / bars_since_anchor * 100
             [%/bar] on every bar after the anchor bar, else None.

Positive = falling away from the anchor (the long / capitulation
direction). Interpretation bands (2-min bars):
    < 0.20 grind, 0.20-0.40 real decline, >= 0.40 capitulation.

Session-scoped: one ``SpeedStore`` lives next to the ``SessionStore``
for the life of the livestream task (livestream = today only), and is
fed every bar in ts order -- the REST-primed bars at startup first,
then the live WS bars.

Pure state, no I/O.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from indicators.candle_row import CandleRow


@dataclass(frozen=True, slots=True)
class SpeedValue:
    """Speed for one bar. Both fields None when no EMA9 cross-down anchor is active."""

    speed: Optional[float]   # %/bar since the anchor
    bars: Optional[int]      # bars since the anchor


NO_SPEED = SpeedValue(speed=None, bars=None)


class SpeedTracker:
    """Per-symbol state machine. Feed bars in ts order via ``update``."""

    __slots__ = ("_i", "_anchor_i", "_anchor_close", "_prev_close", "_prev_ema")

    def __init__(self) -> None:
        self._i = -1
        self._anchor_i: Optional[int] = None
        self._anchor_close: Optional[float] = None
        self._prev_close: Optional[float] = None
        self._prev_ema: Optional[float] = None

    def update(self, close: float, ema9: Optional[float]) -> SpeedValue:
        self._i += 1
        close = float(close)
        e = None if ema9 is None or ema9 != ema9 else float(ema9)   # NaN -> None
        if self._prev_close is not None and self._prev_ema is not None and e is not None:
            if self._prev_close >= self._prev_ema and close < e:
                self._anchor_i, self._anchor_close = self._i, close
            elif self._prev_close <= self._prev_ema and close > e:
                self._anchor_i, self._anchor_close = None, None
        self._prev_close, self._prev_ema = close, e

        if self._anchor_i is None or self._anchor_i == self._i or not self._anchor_close:
            return NO_SPEED
        k = self._i - self._anchor_i
        return SpeedValue(
            speed=(self._anchor_close - close) / self._anchor_close / k * 100.0,
            bars=k,
        )


class SpeedStore:
    """symbol -> SpeedTracker for the current session."""

    __slots__ = ("_by_symbol",)

    def __init__(self) -> None:
        self._by_symbol: dict[str, SpeedTracker] = {}

    def update(self, bar: CandleRow) -> SpeedValue:
        """Fold an ENRICHED bar (``ema9`` set by ``apply_bar``) into its symbol's tracker."""
        tr = self._by_symbol.get(bar.symbol)
        if tr is None:
            tr = self._by_symbol[bar.symbol] = SpeedTracker()
        if bar.close is None:
            return NO_SPEED
        return tr.update(bar.close, bar.ema9)
