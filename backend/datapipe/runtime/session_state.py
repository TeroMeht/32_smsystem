"""
Per-symbol running state + the O(1) ``apply_bar`` that folds one new
bar into it via the shared ``indicators`` package.

Field roles:

    atr         -- yesterday's ATR14, seeded once from
                   ``daily_indicators`` at priming time. Feeds RelATR
                   and DayAtrExt as a divisor. ``None`` if the symbol
                   has no ATR row on disk -- both indicators are then
                   skipped and left as ``None`` on the bar.
    prev_close  -- yesterday's daily close, seeded once from ``daily``
                   at priming time. Feeds DayAtrExt. ``None`` if the
                   symbol has no daily row on disk -- DayAtrExt is
                   then skipped.
    rvol_baseline / baseline_history_sum
                -- rvol_baseline is the seeded per-slot avg volume
                   table for the symbol; baseline_history_sum is the
                   running sum of slot_avgs seen so far this session.
    cum_pv / cum_vol
                -- running (Σ OHLC4·volume, Σ volume) pair for the
                   O(1) session VWAP. cum_vol also serves as the
                   running vol sum for RVOL cum (see note in
                   apply_bar on ordering).
    prev_ema    -- previous EMA9 value, or ``None`` before the first
                   bar (matches ``ewm(adjust=False)`` seeding).

No ``history: list[Bar]`` -- VWAP and EMA are streaming now, and
nothing else read it. That removes the O(N) recompute and the
per-symbol list that grew unbounded through a session.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, time
from typing import Optional

from indicators.day_atr_ext import next_day_atr_ext
from indicators.ema         import next_ema
from indicators.relatr      import next_relatr
from indicators.rvol        import next_rvol_cum
from indicators.vwap        import next_vwap

from backend.datapipe.schemas import Bar
from backend.datapipe.time_utils import helsinki_time_slot


EMA_SPAN: int = 9


@dataclass
class SymbolSessionState:
    symbol: str
    symbolid: int
    session_date: date  # ET session date
    # Seeded once at priming time -- read-only afterward.
    atr:        Optional[float] = None
    prev_close: Optional[float] = None
    rvol_baseline: dict[time, float] = field(default_factory=dict)
    # Running sums advanced by apply_bar every call.
    cum_pv:               float = 0.0    # Σ OHLC4·volume  (VWAP numerator)
    cum_vol:              float = 0.0    # Σ volume        (VWAP denom + RVOL num)
    baseline_history_sum: float = 0.0    # Σ slot_avg      (RVOL denom)
    prev_ema:             Optional[float] = None

    def apply_bar(self, bar: Bar) -> Bar:
        """
        Enrich ``bar`` in place using this state, advance the running
        fields, and return the same Bar for chaining.

        Ordering matters: RVOL cum runs BEFORE VWAP because both fold
        ``bar.volume`` into ``self.cum_vol``, and RVOL uses the
        pre-fold value as its ``running_vol_sum``. VWAP commits the
        post-fold value on the same call.
        """
        slot_avg = self.rvol_baseline.get(helsinki_time_slot(bar.ts), 0.0)

        # RVOL first (reads pre-fold cum_vol).
        bar.rvol_cum, _, self.baseline_history_sum = next_rvol_cum(
            bar.volume,
            running_vol_sum      = self.cum_vol,
            slot_avg             = slot_avg,
            running_baseline_sum = self.baseline_history_sum,
        )

        # VWAP (folds bar.volume into cum_vol; commit post-fold).
        bar.vwap, self.cum_pv, self.cum_vol = next_vwap(
            bar.open, bar.high, bar.low, bar.close, bar.volume,
            cum_pv=self.cum_pv, cum_vol=self.cum_vol,
        )

        # EMA9 (streaming; None on first bar returns the close).
        bar.ema9 = self.prev_ema = next_ema(
            bar.close, prev_ema=self.prev_ema, span=EMA_SPAN,
        )

        # RelATR + DayAtrExt -- skip when their inputs are missing so
        # the livestream columns stay nullable instead of throwing.
        # ``daily_indicators`` may lack an ATR row for a symbol whose
        # daily backfill failed; the ``daily`` table may lack a
        # yesterday-close row for the same reason.
        bar.relatr = (
            next_relatr(bar.vwap, bar.close, self.atr)
            if self.atr else None
        )
        bar.day_atr_ext = (
            next_day_atr_ext(self.prev_close, bar.close, self.atr)
            if self.atr and self.prev_close is not None else None
        )

        return bar


class SessionStore:
    """symbol -> SymbolSessionState. New instance per session boundary."""

    def __init__(self) -> None:
        self._by_symbol: dict[str, SymbolSessionState] = {}

    def init(
        self,
        symbol: str,
        symbolid: int,
        session_date: date,
    ) -> SymbolSessionState:
        """
        Create and register fresh state for ``symbol``. Called ONCE
        per symbol at startup from ``seed_session_state``. Overwrites
        any existing entry -- callers should not rely on that path.
        """
        st = SymbolSessionState(symbol=symbol, symbolid=symbolid, session_date=session_date)
        self._by_symbol[symbol] = st
        return st

    def get(self, symbol: str) -> SymbolSessionState:
        """
        Runtime lookup. Raises ``KeyError`` if the symbol was never
        seeded -- that means the universe changed mid-session or seed
        missed it, and silently constructing empty state here would
        produce garbage indicators.
        """
        st = self._by_symbol.get(symbol)
        if st is None:
            raise KeyError(
                f"SessionStore has no state for {symbol!r} -- seed missed it?"
            )
        return st
