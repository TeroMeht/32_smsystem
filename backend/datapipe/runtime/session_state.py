from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, time
from typing import Optional

from backend.datapipe.calculations.calculations import (
    calculate_next_ema,
    calculate_next_relatr,
    calculate_next_rvol_cum,
    calculate_next_vwap,
)
from backend.datapipe.schemas import Bar
from backend.datapipe.time_utils import helsinki_time_slot


@dataclass
class SymbolSessionState:
    symbol: str
    symbolid: int
    session_date: date  # ET session date
    history: list[Bar] = field(default_factory=list)
    atr: Optional[float] = None
    rvol_baseline: dict[time, float] = field(default_factory=dict)
    baseline_history_sum: float = 0.0
    vol_sum: float = 0.0

    def apply_bar(self, bar: Bar) -> Bar:
        """
        Enrich ``bar`` in place using this state, append to history, and
        advance the running sums. Returns the same Bar for chaining.

        The per-bar rvol baseline is looked up internally from
        ``rvol_baseline`` keyed by the bar's Helsinki time slot -- callers
        never need to pass it. Called by the live WS consumer (via
        ``process_bar``). Persistence + optional sink live in
        ``bar_processor.process_bar``.
        """
        slot_avg = self.rvol_baseline.get(helsinki_time_slot(bar.ts), 0.0)
        vwap = calculate_next_vwap(bar, self.history)
        bar.vwap = vwap
        bar.ema9 = calculate_next_ema(bar, self.history)
        # Skip RelATR when ATR is missing (symbol had no daily_indicators
        # row at seed time). The livestream ``relatr`` column is nullable;
        # calculate_next_relatr assumes a valid ATR and would ZeroDiv /
        # TypeError otherwise. Never pass NaN/None into the calc layer.
        bar.relatr = (
            calculate_next_relatr(vwap, bar.close, self.atr)
            if self.atr
            else None
        )
        bar.rvol_cum = calculate_next_rvol_cum(
            bar.volume, self.vol_sum, slot_avg, self.baseline_history_sum,
        )
        # Advance state -- O(1) per bar.
        self.history.append(bar)
        self.vol_sum += bar.volume
        self.baseline_history_sum += slot_avg
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
        Create and register fresh state for ``symbol``. Called ONCE per
        symbol at startup from ``seed_session_state``. Overwrites any
        existing entry -- callers should not rely on that path.
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
