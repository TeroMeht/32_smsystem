"""
In-memory per-symbol session state used by both livestream and replay.

Both paths need the same lightweight running state to enrich each incoming
1-min bar cheaply:

  * ``history``              -- list of prior bars this session (in order)
  * ``atr``                  -- latest daily ATR (feeds RelATR)
  * ``rvol_baseline``        -- dict[ET bar_time -> per-bar avg volume]
  * ``baseline_history_sum`` -- running sum of per-bar baselines consumed
                                by prior bars this session. Together with
                                the current slot's baseline, this forms
                                the cumulative denominator for RVOL.

The state can be rehydrated on process restart from ``load_session_bars``
in backend.database.readers, but the RVOL denominator is a session-local
concept -- on a mid-session restart you'd want to rebuild
``baseline_history_sum`` from the sequence of already-persisted bars'
slots. Not implemented yet.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, time
from typing import Optional

from backend.datapipe.schemas import Bar1m


@dataclass
class SymbolSessionState:
    symbol: str
    symbolid: int
    session_date: date  # ET session date
    history: list[Bar1m] = field(default_factory=list)
    atr: Optional[float] = None
    # rvol_baseline for THIS symbol keyed by Helsinki bar_time. Values are
    # PER-BAR average volumes -- the live path accumulates them.
    rvol_baseline: dict[time, float] = field(default_factory=dict)
    # Running sum of per-bar baselines consumed so far this session.
    # Advanced by the bar processor after each bar is enriched.
    baseline_history_sum: float = 0.0

    def baseline_for_slot(self, slot: time) -> float:
        return self.rvol_baseline.get(slot, 0.0)


class SessionStore:
    """symbol -> SymbolSessionState. Reset at each session boundary."""

    def __init__(self) -> None:
        self._by_symbol: dict[str, SymbolSessionState] = {}

    def get_or_init(
        self,
        symbol: str,
        symbolid: int,
        session_date: date,
    ) -> SymbolSessionState:
        st = self._by_symbol.get(symbol)
        if st is None or st.session_date != session_date:
            st = SymbolSessionState(symbol=symbol, symbolid=symbolid, session_date=session_date)
            self._by_symbol[symbol] = st
        return st

    def all(self) -> dict[str, SymbolSessionState]:
        return dict(self._by_symbol)

    def reset(self) -> None:
        self._by_symbol.clear()

    def contains(self, symbol: str) -> bool:
        return symbol in self._by_symbol
