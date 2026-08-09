"""
In-memory per-symbol session state used by both livestream and replay.

Both paths need the same lightweight running state to enrich each incoming
1-min bar cheaply:

  * ``history``               -- list of prior bars this session (in order)
  * ``atr``                   -- latest daily ATR (feeds RelATR)
  * ``baseline_slot_avg``     -- avg cum volume for the current ET minute slot
  * ``baseline_history_sum``  -- running sum of avg cum volumes for prior slots

Keeping these in memory means the enrichment step is O(1) per incoming bar
apart from an EMA9 recompute over the small session-history list -- and
avoids a DB read in the hot path.

The state can be rehydrated on process restart from ``load_session_bars``
in backend.database.readers.
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
    # rvol_baseline for THIS symbol keyed by ET bar_time
    rvol_baseline: dict[time, float] = field(default_factory=dict)
    # running sum of baseline slot averages consumed by prior bars this session
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
