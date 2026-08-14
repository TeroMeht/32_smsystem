"""
In-memory per-symbol session state used by both livestream and replay.

Both paths need the same lightweight running state to enrich each incoming
bar cheaply:

  * ``history``              -- list of prior bars this session (in order)
  * ``atr``                  -- latest daily ATR (feeds RelATR)
  * ``rvol_baseline``        -- dict[Helsinki bar_time -> per-bar avg volume]
  * ``baseline_history_sum`` -- running sum of per-bar baselines consumed
                                by prior bars this session. Together with
                                the current slot's baseline, this forms
                                the cumulative denominator for RVOL.

Not persisted across process restarts today; a mid-session restart
rebuilds state fresh from the REST prime (livestream) or from
``intraday_bars`` (replay).
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
    """symbol -> SymbolSessionState. New instance per session boundary."""

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
