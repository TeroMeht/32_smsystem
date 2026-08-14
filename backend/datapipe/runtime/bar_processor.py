"""
The single entrypoint that turns a raw incoming Bar1m into an enriched,
persisted bar. Used by BOTH livestream.py and replay.py so behavior is
identical between them.

Steps:
  1. Look up or create the per-symbol SymbolSessionState.
  2. Compute the Helsinki bar_time slot; grab that slot's rvol baseline
     (per-bar average from the rvol_baseline table).
  3. Enrich the bar with VWAP/EMA9/RelATR/RVOL cumulative.
  4. Append the enriched bar to session history.
  5. Persist to livestream.
  6. Emit the enriched bar via an optional callback -- this is the hook the
     service layer plugs into to run strategies + push SSE events.
"""

from __future__ import annotations

import logging
from typing import Awaitable, Callable, Optional

import asyncpg

from backend.database.writers import insert_livestream_bar
from backend.datapipe.calculations import (
    calculate_next_ema,
    calculate_next_relatr,
    calculate_next_rvol_cum,
    calculate_next_vwap,
)
from backend.datapipe.schemas import Bar1m
from backend.datapipe.runtime.session_state import SessionStore
from backend.datapipe.time_utils import helsinki_time_slot, session_date_et

logger = logging.getLogger(__name__)


# Callback signature: async fn taking the enriched Bar1m. Return value ignored.
BarSink = Callable[[Bar1m], Awaitable[None]]





# ---------------------------------------------------------------------------
# High-level enrichment -- glues the four indicators onto a Bar1m in one call
# ---------------------------------------------------------------------------


def enrich_bar(
    new_bar: Bar1m,
    history: list[Bar1m],
    atr: Optional[float],
    baseline_slot_avg: float = 0.0,
    baseline_history_sum: float = 0.0,
) -> Bar1m:
    """
    Populate all four indicator slots on ``new_bar`` and return it.

    ``history`` is the list of session bars strictly BEFORE ``new_bar``, in
    chronological order. Callers (livestream / replay) maintain a small
    in-memory deque per symbol so this stays cheap.

    ``baseline_slot_avg`` is the per-bar avg for THIS bar's ET slot.
    ``baseline_history_sum`` is the running sum of per-bar baselines from
    every prior bar this session -- so together they form the cumulative
    denominator for RVOL. See ``next_rvol_cum`` for semantics.
    """
    vwap = calculate_next_vwap(new_bar, history)
    new_bar.vwap = vwap
    new_bar.ema9 = calculate_next_ema(new_bar, history)
    new_bar.relatr = calculate_next_relatr(vwap, new_bar.close, atr)
    hist_vol_sum = float(sum(b.volume for b in history))
    new_bar.rvol_cum = calculate_next_rvol_cum(
        new_bar.volume, hist_vol_sum, baseline_slot_avg, baseline_history_sum,
    )
    return new_bar







async def process_bar(
    pool: asyncpg.Pool,
    store: SessionStore,
    bar: Bar1m,
    sink: Optional[BarSink] = None,
) -> Bar1m:
    """
    Enrich + persist one incoming bar. Returns the enriched Bar1m so
    callers doing synchronous follow-up work (unit tests, replay) can see
    the same object the sink saw.
    """
    st = store.get_or_init(bar.symbol, bar.symbolid, session_date_et(bar.ts))

    slot = helsinki_time_slot(bar.ts)
    slot_avg = st.baseline_for_slot(slot)

    enriched = enrich_bar(
        new_bar=bar,
        history=st.history,
        atr=st.atr,
        baseline_slot_avg=slot_avg,
        baseline_history_sum=st.baseline_history_sum,
    )

    await insert_livestream_bar(pool, enriched)

    st.history.append(enriched)
    st.baseline_history_sum += slot_avg

    if sink is not None:
        try:
            await sink(enriched)
        except Exception:
            logger.exception("bar sink failed for %s @ %s", enriched.symbol, enriched.ts)

    return enriched
