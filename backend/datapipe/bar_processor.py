"""
The single entrypoint that turns a raw incoming Bar1m into an enriched,
persisted bar. Used by BOTH livestream.py and replay.py so behavior is
identical between them.

Steps:
  1. Look up or create the per-symbol SymbolSessionState.
  2. Compute the ET bar_time slot; grab that slot's rvol baseline
     (already cumulative-through-this-slot from the rvol_baseline table).
  3. Enrich the bar with VWAP/EMA9/RelATR/RVOL cumulative.
  4. Append the enriched bar to session history.
  5. Persist to livestream + intraday_bars (concurrent).
  6. Emit the enriched bar via an optional callback -- this is the hook the
     service layer plugs into to run strategies + push SSE events.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, Optional

import asyncpg

from backend.database.writers import insert_intraday_bar, insert_livestream_bar
from backend.datapipe.calculations import enrich_bar
from backend.datapipe.schemas import Bar1m
from backend.datapipe.session_state import SessionStore, SymbolSessionState
from backend.datapipe.time_utils import et_time_slot, session_date_et, to_helsinki

logger = logging.getLogger(__name__)


# Callback signature: async fn taking the enriched Bar1m. Return value ignored.
BarSink = Callable[[Bar1m], Awaitable[None]]


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

    slot = et_time_slot(bar.ts)
    slot_avg = st.baseline_for_slot(slot)

    enriched = enrich_bar(
        new_bar=bar,
        history=st.history,
        atr=st.atr,
        baseline_slot_avg=slot_avg,
        baseline_history_sum=st.baseline_history_sum,
    )

    logger.info("[calc] %s %s ", bar.symbol, to_helsinki(bar.ts).strftime("%H:%M"))

    # Persist first; the sink shouldn't see a bar that isn't in the DB yet.
    await asyncio.gather(
        insert_livestream_bar(pool, enriched),
        insert_intraday_bar(pool, enriched),
    )

    st.history.append(enriched)
    # Advance the running baseline sum so the NEXT bar sees this slot's
    # per-bar average included in its denominator.
    st.baseline_history_sum += slot_avg

    if sink is not None:
        try:
            await sink(enriched)
        except Exception:
            logger.exception("bar sink failed for %s @ %s", enriched.symbol, enriched.ts)

    return enriched
