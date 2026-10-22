"""
Persistence + fan-out wrapper around a single incoming CandleRow.

The per-bar state transition (compute indicators, append to history,
advance running sums) lives on ``SymbolSessionState.apply_bar``. This
module adds the EMA9-anchored speed (``calculations.speed``) on top,
then owns the two boundaries the state doesn't touch: writing the
enriched bar (+ speed) to the livestream table and firing the
strategy/SSE sink.

Used by:
  * ``runtime.livestream._consume``      -- one bar per WS aggregate
"""

from __future__ import annotations

import logging
from typing import Awaitable, Callable, Optional

import asyncpg

from backend.database.writers import insert_livestream_bar
from backend.datapipe.calculations.speed import SpeedStore, SpeedValue
from indicators.session_state import SessionStore
from backend.datapipe.schemas import CandleRow

logger = logging.getLogger(__name__)


# Callback signature: async fn taking the enriched CandleRow and its
# speed (SpeedValue). Return value ignored.
BarSink = Callable[[CandleRow, SpeedValue], Awaitable[None]]


async def process_bar(
    pool: asyncpg.Pool,
    store: SessionStore,
    bar: CandleRow,
    sink: BarSink,
    speed_store: SpeedStore,
) -> CandleRow:
    """
    Enrich ``bar`` via session state, add speed, persist to livestream,
    fire sink.

    Returns the enriched CandleRow so callers doing synchronous follow-up
    work (unit tests) can inspect the same object the sink saw.
    """
    st = store.get(bar.symbol)
    enriched = st.apply_bar(bar)
    speed = speed_store.update(enriched)

    await insert_livestream_bar(pool, enriched, speed)


    try:
        await sink(enriched, speed)
    except Exception:
        logger.exception("bar sink failed for %s @ %s", enriched.symbol, enriched.ts)

    return enriched
