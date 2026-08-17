"""
Persistence + fan-out wrapper around a single incoming Bar.

The per-bar state transition (compute indicators, append to history,
advance running sums) lives on ``SymbolSessionState.apply_bar`` so both
live and replay paths share identical enrichment. This module owns only
the two boundaries the state doesn't touch: writing the enriched bar to
the livestream table and firing the optional strategy/SSE sink.

Used by:
  * ``runtime.livestream._consume``      -- one bar per WS aggregate
  * ``runtime.replay._consume``          -- one bar per intraday_bars row
"""

from __future__ import annotations

import logging
from typing import Awaitable, Callable, Optional

import asyncpg

from backend.database.writers import insert_livestream_bar
from backend.datapipe.runtime.session_state import SessionStore
from backend.datapipe.schemas import Bar

logger = logging.getLogger(__name__)


# Callback signature: async fn taking the enriched Bar. Return value ignored.
BarSink = Callable[[Bar], Awaitable[None]]


async def process_bar(
    pool: asyncpg.Pool,
    store: SessionStore,
    bar: Bar,
    sink: Optional[BarSink] = None,
) -> Bar:
    """
    Enrich ``bar`` via session state, persist to livestream, fire sink.

    Returns the enriched Bar so callers doing synchronous follow-up
    work (unit tests, replay) can inspect the same object the sink saw.
    """
    st = store.get(bar.symbol)
    enriched = st.apply_bar(bar)

    await insert_livestream_bar(pool, enriched)

    if sink is not None:
        try:
            await sink(enriched)
        except Exception:
            logger.exception("bar sink failed for %s @ %s", enriched.symbol, enriched.ts)

    return enriched
