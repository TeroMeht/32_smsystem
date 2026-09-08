"""
Alarms dispatcher -- the ``BarSink`` the datapipe fires per bar.

``bar_processor.process_bar`` accepts an optional ``sink: BarSink``
callback; ``main.lifespan`` builds one via ``build_alarm_sink`` and
hands it to ``pipeline.startup``. Every enriched ``CandleRow`` then
lands here after it has been written to the livestream table.

Per bar the dispatcher:
    1. lazily seeds ``AlarmState`` on the first bar (loads SMA200 and
       cum_volume from the DB -- deferred until here so REST-primed
       bars from ``_initialize_livestream`` are already in the table),
    2. folds the bar's volume into the running cum_volume,
    3. runs each registered alarm strategy against (bar, state),
       sequentially and order-stable,
    4. each strategy owns its own detection and delegates dedupe +
       Telegram to ``alarm_generator.generate_signal_alarm``.

Adding a new alarm strategy: drop a file in ``strategies/`` exposing
``async def run(bar, state)`` and append it to ``_STRATEGIES``.
"""

from __future__ import annotations

import logging
from typing import Awaitable, Callable, Iterable, List

import asyncpg

from backend.alarms.state import AlarmState
from backend.alarms.strategies import capitulation
from backend.datapipe.runtime.bar_processor import BarSink
from indicators.candle_row import CandleRow

logger = logging.getLogger(__name__)


# Signature: async fn taking (bar, state) -- state is the session-scoped
# AlarmState the dispatcher owns. Strategies read sma200 / cum_volume
# off it without knowing where either number came from.
StrategyFn = Callable[[CandleRow, AlarmState], Awaitable[None]]


# One entry per active alarm strategy. Order = evaluation order per bar.
_STRATEGIES: List[StrategyFn] = [
    capitulation.run,
]


def build_alarm_sink(
    pool: asyncpg.Pool,
    strategies: Iterable[StrategyFn] | None = None,
) -> BarSink:
    """
    Build the ``BarSink`` closure. Synchronous -- the state's DB seed
    is deferred to first-bar via ``state.ensure_seeded()`` so the sink
    doesn't need to be awaited at build time and startup ordering
    stays a lifespan concern.

    Each strategy is awaited under its own try/except so a bad
    strategy can't starve the rest of the pipeline. ``process_bar``
    already guards the sink call itself, so this is defense in depth.
    """
    state = AlarmState(pool)
    strats = list(strategies) if strategies is not None else list(_STRATEGIES)

    async def sink(bar: CandleRow) -> None:
        # Lazy seed on first bar: by now _initialize_livestream has
        # persisted today's REST-primed bars, so cum_volume reads a
        # correct snapshot rather than the empty post-truncate table.
        await state.ensure_seeded()

        # Update running state BEFORE strategies so they see this bar's
        # volume in the cum_volume they read.
        state.add_bar(bar)
        for run in strats:
            try:
                await run(bar, state)
            except Exception:
                logger.exception(
                    "alarm strategy %s failed for %s @ %s",
                    getattr(run, "__module__", "?"), bar.symbol, bar.ts,
                )

    return sink
