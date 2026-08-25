"""
Top-level orchestrator for the datapipe.

One function per lifecycle event, called from the FastAPI lifespan (see
main.py). Keeps ``main.py`` focused on infra lifecycle (pool, polygon
source) and this module focused on business logic (universe check,
retention cleanup, backfill, spawn the livestream task).

Sequence on startup:

    1. load_active_symbol_map -- fail fast on empty universe
    2. data_cleanup           -- respect retention before we write
    3. backfill_all_symbols   -- daily + intraday + rvol_baseline
                                 (skipped if backfill_status shows same-day)
    4. empty_livestream_table -- fresh session
    5. run_livestream         -- background task

Long-lived infra resources (pool, PolygonSource) are created + torn down
in ``main.py``'s lifespan and passed in. The only lifetime we own here
is the background asyncio.Task, stashed on ``app.state.live_task``.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date

import asyncpg
from fastapi import FastAPI

from backend.database.partitions import data_cleanup
from backend.database.readers import get_last_backfill_run, load_active_symbol_map
from backend.database.writers import empty_livestream_table
from backend.datapipe.historian import backfill_all_symbols
from backend.datapipe.runtime.bar_processor import BarSink
from backend.datapipe.runtime.livestream import run_livestream
from data_sources.polygon import PolygonSource

logger = logging.getLogger(__name__)


async def startup(
    app: FastAPI,
    pool: asyncpg.Pool,
    polygon: PolygonSource,
    sink: BarSink | None = None,
) -> None:
    """
    Run the startup sequence and spawn the background livestream task.

    The task handle is stashed on ``app.state.live_task`` so ``shutdown``
    can cancel it. All exceptions propagate to the caller; infra cleanup
    (pool, polygon) is main.py's ``finally``.
    """
    today = date.today()
    logger.info("today = %s", today)

    # Fail fast: nothing further makes sense without an active universe.
    symbol_map = await load_active_symbol_map(pool)
    if not symbol_map:
        raise RuntimeError("no active symbols in monitored_symbols -- refusing to start")
    logger.info("%d active symbols", len(symbol_map))

    app.state.live_task = None

    await data_cleanup(pool, today)
    await empty_livestream_table(pool)


    last_daily, last_intraday = await get_last_backfill_run(pool)
    need_daily    = last_daily    is None or last_daily.date()    < today
    need_intraday = last_intraday is None or last_intraday.date() < today

    if need_daily or need_intraday:
        await backfill_all_symbols(
            pool, polygon, today, symbol_map,
            need_daily=need_daily,
            need_intraday=need_intraday,
        )
        logger.info("Historian backfill complete")
    else:
        logger.info(
            "Backfill already ran today "
            "(daily @ %s, intraday @ %s)",
            last_daily, last_intraday,
        )

    app.state.live_task = asyncio.create_task(run_livestream(pool, polygon, symbol_map, sink=sink))

    logger.info("Live market data startup complete")


async def shutdown(app: FastAPI) -> None:
    """
    Cancel the background livestream task. Infra teardown (polygon close,
    pool close) is main.py's responsibility.
    """
    live_task: asyncio.Task | None = getattr(app.state, "live_task", None)
    if live_task is not None and not live_task.done():
        logger.info("Cancelling livestream task")
        live_task.cancel()
        try:
            await live_task
        except (asyncio.CancelledError, Exception):
            pass
        app.state.live_task = None
