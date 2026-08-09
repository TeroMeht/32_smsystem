"""
Top-level orchestrator for the datapipe.

One function per lifecycle event, called from the FastAPI lifespan (see
main.py). Keeps ``main.py`` free of pipeline plumbing details.

Sequence on startup:

    1. init_pool           -- asyncpg pool up
    2. drop_old_partitions -- respect retention before we write
    3. backfill_all_symbols (historian) -- daily + intraday + rvol_baseline
    4. truncate_livestream -- fresh session
    5. run_livestream      -- WS AM consumer, background task

Replay mode replaces step 5 with ``run_replay(cfg)`` -- everything up to
that point is identical, so replay starts with the same warm state a live
session would.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date

from backend.database.partitions import drop_old_partitions
from backend.database.pool import close_pool, init_pool
from backend.database.writers import truncate_livestream
from backend.datapipe.bar_processor import BarSink
from backend.datapipe.historian import backfill_all_symbols
from backend.datapipe.livestream import run_livestream
from backend.datapipe.replay import ReplayConfig, run_replay
from backend.datapipe.rest_client import RestClient

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# process-lifetime handles (populated by ``startup``, released by ``shutdown``)
# ---------------------------------------------------------------------------


_rest: RestClient | None = None
_live_task: asyncio.Task | None = None


async def startup_live(sink: BarSink | None = None) -> None:
    """Live mode startup. Idempotent -- safe to re-call if pool already exists."""
    global _rest, _live_task

    pool = await init_pool()
    _rest = RestClient()
    today = date.today()

    await drop_old_partitions(pool, today)
    await backfill_all_symbols(pool, _rest, today)
    await truncate_livestream(pool)

    _live_task = asyncio.create_task(run_livestream(pool, sink=sink))
    logger.info("datapipe: live stream task started")


async def startup_replay(cfg: ReplayConfig, sink: BarSink | None = None) -> None:
    """
    Replay mode: warm the DB from REST, then feed bars through the shared
    processor. Blocks until the replay finishes (unlike live mode).
    """
    global _rest
    pool = await init_pool()
    _rest = RestClient()
    try:
        await run_replay(pool, _rest, cfg, sink=sink)
    finally:
        await _rest.close()


async def shutdown() -> None:
    """Cancel background tasks + release network resources."""
    global _rest, _live_task
    if _live_task is not None and not _live_task.done():
        _live_task.cancel()
        try:
            await _live_task
        except (asyncio.CancelledError, Exception):
            pass
        _live_task = None
    if _rest is not None:
        await _rest.close()
        _rest = None
    await close_pool()
    logger.info("datapipe: shutdown complete")
