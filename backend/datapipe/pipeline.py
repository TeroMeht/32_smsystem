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
    """
    Live mode startup. Idempotent -- safe to re-call if pool already exists.

    Any failure between RestClient() and create_task() must not leak the
    aiohttp session. We install a scoped try/except so a failed startup
    releases the client + pool cleanly before re-raising.
    """
    global _rest, _live_task

    logger.info("[pipeline] step 1/5: initializing asyncpg pool")
    pool = await init_pool()
    logger.info("[pipeline] step 1/5: pool ready")

    _rest = RestClient()
    try:
        today = date.today()

        logger.info("[pipeline] step 2/5: pruning partitions older than retention (today=%s)", today)
        await drop_old_partitions(pool, today)

        logger.info("[pipeline] step 3/5: historian backfill starting")
        await backfill_all_symbols(pool, _rest, today)
        logger.info("[pipeline] step 3/5: historian backfill complete")

        logger.info("[pipeline] step 4/5: truncating livestream (fresh session)")
        await truncate_livestream(pool)

        logger.info("[pipeline] step 5/5: spawning livestream task")
        _live_task = asyncio.create_task(run_livestream(pool, sink=sink))
        logger.info("[pipeline] live startup complete -- livestream running in background")
    except Exception:
        logger.exception("[pipeline] startup_live FAILED -- releasing resources")
        await _rest.close()
        _rest = None
        await close_pool()
        raise


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
        logger.info("[pipeline] cancelling livestream task")
        _live_task.cancel()
        try:
            await _live_task
        except (asyncio.CancelledError, Exception):
            pass
        _live_task = None
    if _rest is not None:
        logger.info("[pipeline] closing REST client")
        await _rest.close()
        _rest = None
    logger.info("[pipeline] closing DB pool")
    await close_pool()
    logger.info("[pipeline] shutdown complete")
