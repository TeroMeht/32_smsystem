"""
Top-level orchestrator for the datapipe.

One function per lifecycle event, called from the FastAPI lifespan (see
main.py). Keeps ``main.py`` free of pipeline plumbing details.

Sequence on startup:

    1. init_pool           -- asyncpg pool up
    2. drop_old_partitions -- respect retention before we write
    3. backfill_all_symbols (historian) -- daily + intraday + rvol_baseline
    4. empty_livestream_table -- fresh session
    5. run_livestream      -- WS AM consumer, background task

Replay mode replaces step 5 with ``run_replay(cfg)`` -- everything up to
that point is identical, so replay starts with the same warm state a live
session would.
"""

from __future__ import annotations

import asyncio
import logging

from backend.core.config import settings
from backend.database.partitions import drop_old_partitions
from backend.database.pool import close_pool, init_pool
from backend.database.readers import get_last_backfill_run, load_active_symbol_map
from backend.database.writers import empty_livestream_table
from backend.datapipe.runtime.bar_processor import BarSink
from backend.datapipe.historian import backfill_all_symbols
from backend.datapipe.runtime.livestream import run_livestream
from backend.datapipe.runtime.replay import ReplayConfig, run_replay
from backend.datapipe.sources.rest_client import RestClient
from backend.datapipe.time_utils import effective_today, helsinki_hhmm_to_utc

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# process-lifetime handles (populated by ``startup``, released by ``shutdown``)
# ---------------------------------------------------------------------------


_rest: RestClient | None = None
_live_task: asyncio.Task | None = None


async def startup(sink: BarSink | None = None) -> None:
    global _rest, _live_task

    mode = settings.MODE
    today = effective_today()
    logger.info("MODE = %s effective_today = %s", mode, today)
    pool = await init_pool()

    # Fail fast: nothing further makes sense without an active universe.
    symbol_map = await load_active_symbol_map(pool)
    if not symbol_map:
        await close_pool()
        raise RuntimeError("no active symbols in monitored_symbols -- refusing to start")
    logger.info("%d active symbols", len(symbol_map))

    _rest = RestClient()
    try:
        await drop_old_partitions(pool, today)
        await empty_livestream_table(pool)

        # TESTING: backfill_status freshness gate is temporarily disabled --
        # historian runs on every startup regardless of when it last ran.
        # Restore the block below when done testing.
        #
        last_daily, last_intraday = await get_last_backfill_run(pool)
        need_daily    = last_daily    is None or last_daily.date()    < today
        need_intraday = last_intraday is None or last_intraday.date() < today
        
        if need_daily or need_intraday:
            await backfill_all_symbols(
                pool, _rest, today, symbol_map,
                need_daily=need_daily,
                need_intraday=need_intraday,
                replay_mode=(mode == "replay"),
            )
            logger.info("Historian backfill complete")
        else:
            logger.info(
                "Backfill already ran today "
                "(daily @ %s, intraday @ %s)",
                last_daily, last_intraday,
            )

        # await backfill_all_symbols(
        #     pool, _rest, today, symbol_map,
        #     need_daily=True,
        #     need_intraday=True,
        #     replay_mode=(mode == "replay"),
        # )
        # logger.info("Historian backfill complete (freshness gate disabled)")

        if mode == "replay":
            start_utc = (
                helsinki_hhmm_to_utc(settings.REPLAY_DAY, settings.REPLAY_START_TIME)
                if settings.REPLAY_START_TIME else None
            )
            cfg = ReplayConfig(
                day=settings.REPLAY_DAY,
                speed=settings.REPLAY_SPEED,
                start_utc=start_utc,
            )
            _live_task = asyncio.create_task(
                run_replay(pool, _rest, cfg, symbol_map, sink=sink),
            )
            logger.info("Replay startup complete")
        else:
            _live_task = asyncio.create_task(
                run_livestream(pool, _rest, symbol_map, sink=sink),
            )
            logger.info("Live market data startup complete")
    except Exception:
        logger.exception("Startup FAILED -- releasing resources")
        await _rest.close()
        _rest = None
        await close_pool()
        raise


async def shutdown() -> None:
    """Cancel background tasks + release network resources."""
    global _rest, _live_task
    if _live_task is not None and not _live_task.done():
        logger.info("Cancelling livestream/replay task")
        _live_task.cancel()
        try:
            await _live_task
        except (asyncio.CancelledError, Exception):
            pass
        _live_task = None
    if _rest is not None:
        logger.info("Closing REST client")
        await _rest.close()
        _rest = None
    logger.info("Closing DB pool")
    await close_pool()
    logger.info("Shutdown complete")
