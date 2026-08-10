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

from backend.core.config import settings
from backend.database.partitions import drop_old_partitions
from backend.database.pool import close_pool, init_pool
from backend.database.writers import truncate_livestream
from backend.datapipe.bar_processor import BarSink
from backend.datapipe.historian import backfill_all_symbols
from backend.datapipe.livestream import run_livestream
from backend.datapipe.replay import ReplayConfig, run_replay
from backend.datapipe.rest_client import RestClient
from backend.datapipe.time_utils import effective_today, helsinki_hhmm_to_utc

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# process-lifetime handles (populated by ``startup``, released by ``shutdown``)
# ---------------------------------------------------------------------------


_rest: RestClient | None = None
_live_task: asyncio.Task | None = None


async def startup_live(sink: BarSink | None = None) -> None:
    """
    Unified startup for both MODE=live and MODE=replay. Named
    ``startup_live`` for backward compatibility with main.py; the actual
    mode is chosen from settings.MODE:

      * live   -> historian aligned to wall-clock today, WS livestream in
                  background.
      * replay -> historian aligned to settings.REPLAY_DAY, then replay
                  driver in background (no WS is opened).

    Any failure between RestClient() and create_task() releases the
    aiohttp session and pool cleanly before re-raising.
    """
    global _rest, _live_task

    mode = settings.MODE
    today = effective_today()
    logger.info("[pipeline] MODE=%s effective_today=%s", mode, today)

    logger.info("[pipeline] step 1/5: initializing asyncpg pool")
    pool = await init_pool()
    logger.info("[pipeline] step 1/5: pool ready")

    _rest = RestClient()
    try:
        logger.info("[pipeline] step 2/5: pruning partitions older than retention (today=%s)", today)
        await drop_old_partitions(pool, today)

        logger.info("[pipeline] step 3/5: historian backfill starting")
        await backfill_all_symbols(pool, _rest, today, replay_mode=(mode == "replay"))
        logger.info("[pipeline] step 3/5: historian backfill complete")

        # Truncate unconditionally so priming writes into a clean table.
        # Every startup MUST re-pull today's bars from REST so any gaps
        # left by the previous WS session get filled in.
        logger.info("[pipeline] step 4/5: truncating livestream (fresh session; will be re-primed)")
        await truncate_livestream(pool)

        if mode == "replay":
            start_raw = (settings.REPLAY_START_TIME or "").strip()
            start_utc = (
                helsinki_hhmm_to_utc(settings.REPLAY_DAY, start_raw)
                if start_raw else None
            )
            cfg = ReplayConfig(
                day=settings.REPLAY_DAY,
                speed=settings.REPLAY_SPEED,
                lookback_days=settings.REPLAY_LOOKBACK_DAYS,
                sample_sessions=settings.REPLAY_SAMPLE_SESSIONS,
                start_utc=start_utc,
            )
            logger.info(
                "[pipeline] step 5/5: MODE=replay -- spawning replay task "
                "(day=%s start=%s speed=%.2f lookback=%dd sample_sessions=%d)",
                cfg.day, start_raw or "session-open (00:00 HKI)",
                cfg.speed, cfg.lookback_days, cfg.sample_sessions,
            )
            _live_task = asyncio.create_task(run_replay(pool, _rest, cfg, sink=sink))
            logger.info("[pipeline] replay startup complete -- replay running in background")
        else:
            logger.info("[pipeline] step 5/5: spawning livestream task")
            _live_task = asyncio.create_task(run_livestream(pool, _rest, sink=sink))
            logger.info("[pipeline] live startup complete -- livestream running in background")
    except Exception:
        logger.exception("[pipeline] startup FAILED -- releasing resources")
        await _rest.close()
        _rest = None
        await close_pool()
        raise


async def startup_replay(cfg: ReplayConfig, sink: BarSink | None = None) -> None:
    """
    Replay mode: read bars from ``intraday_bars`` (populated earlier by
    the historian) and stream them through the shared processor. Blocks
    until the replay finishes (unlike live mode).

    Uses a local RestClient so we don't clobber the process-wide ``_rest``
    that the live stream owns -- replay can safely be triggered while the
    livestream task is running.
    """
    pool = await init_pool()
    local_rest = RestClient()
    try:
        await run_replay(pool, local_rest, cfg, sink=sink)
    finally:
        await local_rest.close()


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
