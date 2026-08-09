"""
FastAPI entrypoint.

Two responsibilities:

  * On lifespan startup, call ``datapipe.pipeline.startup_live`` -- this
    creates the DB pool, warms the historian, and spawns the WS livestream
    background task.
  * On lifespan shutdown, cancel it via ``datapipe.pipeline.shutdown``.

The replay endpoint (``POST /replay``) starts a replay run in the
background. Live mode and replay mode aren't run at the same time in
practice; the endpoint is provided as an operator escape hatch. In a
production deploy you'd flip this via a config flag instead.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path

from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel

from backend.common.logging_config import setup_app_logging
from backend.core.config import settings
from backend.datapipe import pipeline
from backend.datapipe.replay import ReplayConfig

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Root-logger setup so every backend.* module surfaces to stdout + logs/app.log.
    # Called first so a startup failure below is captured in the log.
    setup_app_logging(log_dir=Path("logs"))
    logger.info("=" * 72)
    logger.info("32_smsystem starting up")
    logger.info("=" * 72)
    try:
        await pipeline.startup_live()
        logger.info("32_smsystem ready -- serving HTTP")
        yield
    finally:
        logger.info("32_smsystem shutting down")
        await pipeline.shutdown()
        logger.info("32_smsystem shutdown complete")


app = FastAPI(title="32_smsystem", lifespan=lifespan)


class ReplayRequest(BaseModel):
    day: date            # ET session date to replay, e.g. "2026-08-07"
    speed: float = 60.0  # default 60x = 1 replay minute per real second
    lookback_days: int = 8
    sample_sessions: int = 5


@app.post("/replay")
async def trigger_replay(req: ReplayRequest, background_tasks: BackgroundTasks):
    """
    Ad-hoc replay trigger. Only usable when the app was started in
    MODE=live -- in MODE=replay a replay is already running from startup
    and a second one would clobber the shared session state.
    """
    if settings.MODE == "replay":
        raise HTTPException(
            status_code=409,
            detail=(
                "App is running in MODE=replay -- a replay is already active "
                "(REPLAY_DAY=%s). To run a different replay, restart with "
                "different env vars." % settings.REPLAY_DAY
            ),
        )
    cfg = ReplayConfig(
        day=req.day,
        speed=req.speed,
        lookback_days=req.lookback_days,
        sample_sessions=req.sample_sessions,
    )
    logger.info(
        "[api] /replay requested: day=%s speed=%.2f lookback=%dd sample_sessions=%d",
        req.day, req.speed, req.lookback_days, req.sample_sessions,
    )
    background_tasks.add_task(pipeline.startup_replay, cfg)
    return {
        "status": "scheduled",
        "day": req.day.isoformat(),
        "speed": req.speed,
        "note": "follow progress in the app logs",
    }


@app.get("/health")
async def health():
    return {"ok": True, "mode": settings.MODE, "replay_day": (
        settings.REPLAY_DAY.isoformat() if settings.REPLAY_DAY else None
    )}
