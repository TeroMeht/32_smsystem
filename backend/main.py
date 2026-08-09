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

from fastapi import BackgroundTasks, FastAPI
from pydantic import BaseModel

from backend.common.logging_config import setup_app_logging
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
    day: date
    speed: float = 1.0
    lookback_days: int = 5


@app.post("/replay")
async def trigger_replay(req: ReplayRequest, background_tasks: BackgroundTasks):
    """Kick off a replay in the background. Returns immediately."""
    cfg = ReplayConfig(day=req.day, speed=req.speed, lookback_days=req.lookback_days)
    background_tasks.add_task(pipeline.startup_replay, cfg)
    return {"status": "scheduled", "day": req.day.isoformat(), "speed": req.speed}


@app.get("/health")
async def health():
    return {"ok": True}
