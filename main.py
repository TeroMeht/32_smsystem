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

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import date

from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel

from backend.common.logging_config import setup_logging
from backend.datapipe import pipeline
from backend.datapipe.replay import ReplayConfig

logger = logging.getLogger("32_smsystem")


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging("32_smsystem", log_dir=__import__("pathlib").Path("logs"))
    await pipeline.startup_live()
    yield
    await pipeline.shutdown()


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
