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

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from backend.common.logging_config import setup_app_logging
from backend.core.config import settings
from backend.database.pool import get_pool
from backend.database.readers import (
    load_livestream_bars_for_symbol,
    load_top_relatr,
)
from backend.datapipe import pipeline
from backend.datapipe.replay import ReplayConfig


# ---------------------------------------------------------------------------
# Frontend directory -- resolved from this file's location so it works no
# matter what CWD uvicorn is launched from.
# Layout: <repo>/frontend/*.html  (UI files, decoupled from backend code).
# ---------------------------------------------------------------------------
FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"

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

# Serve everything under frontend/ at /ui/ so relatr.html can reference
# sibling modules with an absolute path (e.g. /ui/chart.js) that survives
# whatever URL the page itself is served from.
app.mount("/ui", StaticFiles(directory=FRONTEND_DIR), name="frontend")


class ReplayRequest(BaseModel):
    day: date            # ET session date to replay, e.g. "2026-08-07"
    speed: float # default 60x = 1 replay minute per real second
    lookback_days: int
    sample_sessions: int


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


# ---------------------------------------------------------------------------
# /relatr -- live dashboard: top-N latest RelATR per symbol, polling refresh
# ---------------------------------------------------------------------------


@app.get("/api/livestream/top")
async def api_livestream_top(
    n: int = Query(20, ge=1, le=200),
    order: str = Query("desc", pattern="^(desc|abs)$"),
    min_volume: int = Query(10_000, ge=0),
    min_rvol: float = Query(2.0, ge=0.0),
):
    """
    Top N symbols by RelATR from ``livestream``. Filters:
      * volume    >= min_volume  (default 10,000)
      * rvol_cum  >= min_rvol    (default 2.0)
    """
    pool = get_pool()
    rows = await load_top_relatr(
        pool, n=n, order=order,
        min_volume=min_volume, min_rvol=min_rvol,
    )
    return {
        "n": n, "order": order,
        "min_volume": min_volume, "min_rvol": min_rvol,
        "rows": rows,
    }


@app.get("/api/livestream/bars/{symbol}")
async def api_livestream_bars(symbol: str):
    """
    Every livestream row currently on disk for the symbol, ordered by ts.
    Since livestream is truncated at session start, this is the current
    session in progress -- feeds the frontend candlestick chart on hover.
    """
    pool = get_pool()
    rows = await load_livestream_bars_for_symbol(pool, symbol.upper())
    return {"symbol": symbol.upper(), "bars": rows}


# ---------------------------------------------------------------------------
# Frontend page -- served from the frontend/ folder as a plain file so
# main.py stays free of UI markup.
# ---------------------------------------------------------------------------


@app.get("/relatr")
async def relatr_page():
    path = FRONTEND_DIR / "relatr.html"
    if not path.exists():
        raise HTTPException(status_code=500, detail=f"UI file missing: {path}")
    return FileResponse(path, media_type="text/html")
