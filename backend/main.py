"""
FastAPI entrypoint.

  * On lifespan startup, call ``datapipe.pipeline.startup`` -- this
    creates the DB pool, warms the historian, and spawns the background
    task (WS livestream in live mode, replay driver in replay mode).
  * On lifespan shutdown, cancel it via ``datapipe.pipeline.shutdown``.

Mode selection is driven entirely by settings.MODE / env; there is no
runtime endpoint to switch or trigger replays.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from backend.common.logging_config import setup_app_logging
from backend.core.config import settings
from backend.database.pool import get_pool
from backend.database.readers import (
    load_latest_livestream_per_symbol,
    load_livestream_bars_for_symbol,
)
from backend.datapipe import pipeline


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
        await pipeline.startup()
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


@app.get("/health")
async def health():
    return {"ok": True, "mode": settings.MODE, "replay_day": (
        settings.REPLAY_DAY.isoformat() if settings.REPLAY_DAY else None
    )}


# ---------------------------------------------------------------------------
# /relatr -- live dashboard: top-N latest RelATR per symbol, polling refresh
# ---------------------------------------------------------------------------


@app.get("/api/livestream/top")
async def api_livestream_top():
    """
    Latest livestream row per symbol -- ALL rows, unfiltered, unsorted.

    Display concerns (volume floor, RVOL floor, RelATR floor, sort order,
    row cap) live entirely in the frontend so they can be tweaked without
    touching backend code.
    """
    pool = get_pool()
    rows = await load_latest_livestream_per_symbol(pool)
    return {"rows": rows}


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
