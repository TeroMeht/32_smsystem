"""
FastAPI entrypoint and composition root.

This file is the ONLY place where long-lived infra resources are created
and destroyed:

  * DB pool        -- create_db_pool() / pool.close()
  * Polygon source -- create_polygon()  / close_polygon(source)

Both are stashed on ``app.state`` so any route can reach them via the
``get_pool`` / ``get_polygon`` ``Depends`` helpers in
``backend.dependencies``. Nothing lives at module scope -- the
lifespan's locals are the single source of truth, and both resources
are torn down in the lifespan's ``finally`` block regardless of whether
startup succeeded.

The datapipe is pure orchestration -- ``pipeline.startup(app, pool, polygon)``
receives the infra as arguments and only owns the background livestream
task (stashed on ``app.state.live_task``).

All HTTP routes live in ``backend.routers.*`` and are composed here via
``include_router``. Route bodies MUST NOT contain SQL -- all persistence
goes through ``backend.database``.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.responses import Response

from backend.alarms.dispatcher import build_alarm_sink
from backend.common.logging_config import setup_app_logging
from backend.datapipe import pipeline
from backend.dependencies import close_polygon, create_db_pool, create_polygon
from backend.alarms.alarm_generator import drain_pending as drain_alarms
from backend.routers import alarms as alarms_routes
from backend.routers import livestream as livestream_routes
from backend.routers import pages as pages_routes


# ---------------------------------------------------------------------------
# Frontend directory -- resolved from this file's location so it works no
# matter what CWD uvicorn is launched from.
# Layout: <repo>/frontend/*.html  (UI files, decoupled from backend code).
# ---------------------------------------------------------------------------
FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"

# Logging is configured at import time (not in the lifespan) so uvicorn's
# own startup lines ("Started server process", ...) also land in
# logs/smsystem.log -- uvicorn imports this module before logging them.
setup_app_logging()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("=" * 72)
    logger.info("32_smsystem starting up")
    logger.info("=" * 72)

    pool = await create_db_pool()
    polygon = await create_polygon()
    app.state.pool = pool
    app.state.polygon = polygon


    alarm_sink = build_alarm_sink(pool)

    try:
        await pipeline.startup(app, pool, polygon, sink=alarm_sink)
        # Print the dashboard URL once the datapipe is up. Reads HOST/PORT
        # from the environment (set by start.bat) with 127.0.0.1:8000
        # defaults, so a manual `uvicorn --port 8001` invocation should
        # also export PORT=8001 to keep this line accurate.
        _host = os.environ.get("HOST", "127.0.0.1")
        _port = os.environ.get("PORT", "8001")
        logger.info("UI ready: http://%s:%s/ui/relatr.html", _host, _port)
        yield
    finally:
        logger.info("32_smsystem shutting down")
        await pipeline.shutdown(app)   # cancels the background task only
        await drain_alarms(timeout=15)  # let in-flight Telegram sends finish
        await close_polygon(polygon)
        await pool.close()
        logger.info("32_smsystem shutdown complete")


app = FastAPI(title="32_smsystem", lifespan=lifespan)


class NoCacheStaticFiles(StaticFiles):
    """
    StaticFiles that sends ``Cache-Control: no-store`` on every response.

    We iterate on the UI files (relatr.html, chart.js) constantly; without
    this the browser silently serves the cached copy on reload, which
    makes CSS/JS tweaks look like they "didn't apply". Cost is negligible
    -- the files are small and only fetched by the dashboard tab.
    """

    async def get_response(self, path, scope):
        resp: Response = await super().get_response(path, scope)
        resp.headers["Cache-Control"] = "no-store"
        return resp


# Serve everything under frontend/ at /ui/ so relatr.html can reference
# sibling modules with an absolute path (e.g. /ui/chart.js) that survives
# whatever URL the page itself is served from.
app.mount("/ui", NoCacheStaticFiles(directory=FRONTEND_DIR), name="frontend")


# ---------------------------------------------------------------------------
# Route composition -- all endpoints live under backend/routers/.
# ---------------------------------------------------------------------------
app.include_router(livestream_routes.router)
app.include_router(alarms_routes.router)
app.include_router(pages_routes.router)
