"""
FastAPI entrypoint and composition root.

This file is the ONLY place where long-lived infra resources are created
and destroyed:

  * DB pool     -- init_pool() / close_pool() from backend.dependencies
  * REST client -- RestClient() / .close()

Both are stashed on ``app.state`` so any route or task can reach them
without module-level globals, and both are torn down in the lifespan's
``finally`` block regardless of whether startup succeeded.

The datapipe is pure orchestration -- ``pipeline.startup(app, pool, rest)``
receives the infra as arguments and only owns the background livestream/
replay task (stashed on ``app.state.live_task``).

Mode selection is driven entirely by ``settings.MODE`` / env; there is no
runtime endpoint to switch or trigger replays.

All HTTP routes live in ``backend.routers.*`` and are composed here via
``include_router``. Route bodies MUST NOT contain SQL -- all persistence
goes through ``backend.database``.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.responses import Response

from backend.common.logging_config import setup_app_logging
from backend.datapipe import pipeline
from backend.dependencies import (
    close_db_pool,
    close_rest_client,
    init_db_pool,
    init_rest_client,
)
from backend.routers import livestream as livestream_routes
from backend.routers import pages as pages_routes


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

    # Infra lifecycle (pool + REST client) is owned entirely here via the
    # singletons in backend.dependencies. Created BEFORE pipeline.startup
    # so business logic sees them ready; torn down in ``finally``
    # regardless of startup success.
    pool = await init_db_pool()
    rest = await init_rest_client()
    app.state.pool = pool
    app.state.rest = rest

    try:
        await pipeline.startup(app, pool, rest)
        yield
    finally:
        logger.info("32_smsystem shutting down")
        await pipeline.shutdown(app)   # cancels the background task only
        await close_rest_client()
        await close_db_pool()
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
app.include_router(pages_routes.router)
