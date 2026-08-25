"""
Process-wide runtime dependencies for the FastAPI app.

The DB pool and Polygon source are created and owned by ``main.py``'s
lifespan, stashed on ``app.state``, and reached from route handlers via
the ``get_pool`` / ``get_polygon`` FastAPI ``Depends`` helpers below.

The datapipe (livestream, historian) receives them as explicit
parameters -- see ``pipeline.startup(app, pool, polygon)`` -- so
business logic never touches ``app.state`` either. Only route handlers
and the lifespan interact with the shared infra through this module.

``PolygonSource`` is the source-agnostic handle from
``data_sources.polygon`` -- session + api key + base + WS URL. The
adapter classes (``PolygonHistoricalSource`` /
``PolygonRealtimeSource``) wrap it at the call sites (historian,
livestream) so this module stays a thin infra-owner.

Kept out of ``backend.database`` so that module stays focused on SQL.
``backend.database.connection.connect()`` (psycopg2 sync) remains for
one-shot batch scripts; anything running inside the FastAPI lifespan
should go through the async singletons plumbed via ``app.state`` here.
"""

from __future__ import annotations

import asyncpg
from data_sources.polygon import (
    PolygonSource,
    connect as polygon_connect,
    disconnect as polygon_disconnect,
    from_config as polygon_from_config,
)
from fastapi import Request

from backend.core.config import settings


# ---------------------------------------------------------------------------
# DB pool
# ---------------------------------------------------------------------------


async def create_db_pool(min_size: int = 2, max_size: int = 20) -> asyncpg.Pool:
    """Create the asyncpg pool. Called ONCE by lifespan; closed there too."""
    return await asyncpg.create_pool(
        dsn=settings.DATABASE_URL,
        min_size=min_size,
        max_size=max_size,
    )


def get_pool(request: Request) -> asyncpg.Pool:
    """FastAPI ``Depends`` helper for route handlers."""
    return request.app.state.pool


# ---------------------------------------------------------------------------
# Polygon source (REST + WS)
# ---------------------------------------------------------------------------


async def create_polygon() -> PolygonSource:
    """
    Build the ``PolygonSource`` and open its aiohttp session.

    Called ONCE by the lifespan; ``close_polygon`` runs in the lifespan
    ``finally`` regardless of whether startup succeeded. A hard
    per-request timeout is critical -- without it a single slow/hung
    symbol can pin a worker slot forever (the historian's bounded
    semaphore then wedges the whole backfill). The default lives in
    ``PolygonSourceConfig.POLYGON_REQUEST_TIMEOUT_S`` and is applied
    inside ``polygon.from_config`` when the setting is absent.
    """
    source = polygon_from_config(settings)
    await polygon_connect(source)
    return source


async def close_polygon(source: PolygonSource) -> None:
    """Idempotent teardown -- safe to call in a ``finally``."""
    await polygon_disconnect(source)


def get_polygon(request: Request) -> PolygonSource:
    """FastAPI ``Depends`` helper for route handlers."""
    return request.app.state.polygon
