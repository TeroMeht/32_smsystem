"""
Process-wide runtime dependencies for the FastAPI app.

The DB pool and REST client are created and owned by ``main.py``'s
lifespan, stashed on ``app.state``, and reached from route handlers via
the ``get_pool`` / ``get_rest`` FastAPI ``Depends`` helpers below.

The datapipe (livestream, historian) receives them as explicit
parameters -- see ``pipeline.startup(app, pool, rest)`` -- so business
logic never touches ``app.state`` either. Only route handlers and the
lifespan interact with the shared infra through this module.

``RestClient`` is a small state container -- session + api key + base
url -- kept here so the factory + the dataclass live in one place; the
actual HTTP call bodies live in ``backend.datapipe.sources.datasource``
as free functions that take a ``RestClient``.

Kept out of ``backend.database`` so that module stays focused on SQL.
``backend.database.connection.connect()`` (psycopg2 sync) remains for
one-shot batch scripts; anything running inside the FastAPI lifespan
should go through the async singletons plumbed via ``app.state`` here.
"""

from __future__ import annotations

from dataclasses import dataclass

import aiohttp
import asyncpg
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
# REST client (Polygon)
# ---------------------------------------------------------------------------


@dataclass
class RestClient:
    session: aiohttp.ClientSession
    api_key: str
    base_url: str


async def create_rest_client(request_timeout_s: float = 30.0) -> RestClient:
    """
    Create the REST client. Called ONCE by lifespan; closed there too.

    Async because ``aiohttp.ClientSession`` prefers being instantiated
    inside a running event loop. A hard per-request timeout is critical
    -- without it a single slow/hung symbol can pin a worker slot
    forever (the historian's bounded semaphore then wedges the whole
    backfill).
    """
    session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=request_timeout_s),
    )
    return RestClient(
        session=session,
        api_key=settings.POLYGON_API_KEY,
        base_url=settings.POLYGON_BASE_URL.rstrip("/"),
    )


def get_rest(request: Request) -> RestClient:
    """FastAPI ``Depends`` helper for route handlers."""
    return request.app.state.rest
