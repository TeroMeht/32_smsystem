"""
Process-wide runtime dependencies for the FastAPI app.

Currently just the asyncpg connection pool -- created at lifespan
startup, closed at shutdown, and injected into API handlers via
``get_pool()``. Kept out of ``backend.database`` so the database module
stays focused on read/write functions and SQL.

``backend.database.connection.connect()`` (psycopg2 sync) remains for
one-shot scripts like the weekly universe refresh; anything running
inside the FastAPI lifespan should go through this async pool.

Pattern mirrors 22_WatchlistStreamer/src/dependencies.py: one
process-wide pool created at startup, closed at shutdown.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

import asyncpg

from backend.core.config import settings

logger = logging.getLogger(__name__)

_pool: Optional[asyncpg.Pool] = None


async def init_pool(min_size: int = 2, max_size: int = 20) -> asyncpg.Pool:
    """Create the pool once. Idempotent -- returns the existing pool if set."""
    global _pool
    if _pool is not None:
        logger.debug("Init_pool called but pool already exists -- reusing")
        return _pool

    _pool = await asyncpg.create_pool(
        dsn=settings.DATABASE_URL,
        min_size=min_size,
        max_size=max_size,
    )
    logger.debug("DB Pool created")
    return _pool


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialized -- call init_pool() first")
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        logger.debug("DB Pool closing")
        await _pool.close()
        _pool = None
        logger.debug("DB Pool closed")
