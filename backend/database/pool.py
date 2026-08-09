"""
Global asyncpg connection pool for the live datapipe path.

``connection.connect()`` (psycopg2) is kept for one-shot scripts like the
weekly universe refresh; anything running inside the FastAPI lifespan
should go through the async pool set up here.

Pattern mirrors 22_WatchlistStreamer/src/dependencies.py: one process-wide
pool created at startup, closed at shutdown.
"""

from __future__ import annotations

from typing import Optional

import asyncpg

from backend.core.config import settings

_pool: Optional[asyncpg.Pool] = None


async def init_pool(min_size: int = 2, max_size: int = 20) -> asyncpg.Pool:
    """Create the pool once. Idempotent -- returns the existing pool if set."""
    global _pool
    if _pool is not None:
        return _pool
    _pool = await asyncpg.create_pool(
        dsn=settings.DATABASE_URL,
        min_size=min_size,
        max_size=max_size,
    )
    return _pool


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialized -- call init_pool() first")
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
