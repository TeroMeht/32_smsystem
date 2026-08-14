"""
Process-wide runtime dependencies for the FastAPI app.

Two long-lived singletons live here, both owned by main.py's lifespan
and shared by every downstream caller:

  * asyncpg **pool**  -- ``init_pool()`` / ``get_pool()`` / ``close_pool()``
  * ``RestClient``    -- ``init_rest()`` / ``get_rest()`` / ``close_rest()``

Both follow the same shape: created at startup, closed at shutdown,
reachable via the ``get_*`` accessor. The datapipe passes them as
explicit parameters so business logic never touches globals.

``RestClient`` here is a small state container -- session + api key +
base url -- created eagerly inside ``init_rest()``. The actual HTTP
call bodies live in ``backend.datapipe.sources.rest_client`` as free
functions that accept a ``RestClient``, so this module stays about
lifecycle and that module stays about endpoint logic.

Kept out of ``backend.database`` so that module stays focused on SQL.
``backend.database.connection.connect()`` (psycopg2 sync) remains for
one-shot batch scripts (weekly universe refresh); anything running
inside the FastAPI lifespan should go through the async singletons here.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import aiohttp
import asyncpg

from backend.core.config import settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DB pool
# ---------------------------------------------------------------------------

db_pool: Optional[asyncpg.Pool] = None
rest_client: Optional[RestClient] = None

async def init_db_pool(min_size: int = 2, max_size: int = 20) -> asyncpg.Pool:
    """Create the pool once. Idempotent -- returns the existing pool if set."""
    global db_pool
    if db_pool is not None:
        logger.debug("Init_pool called but pool already exists -- reusing")
        return db_pool

    db_pool = await asyncpg.create_pool(
        dsn=settings.DATABASE_URL,
        min_size=min_size,
        max_size=max_size,
    )
    logger.debug("DB Pool created")
    return db_pool


def get_db_pool() -> asyncpg.Pool:
    if db_pool is None:
        raise RuntimeError("DB pool not initialized -- call init_pool() first")
    return db_pool


async def close_db_pool() -> None:
    global db_pool
    if db_pool is not None:
        logger.debug("DB Pool closing")
        await db_pool.close()
        db_pool = None
        logger.debug("DB Pool closed")


# ---------------------------------------------------------------------------
# REST client (Polygon)
# ---------------------------------------------------------------------------


@dataclass
class RestClient:
    """
    State container for one Polygon REST connection: the aiohttp session
    plus the auth + base URL every request needs. Endpoint logic lives in
    ``backend.datapipe.sources.rest_client`` -- functions there take an
    instance of this class rather than being methods on it, so the
    singleton lifecycle stays in one place (here).
    """

    session: aiohttp.ClientSession
    api_key: str
    base_url: str





async def init_rest_client(request_timeout_s: float = 30.0) -> RestClient:
    """
    Create the REST client once. Idempotent.

    Async because ``aiohttp.ClientSession`` prefers being instantiated
    inside a running event loop. A hard per-request timeout is critical
    -- without it a single slow/hung symbol can pin a worker slot
    forever (the historian's bounded semaphore then wedges the whole
    backfill).
    """
    global rest_client
    if rest_client is not None:
        logger.debug("init_rest_client called but client already exists -- reusing")
        return rest_client

    session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=request_timeout_s),
    )
    rest_client = RestClient(
        session=session,
        api_key=settings.POLYGON_API_KEY,
        base_url=settings.POLYGON_BASE_URL.rstrip("/"),
    )
    logger.info("REST client created")
    return rest_client


def get_rest_client() -> RestClient:
    if rest_client is None:
        raise RuntimeError("REST client not initialized -- call init_rest_client() first")
    return rest_client


async def close_rest_client() -> None:
    global rest_client
    if rest_client is not None:
        logger.debug("REST client closing")
        if not rest_client.session.closed:
            await rest_client.session.close()
        rest_client = None
        logger.debug("REST client closed")
