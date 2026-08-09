"""
Small sync connection helper for one-shot scripts (universe refresh, etc.).

The live monitoring loop should use an async pool (asyncpg) similar to
22_WatchlistStreamer; this module is intentionally kept simple for
weekly batch jobs.
"""

from contextlib import contextmanager

import psycopg2

from backend.core.config import settings


@contextmanager
def connect():
    """
    Yield a psycopg2 connection with autocommit off. Caller is responsible
    for committing or rolling back inside the `with` block.
    """
    conn = psycopg2.connect(settings.DATABASE_URL)
    conn.autocommit = False
    try:
        yield conn
    finally:
        conn.close()
