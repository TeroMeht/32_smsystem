"""
Sync DB operations for the monitored-symbols universe refresh.

The universe pipeline (``backend/stock_universe/``) is a batch script that
runs periodically to rebuild the tradable universe. It uses psycopg2
(sync) rather than asyncpg -- matches the script's overall style and
avoids an event-loop dependency for a one-shot job.

All SQL for that pipeline lives here; the callers in stock_universe/
should only orchestrate.
"""

from __future__ import annotations

from datetime import datetime
from typing import Iterable

import psycopg2.extras


# All functions take a psycopg2 connection and manage their own cursor
# internally, so callers never touch psycopg2 primitives -- keeping the
# "no SQL / no DB primitives outside database/" boundary clean. All
# cursors share the connection's transaction, so a caller wrapping
# fetch/upsert/deactivate/count in one ``with connect() as conn`` block
# still gets atomic semantics (nothing commits until conn.commit()).


# --- READS ------------------------------------------------------------------


def fetch_symbol_active_map(conn) -> dict[str, bool]:
    """Return {symbol: active} for every row currently in monitored_symbols."""
    with conn.cursor() as cur:
        cur.execute("SELECT symbol, active FROM monitored_symbols;")
        return {sym: active for sym, active in cur.fetchall()}


def count_symbols(conn) -> tuple[int, int]:
    """Return (active_count, total_count) from monitored_symbols."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FILTER (WHERE active), COUNT(*) FROM monitored_symbols;"
        )
        active, total = cur.fetchone()
    return int(active), int(total)


# --- WRITES -----------------------------------------------------------------


def upsert_symbols(
    conn,
    rows: Iterable[tuple[str, str, int, int, datetime, bool]],
    page_size: int = 500,
) -> None:
    """
    Bulk upsert (symbol, exchange, market_cap, adv_dollar, last_refresh,
    active). ON CONFLICT updates every field except symbol -- refreshed
    universe wins for existing rows, and active is forced true so any
    previously-deactivated symbol comes back in when it re-enters the
    tradable list.
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO monitored_symbols
                (symbol, exchange, market_cap, adv_dollar, last_refresh, active)
            VALUES %s
            ON CONFLICT (symbol) DO UPDATE SET
                exchange     = EXCLUDED.exchange,
                market_cap   = EXCLUDED.market_cap,
                adv_dollar   = EXCLUDED.adv_dollar,
                last_refresh = EXCLUDED.last_refresh,
                active       = true
            """,
            list(rows),
            page_size=page_size,
        )


def deactivate_symbols(
    conn,
    symbols: list[str],
    when: datetime,
) -> int:
    """
    Mark the given symbols inactive (``active=false``). Never DELETE --
    intraday_bars / daily / etc. still FK to symbolid, so history stays
    intact when the universe shrinks.

    Returns the number of rows actually flipped.
    """
    if not symbols:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE monitored_symbols
               SET active = false, last_refresh = %s
             WHERE symbol = ANY(%s) AND active = true;
            """,
            (when, symbols),
        )
        return cur.rowcount
