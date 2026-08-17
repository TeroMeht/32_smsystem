"""
Startup priming helpers shared by live + replay.

Three orthogonal steps live here so both runtime paths can compose them:

  * ``seed_session_state``  -- pull ATR (daily_indicators) and rvol
                               baselines (rvol_baseline) from the DB and
                               hydrate the in-memory SessionStore.
  * ``enrich_prime_bars``   -- walk a list of raw bars through
                               ``st.apply_bar`` in ts order; pure state
                               work, no DB.
  * ``bulk_persist_bars``   -- bulk-insert an enriched batch into the
                               ``livestream`` table.

Neither enrich nor persist knows where its inputs came from -- REST
prime (live) or intraday_bars (replay) is the caller's concern.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Iterable

import asyncpg

from backend.database.readers import (
    load_latest_atr_map,
    load_rvol_baseline_for_symbol,
)
from backend.database.writers import bulk_insert_livestream_bars
from backend.datapipe.runtime.session_state import SessionStore
from backend.datapipe.schemas import Bar, MonitoredSymbols

logger = logging.getLogger(__name__)


async def seed_session_state(
    pool: asyncpg.Pool,
    store: SessionStore,
    symbol_map: MonitoredSymbols,
    session_date: date,
) -> None:
    """
    Load daily ATR + rvol_baseline for every active symbol and stash
    them onto per-symbol ``SymbolSessionState``. Runs once at startup;
    values are read-only reference data after this point.

    Logs the seeded ATR + rvol_baseline shape per symbol so operators
    can eyeball what was hydrated. Availability is assumed at this point
    -- upstream priming guarantees both are populated.
    """
    atr_map = await load_latest_atr_map(pool)
    logger.info(
        "Seeding session state (session=%s, symbols=%d) -- showing first 10",
        session_date.isoformat(), len(symbol_map),
    )
    for i, (sym, sid) in enumerate(symbol_map.items()):
        baseline = await load_rvol_baseline_for_symbol(pool, sid)
        st = store.init(sym, sid, session_date)
        st.atr = atr_map.get(sid)
        st.rvol_baseline = baseline
        if i < 10:
            logger.info(
                "  %-8s sid=%-6d ATR=%s  rvol_baseline slots=%-3d sum=%.2f",
                sym, sid,
                f"{st.atr:.4f}" if st.atr is not None else "None",
                len(baseline), sum(baseline.values()),
            )


def enrich_prime_bars(
    store: SessionStore,
    bars_by_symbol: Iterable[tuple[str, int, list[Bar]]],
) -> list[Bar]:
    """
    Walk each symbol's bars through ``st.apply_bar`` in ts order and
    return the enriched batch. Pure in-memory state work -- no DB, no
    awaits. Same code path fed by live's REST prime and replay's
    intraday_bars prefix; the caller shapes the input into
    ``(symbol, symbolid, [bars])`` tuples.
    """
    all_enriched: list[Bar] = []
    for sym, _sid, bars in bars_by_symbol:
        if not bars:
            continue
        st = store.get(sym)
        for bar in bars:
            all_enriched.append(st.apply_bar(bar))
    return all_enriched


async def bulk_persist_bars(pool: asyncpg.Pool, bars: list[Bar]) -> int:
    """
    Bulk-insert an enriched batch into ``livestream`` and return the row
    count. No-op on empty input.
    """
    if bars:
        await bulk_insert_livestream_bars(pool, bars)
    return len(bars)
