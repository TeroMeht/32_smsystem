"""
Async DB reads used by the live enrichment path.

Kept separate from writers.py so the read/write contracts are easy to
audit. Reads here are:

  * monitored symbol map    -- symbol -> symbolid, used to translate WS
                               messages into Bar1m without a per-message
                               DB round trip.
  * latest ATR per symbol   -- from the ``daily`` table; feeds RelATR.
  * session bars so far     -- read from ``livestream`` to warm up the
                               per-symbol in-memory history when a live
                               loop restarts mid-session.
  * rvol baseline lookup    -- avg_cum_volume for (symbolid, bar_time).
"""

from __future__ import annotations

from datetime import date, datetime, time
from typing import Optional

import asyncpg

from backend.datapipe.schemas import Bar1m


async def load_active_symbol_map(pool: asyncpg.Pool) -> dict[str, int]:
    """symbol -> symbolid for all active monitored symbols."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT symbol, symbolid FROM monitored_symbols WHERE active = true;"
        )
    return {r["symbol"]: r["symbolid"] for r in rows}


async def load_latest_atr_map(pool: asyncpg.Pool) -> dict[int, float]:
    """
    symbolid -> latest ATR from ``daily`` (DISTINCT ON per symbol).

    Uses idx_daily_symbolid_date_desc for the ORDER BY. If a symbol has
    no daily rows yet, it simply won't appear in the returned dict --
    RelATR for that symbol will be None until the historian backfills.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT ON (symbolid) symbolid, atr
              FROM daily
             WHERE atr IS NOT NULL
             ORDER BY symbolid, date DESC;
            """
        )
    return {r["symbolid"]: float(r["atr"]) for r in rows}


async def load_session_bars(
    pool: asyncpg.Pool,
    symbolid: int,
    session_start_utc: datetime,
) -> list[Bar1m]:
    """
    All livestream bars for ``symbolid`` since ``session_start_utc``,
    chronological. Used to rehydrate the in-memory per-symbol history if
    the live loop restarts mid-session.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT symbolid, ts, open, high, low, close, volume,
                   vwap, ema9, rvol_cum, relatr
              FROM livestream
             WHERE symbolid = $1 AND ts >= $2
             ORDER BY ts ASC;
            """,
            symbolid, session_start_utc,
        )
    sym_row = await _fetch_symbol_for_id(pool, symbolid)
    symbol = sym_row or ""
    return [
        Bar1m(
            symbol=symbol,
            symbolid=r["symbolid"],
            ts=r["ts"],
            open=float(r["open"]),
            high=float(r["high"]),
            low=float(r["low"]),
            close=float(r["close"]),
            volume=int(r["volume"]),
            vwap=float(r["vwap"]) if r["vwap"] is not None else None,
            ema9=float(r["ema9"]) if r["ema9"] is not None else None,
            rvol_cum=float(r["rvol_cum"]) if r["rvol_cum"] is not None else None,
            relatr=float(r["relatr"]) if r["relatr"] is not None else None,
        )
        for r in rows
    ]


async def _fetch_symbol_for_id(pool: asyncpg.Pool, symbolid: int) -> Optional[str]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT symbol FROM monitored_symbols WHERE symbolid = $1;",
            symbolid,
        )
    return row["symbol"] if row else None


async def load_rvol_baseline_for_symbol(
    pool: asyncpg.Pool,
    symbolid: int,
) -> dict[time, float]:
    """
    bar_time -> avg_cum_volume for one symbol's full session grid.

    Loaded once per symbol at startup and cached in memory; a full session
    grid is <= ~500 rows so it costs nothing.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT bar_time, avg_cum_volume FROM rvol_baseline WHERE symbolid = $1;",
            symbolid,
        )
    return {r["bar_time"]: float(r["avg_cum_volume"]) for r in rows}


async def last_intraday_ts(
    pool: asyncpg.Pool, symbolid: int
) -> Optional[datetime]:
    """
    Most recent ts we have in intraday_bars for ``symbolid``. Historian
    uses this to know how far back to backfill (i.e. skip re-fetching data
    we already have from an earlier session-start).
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT MAX(ts) AS ts FROM intraday_bars WHERE symbolid = $1;",
            symbolid,
        )
    return row["ts"] if row and row["ts"] else None


async def last_daily_date(
    pool: asyncpg.Pool, symbolid: int
) -> Optional[date]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT MAX(date) AS d FROM daily WHERE symbolid = $1;",
            symbolid,
        )
    return row["d"] if row and row["d"] else None


# ---------------------------------------------------------------------------
# Batch readiness -- one query per table instead of one-per-symbol.
# ---------------------------------------------------------------------------


async def load_readiness_snapshot(
    pool: asyncpg.Pool,
) -> tuple[dict[int, date], dict[int, datetime]]:
    """
    Single-shot snapshot of what we already have per symbolid:

      * ``daily_map``    -- symbolid -> most recent date in ``daily``
      * ``intraday_map`` -- symbolid -> most recent ts in ``intraday_bars``

    The historian uses this at startup to decide, per symbol, whether we
    can skip fetching entirely. Two aggregate queries beat 1712 per-symbol
    round trips by orders of magnitude.
    """
    async with pool.acquire() as conn:
        daily_rows = await conn.fetch(
            "SELECT symbolid, MAX(date) AS d FROM daily GROUP BY symbolid;"
        )
        intraday_rows = await conn.fetch(
            "SELECT symbolid, MAX(ts) AS ts FROM intraday_bars GROUP BY symbolid;"
        )
    daily_map = {r["symbolid"]: r["d"] for r in daily_rows if r["d"] is not None}
    intraday_map = {r["symbolid"]: r["ts"] for r in intraday_rows if r["ts"] is not None}
    return daily_map, intraday_map
