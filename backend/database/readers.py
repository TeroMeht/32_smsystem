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
  * rvol baseline lookup    -- per-bar avg_volume for (symbolid, bar_time).
"""

from __future__ import annotations

from datetime import date, datetime, time
from typing import Iterable, Optional

import asyncpg

from backend.datapipe.schemas import Bar1m, MonitoredSymbols
from backend.datapipe.time_utils import HELSINKI


async def load_active_symbol_map(pool: asyncpg.Pool) -> MonitoredSymbols:
    """Active monitored-symbol universe wrapped in a typed snapshot."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT symbol, symbolid FROM monitored_symbols WHERE active = true;"
        )
    return MonitoredSymbols(by_symbol={r["symbol"]: r["symbolid"] for r in rows})


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


async def load_livestream_bars_for_symbol(
    pool: asyncpg.Pool,
    symbol: str,
) -> list[dict]:
    """
    All rows currently in ``livestream`` for ``symbol``, sorted by ts.
    Livestream is truncated at each session boundary, so this is the
    current session in progress -- exactly what a trader wants to see
    when hovering a row: today's chart, not historical context.

    Returns dict-shaped rows (JSON-ready) including the enriched columns
    (vwap, ema9, rvol_cum, relatr) so the client can overlay them later
    if we want, though the initial candles-only view ignores them.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT l.ts,
                   l.open, l.high, l.low, l.close,
                   l.volume,
                   l.vwap, l.ema9, l.rvol_cum, l.relatr
              FROM livestream l
              JOIN monitored_symbols ms USING (symbolid)
             WHERE ms.symbol = $1
             ORDER BY l.ts ASC;
            """,
            symbol,
        )
    return [
        {
            "ts": r["ts"].isoformat(),
            "open": float(r["open"]),
            "high": float(r["high"]),
            "low": float(r["low"]),
            "close": float(r["close"]),
            "volume": int(r["volume"]),
            "vwap": float(r["vwap"]) if r["vwap"] is not None else None,
            "ema9": float(r["ema9"]) if r["ema9"] is not None else None,
            "rvol_cum": float(r["rvol_cum"]) if r["rvol_cum"] is not None else None,
            "relatr": float(r["relatr"]) if r["relatr"] is not None else None,
        }
        for r in rows
    ]


async def load_intraday_bars_for_day(
    pool: asyncpg.Pool,
    day: date,
    symbolids: Iterable[int],
) -> list[Bar1m]:
    """
    All ``intraday_bars`` rows for ``day`` (ET session date) whose
    ``symbolid`` is in ``symbolids``, joined to ``monitored_symbols`` for
    the ticker string. Ordered by ``ts`` ascending -- so a single
    ``async for`` gives a chronologically-merged, cross-symbol timeline
    ready for the replay driver.

    One round trip regardless of how many symbols are in the list.
    """
    sids = list(symbolids)
    if not sids:
        return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT ib.symbolid,
                   ib.ts,
                   ib.open,
                   ib.high,
                   ib.low,
                   ib.close,
                   ib.volume,
                   ms.symbol
              FROM intraday_bars ib
              JOIN monitored_symbols ms ON ms.symbolid = ib.symbolid
             WHERE ib.symbolid = ANY($1::int[])
               AND (ib.ts AT TIME ZONE 'America/New_York')::date = $2
             ORDER BY ib.ts ASC;
            """,
            sids, day,
        )
    return [
        Bar1m(
            symbol=r["symbol"],
            symbolid=r["symbolid"],
            ts=r["ts"],
            open=float(r["open"]),
            high=float(r["high"]),
            low=float(r["low"]),
            close=float(r["close"]),
            volume=int(r["volume"]),
        )
        for r in rows
    ]


async def load_rvol_baseline_for_symbol(
    pool: asyncpg.Pool,
    symbolid: int,
) -> dict[time, float]:
    """
    bar_time -> per-bar avg_volume for one symbol's full session grid.

    Loaded once per symbol at startup and cached in memory; a full session
    grid is <= ~960 rows (04:00-20:00 ET at 1-min) so it costs nothing.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT bar_time, avg_volume FROM rvol_baseline WHERE symbolid = $1;",
            symbolid,
        )
    return {r["bar_time"]: float(r["avg_volume"]) for r in rows}


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
# Presentation helpers -- feed the /relatr dashboard poll endpoint.
# ---------------------------------------------------------------------------


async def load_latest_livestream_per_symbol(
    pool: asyncpg.Pool,
) -> list[dict]:
    """
    Latest livestream row per symbol PLUS session-cumulative volume.

    Since ``livestream`` is truncated at each session boundary, SUM(volume)
    across the whole table per symbolid == today's cumulative volume from
    session open. One CTE does the latest-per-symbol pick, another sums
    per symbolid, LEFT JOIN combines them.

    Unfiltered, unsorted, unlimited -- the frontend owns all display
    filters (volume floor, RVOL floor, RelATR floor, cum-volume floor,
    sort, row cap).
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            WITH latest AS (
                SELECT DISTINCT ON (l.symbolid)
                       ms.symbol,
                       l.symbolid,
                       l.ts,
                       l.close,
                       l.vwap,
                       l.ema9,
                       l.rvol_cum,
                       l.relatr,
                       l.volume
                  FROM livestream l
                  JOIN monitored_symbols ms USING (symbolid)
                 ORDER BY l.symbolid, l.ts DESC
            ),
            cum_vol AS (
                SELECT symbolid, SUM(volume)::bigint AS cum_volume
                  FROM livestream
                 GROUP BY symbolid
            ),
            -- premarket_open = first bar of today's session per symbol.
            -- Livestream is truncated per session, so MIN(ts) row's open
            -- is the premarket open of the current session (typically the
            -- 04:00 ET bar when trading first occurred).
            pm_open AS (
                SELECT DISTINCT ON (symbolid) symbolid, open AS pm_open
                  FROM livestream
                 ORDER BY symbolid, ts ASC
            ),
            -- prev_close = most recent daily close on disk per symbol.
            -- In live mode the historian stops at today-1, so this is
            -- yesterday's close (or Friday's on a Monday).
            prev_close AS (
                SELECT DISTINCT ON (symbolid) symbolid, close AS prev_close
                  FROM daily
                 ORDER BY symbolid, date DESC
            )
            SELECT latest.*,
                   cum_vol.cum_volume,
                   pm_open.pm_open,
                   prev_close.prev_close
              FROM latest
              LEFT JOIN cum_vol    USING (symbolid)
              LEFT JOIN pm_open    USING (symbolid)
              LEFT JOIN prev_close USING (symbolid);
            """
        )
    # Reference for the "Chg%" column switches with Helsinki wall-clock:
    #   11:00 <= HKI <  16:30  -> premarket window: ref = prev_close
    #   HKI  >= 16:30          -> regular hours   : ref = pm_open
    #   HKI  <  11:00          -> off-session     : ref = prev_close
    # Backend computes the derived value so the frontend just renders it.
    now_hki = datetime.now(HELSINKI)
    hki_minutes = now_hki.hour * 60 + now_hki.minute
    in_premarket = 11 * 60 <= hki_minutes < 16 * 60 + 30

    def _ref(pm_open: Optional[float], prev_close: Optional[float]) -> Optional[float]:
        if in_premarket:
            return prev_close
        return pm_open if pm_open is not None else prev_close

    def _chg_pct(close: Optional[float], ref: Optional[float]) -> Optional[float]:
        if close is None or ref is None or ref == 0:
            return None
        return round((close - ref) / ref * 100.0, 2)

    out = []
    for r in rows:
        close = float(r["close"]) if r["close"] is not None else None
        pm_open = float(r["pm_open"]) if r["pm_open"] is not None else None
        prev_close = float(r["prev_close"]) if r["prev_close"] is not None else None
        ref = _ref(pm_open, prev_close)
        out.append({
            "symbol": r["symbol"],
            "ts": r["ts"].isoformat(),
            "close": close,
            "vwap": float(r["vwap"]) if r["vwap"] is not None else None,
            "ema9": float(r["ema9"]) if r["ema9"] is not None else None,
            "rvol_cum": float(r["rvol_cum"]) if r["rvol_cum"] is not None else None,
            # Round relatr at the API boundary so the frontend never sees
            # 4-decimal noise -- 2 decimals is the display precision.
            "relatr": round(float(r["relatr"]), 2) if r["relatr"] is not None else None,
            "volume": int(r["volume"]) if r["volume"] is not None else None,
            "cum_volume": int(r["cum_volume"]) if r["cum_volume"] is not None else None,
            "chg_pct": _chg_pct(close, ref),
        })
    return out


# ---------------------------------------------------------------------------
# Batch readiness -- one query per table instead of one-per-symbol.
# ---------------------------------------------------------------------------


async def load_livestream_freshness(pool: asyncpg.Pool) -> dict[int, datetime]:
    """
    symbolid -> MAX(ts) currently in ``livestream``. One aggregate query.

    Used by the livestream priming step to decide, per symbol, whether we
    can skip the REST fetch (we already have today's data) or need to
    fetch and enrich.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT symbolid, MAX(ts) AS ts FROM livestream GROUP BY symbolid;"
        )
    return {r["symbolid"]: r["ts"] for r in rows if r["ts"] is not None}


async def load_livestream_bars_for_symbol_today(
    pool: asyncpg.Pool,
    symbolid: int,
    today_et: date,
) -> list[Bar1m]:
    """
    All livestream rows for ``symbolid`` whose ET session date is today,
    ordered by ts. Used to rehydrate ``st.history`` from what livestream
    already has, so we don't need to REST-fetch today's bars on restart.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT l.symbolid, l.ts, l.open, l.high, l.low, l.close, l.volume,
                   l.vwap, l.ema9, l.rvol_cum, l.relatr,
                   ms.symbol
              FROM livestream l
              JOIN monitored_symbols ms USING (symbolid)
             WHERE l.symbolid = $1
               AND (l.ts AT TIME ZONE 'America/New_York')::date = $2
             ORDER BY l.ts ASC;
            """,
            symbolid, today_et,
        )
    return [
        Bar1m(
            symbol=r["symbol"],
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


async def get_last_backfill_run(
    pool: asyncpg.Pool,
) -> tuple[Optional[datetime], Optional[datetime]]:
    """
    Return (last_daily_run_utc, last_intraday_run_utc) from ``backfill_status``.

    Either component is ``None`` if that side has never successfully
    completed. Historian uses these to decide -- per side -- whether we
    already did the work today and can skip the REST calls entirely.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT MAX(daily_last_run)    AS daily,
                   MAX(intraday_last_run) AS intraday
              FROM backfill_status;
            """
        )
    return row["daily"], row["intraday"]

