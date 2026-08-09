"""
Historian -- REST backfill.

Responsibilities on startup (or whenever the live loop isn't running):

  1. Make sure the ``daily`` table has the last ~14 sessions for every
     active monitored symbol (feeds ATR14).
  2. Make sure ``intraday_bars`` has the last 5 sessions of 1-min bars for
     every active monitored symbol (feeds RVOL baseline + gives strategies
     recent 1-min context).
  3. Rebuild ``rvol_baseline`` from the 5-day intraday history so RVOL
     comparisons have a denominator.
  4. Ensure partitions exist for the dates we're about to write and prune
     partitions past the retention window.

Explicitly does NOT run while the live stream is running (as per
instructions.md). Startup calls this before opening the WS.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta

import asyncpg
import pandas as pd

from backend.database import rvol_baseline as rvol_baseline_db
from backend.database.partitions import (
    drop_old_partitions,
    ensure_partition_daily,
    ensure_partition_intraday,
    ensure_partitions_for_dates,
)
from backend.database.readers import (
    load_active_symbol_map,
    last_daily_date,
    last_intraday_ts,
)
from backend.database.writers import (
    bulk_insert_daily_bars,
    bulk_insert_intraday_bars,
)
from backend.datapipe.calculations import compute_atr_series
from backend.datapipe.rest_client import RestClient
from backend.datapipe.schemas import Bar1m, DailyBar
from backend.datapipe.time_utils import session_date_et

logger = logging.getLogger(__name__)


INTRADAY_BACKFILL_DAYS = 5
DAILY_BACKFILL_DAYS = 20  # request 20 to guarantee 14 trading days after weekends


# ---------------------------------------------------------------------------
# per-symbol backfill primitives
# ---------------------------------------------------------------------------


async def _backfill_daily_for_symbol(
    rest: RestClient,
    symbol: str,
    symbolid: int,
    up_to: date,
) -> list[DailyBar]:
    """Fetch daily bars + compute ATR14 + return DailyBar list ready to insert."""
    raw = await rest.fetch_daily_bars(symbol, end_day=up_to, lookback_days=DAILY_BACKFILL_DAYS)
    if not raw:
        return []
    df = pd.DataFrame([
        {"Open": b.o, "High": b.h, "Low": b.l, "Close": b.c, "Volume": int(b.v), "t": b.t}
        for b in raw
    ])
    df["date"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.date
    df["atr"] = compute_atr_series(df)
    return [
        DailyBar(
            symbol=symbol,
            symbolid=symbolid,
            d=row["date"],
            open=float(row["Open"]),
            high=float(row["High"]),
            low=float(row["Low"]),
            close=float(row["Close"]),
            volume=int(row["Volume"]),
            atr=float(row["atr"]),
        )
        for _, row in df.iterrows()
    ]


async def _backfill_intraday_for_symbol(
    rest: RestClient,
    symbol: str,
    symbolid: int,
    start_day: date,
    end_day: date,
) -> list[Bar1m]:
    raw = await rest.fetch_intraday_bars_range(symbol, start_day, end_day)
    return [b.to_bar1m(symbol=symbol, symbolid=symbolid) for b in raw]


# ---------------------------------------------------------------------------
# top-level orchestrator
# ---------------------------------------------------------------------------


async def backfill_all_symbols(
    pool: asyncpg.Pool,
    rest: RestClient,
    today: date,
    concurrency: int = 10,
) -> None:
    """
    End-to-end warmup for every active symbol. Idempotent: safe to call on
    every startup -- ``last_daily_date`` / ``last_intraday_ts`` short-circuit
    symbols whose local history is already current, so subsequent runs on
    the same day only touch the DB for symbols missing today's data.

    Runs symbol-level fetches through a bounded semaphore so we don't
    hammer Massive with hundreds of concurrent requests.
    """
    symbol_map = await load_active_symbol_map(pool)
    if not symbol_map:
        logger.warning("historian: no active monitored symbols -- nothing to backfill")
        return

    logger.info("historian: starting backfill for %d symbols (today=%s)",
                len(symbol_map), today.isoformat())

    # 1. Ensure retention window is respected before we insert into fresh partitions
    await drop_old_partitions(pool, today)

    # 2. Pre-create partitions for the whole intraday window + daily window
    intraday_start = today - timedelta(days=INTRADAY_BACKFILL_DAYS)
    intraday_days = [today - timedelta(days=i) for i in range(INTRADAY_BACKFILL_DAYS + 1)]
    daily_days = [today - timedelta(days=i) for i in range(DAILY_BACKFILL_DAYS + 1)]
    await ensure_partitions_for_dates(pool, intraday_days, daily_days)

    sem = asyncio.Semaphore(concurrency)

    async def _work(symbol: str, symbolid: int) -> None:
        async with sem:
            # --- daily ---
            last_d = await last_daily_date(pool, symbolid)
            if last_d is None or last_d < today:
                daily = await _backfill_daily_for_symbol(rest, symbol, symbolid, today)
                if daily:
                    await bulk_insert_daily_bars(pool, daily)
                    logger.debug("historian: %s daily rows=%d", symbol, len(daily))

            # --- intraday ---
            last_ts = await last_intraday_ts(pool, symbolid)
            need_from = intraday_start
            if last_ts is not None:
                # Skip full re-download if we already have data past intraday_start.
                have_date = session_date_et(last_ts)
                if have_date >= intraday_start:
                    need_from = have_date  # re-fetch that day to fill any gap
            intraday = await _backfill_intraday_for_symbol(
                rest, symbol, symbolid, need_from, today,
            )
            if intraday:
                await bulk_insert_intraday_bars(pool, intraday)
                logger.debug("historian: %s intraday rows=%d (from %s)", symbol, len(intraday), need_from)

    await asyncio.gather(*[
        _work(sym, sid) for sym, sid in symbol_map.items()
    ])

    # 3. Rebuild RVOL baseline from what we now have in intraday_bars
    #    (delegated to backend.database.rvol_baseline -- all DB code lives there)
    await rvol_baseline_db.rebuild(pool, end_day=today, sample_days=INTRADAY_BACKFILL_DAYS)

    logger.info("historian: backfill complete")
