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
from datetime import date, datetime, timedelta, timezone

import asyncpg
import pandas as pd

from backend.database import rvol_baseline as rvol_baseline_db
from backend.database.partitions import (
    DAILY_RETENTION_DAYS,
    INTRADAY_RETENTION_DAYS,
    drop_old_partitions,
    ensure_partition_daily,
    ensure_partition_intraday,
    ensure_partitions_for_dates,
)
from backend.database.readers import (
    load_active_symbol_map,
    load_readiness_snapshot,
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


INTRADAY_BACKFILL_DAYS = 8    # 8 calendar days guarantees >=5 trading sessions
DAILY_BACKFILL_DAYS = 20      # 20 calendar days guarantees >=14 trading days
RVOL_SAMPLE_SESSIONS = 5      # RVOL baseline averages this many trading sessions

# Freshness thresholds used to decide whether a symbol needs any backfill.
DAILY_STALE_DAYS = 3            # covers weekend restarts (Fri -> Mon)
INTRADAY_STALE_SECONDS = 5 * 60  # if last bar arrived <5min ago, we're current


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
    End-to-end warmup for every active symbol.

    Readiness gate up front: one batched query per table gives us
    ``symbolid -> last date/ts`` for both daily and intraday. Per symbol we
    then classify into four buckets:

      * ``daily_ok``     -- last_d >= today - DAILY_STALE_DAYS
      * ``daily_missing`` (fetch)
      * ``intraday_ok``  -- last_ts is within INTRADAY_STALE_SECONDS of now
      * ``intraday_missing`` (fetch)

    Symbols in both "_ok" buckets are skipped entirely -- no REST calls,
    no DB writes. Only symbols with actual gaps hit the network. This
    means a restart 30 seconds after a clean shutdown finishes in seconds
    instead of minutes.
    """
    symbol_map = await load_active_symbol_map(pool)
    if not symbol_map:
        logger.warning("[historian] no active monitored symbols -- nothing to backfill")
        return

    total = len(symbol_map)
    logger.info("[historian] readiness check for %d symbols (today=%s)",
                total, today.isoformat())

    # 1. Retention prune + partition creation
    await drop_old_partitions(pool, today)
    intraday_start = today - timedelta(days=INTRADAY_BACKFILL_DAYS)
    intraday_days = [today - timedelta(days=i) for i in range(INTRADAY_BACKFILL_DAYS + 1)]
    daily_days = [today - timedelta(days=i) for i in range(DAILY_BACKFILL_DAYS + 1)]
    await ensure_partitions_for_dates(pool, intraday_days, daily_days)

    # 2. Batched readiness snapshot -- two aggregate queries, no per-symbol I/O
    daily_map, intraday_map = await load_readiness_snapshot(pool)

    now_utc = datetime.now(timezone.utc)
    daily_cutoff_date = today - timedelta(days=DAILY_STALE_DAYS)

    daily_todo: list[tuple[str, int]] = []
    intraday_todo: list[tuple[str, int, date]] = []  # (symbol, symbolid, need_from)

    for symbol, symbolid in symbol_map.items():
        last_d = daily_map.get(symbolid)
        if last_d is None or last_d < daily_cutoff_date:
            daily_todo.append((symbol, symbolid))

        last_ts = intraday_map.get(symbolid)
        if last_ts is None:
            intraday_todo.append((symbol, symbolid, intraday_start))
        elif (now_utc - last_ts).total_seconds() >= INTRADAY_STALE_SECONDS:
            # Refetch just the window from last-known onwards (already inside
            # the retention cutoff, so the row-filter below keeps it clean).
            have_date = session_date_et(last_ts)
            need_from = have_date if have_date >= intraday_start else intraday_start
            intraday_todo.append((symbol, symbolid, need_from))
        # else: intraday_ok -- skipped entirely

    logger.info(
        "[historian] readiness: %d symbols ready, %d need daily, %d need intraday",
        total - max(len(daily_todo), len(intraday_todo)),
        len(daily_todo), len(intraday_todo),
    )

    if not daily_todo and not intraday_todo:
        logger.info("[historian] nothing to backfill -- all symbols current")
    else:
        await _run_backfill_workers(
            pool, rest, today, daily_todo, intraday_todo, concurrency,
        )

    # 3. RVOL baseline rebuild -- cheap even when nothing new was fetched,
    #    and guarantees the table reflects whatever is currently in intraday_bars.
    logger.info(
        "[historian] rebuilding rvol_baseline (lookback=%dd, sample_sessions=%d)",
        INTRADAY_BACKFILL_DAYS, RVOL_SAMPLE_SESSIONS,
    )
    await rvol_baseline_db.rebuild(
        pool, end_day=today,
        lookback_days=INTRADAY_BACKFILL_DAYS,
        sample_sessions=RVOL_SAMPLE_SESSIONS,
    )

    logger.info("[historian] backfill complete")


async def _run_backfill_workers(
    pool: asyncpg.Pool,
    rest: RestClient,
    today: date,
    daily_todo: list[tuple[str, int]],
    intraday_todo: list[tuple[str, int, date]],
    concurrency: int,
) -> None:
    """
    Run the two work lists through a bounded semaphore. Each list is
    independent; we schedule both as one flat coroutine set so the
    concurrency limit applies to the total in-flight REST calls.
    """
    daily_cutoff = today - timedelta(days=DAILY_RETENTION_DAYS - 1)
    intraday_cutoff = today - timedelta(days=INTRADAY_RETENTION_DAYS - 1)

    sem = asyncio.Semaphore(concurrency)
    counters = {"daily_rows": 0, "intraday_rows": 0, "errors": 0}
    total_units = len(daily_todo) + len(intraday_todo)
    done = 0
    progress_step = max(1, total_units // 10)

    def _tick(unit_done: int) -> int:
        nonlocal done
        done += unit_done
        if done % progress_step < unit_done or done == total_units:
            logger.info(
                "[historian] progress: %d/%d units (daily_rows=%d intraday_rows=%d errors=%d)",
                done, total_units,
                counters["daily_rows"], counters["intraday_rows"], counters["errors"],
            )
        return done

    async def _do_daily(symbol: str, symbolid: int) -> None:
        async with sem:
            try:
                daily_all = await _backfill_daily_for_symbol(rest, symbol, symbolid, today)
                daily = [b for b in daily_all if b.d >= daily_cutoff]
                if daily:
                    for d in {b.d for b in daily}:
                        await ensure_partition_daily(pool, d)
                    await bulk_insert_daily_bars(pool, daily)
                    counters["daily_rows"] += len(daily)
            except Exception:
                counters["errors"] += 1
                logger.exception("[historian] %s: daily backfill failed", symbol)
            finally:
                _tick(1)

    async def _do_intraday(symbol: str, symbolid: int, need_from: date) -> None:
        async with sem:
            try:
                intraday_all = await _backfill_intraday_for_symbol(
                    rest, symbol, symbolid, need_from, today,
                )
                intraday = [
                    b for b in intraday_all
                    if session_date_et(b.ts) >= intraday_cutoff
                ]
                if intraday:
                    for d in {session_date_et(b.ts) for b in intraday}:
                        await ensure_partition_intraday(pool, d)
                    await bulk_insert_intraday_bars(pool, intraday)
                    counters["intraday_rows"] += len(intraday)
            except Exception:
                counters["errors"] += 1
                logger.exception("[historian] %s: intraday backfill failed", symbol)
            finally:
                _tick(1)

    tasks = (
        [_do_daily(s, sid) for s, sid in daily_todo]
        + [_do_intraday(s, sid, nf) for s, sid, nf in intraday_todo]
    )
    await asyncio.gather(*tasks)

    logger.info(
        "[historian] fetch complete: %d daily rows, %d intraday rows, %d errors",
        counters["daily_rows"], counters["intraday_rows"], counters["errors"],
    )
