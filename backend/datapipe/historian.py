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

from backend.core.config import settings
from backend.database import rvol_baseline as rvol_baseline_db
from backend.database.partitions import (
    DAILY_RETENTION_DAYS,
    INTRADAY_RETENTION_DAYS,
    ensure_partition_daily,
    ensure_partition_intraday,
    ensure_partitions_for_dates,
)
from backend.database.readers import load_readiness_snapshot
from backend.database.writers import (
    bulk_insert_daily_bars,
    bulk_insert_intraday_bars,
)
from backend.datapipe.calculations import compute_atr_series
from backend.datapipe.rest_client import RestClient
from backend.datapipe.schemas import Bar1m, DailyBar, MonitoredSymbols
from backend.datapipe.time_utils import session_date_et

logger = logging.getLogger(__name__)


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
    raw = await rest.fetch_daily_bars(symbol, end_day=up_to, lookback_days=settings.DAILY_BACKFILL_DAYS)
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
    symbol_map: MonitoredSymbols,
    concurrency: int = 10,
    replay_mode: bool = False,
) -> None:
    """
    End-to-end warmup for every active symbol.

    Readiness gate up front: one batched query per table gives us
    ``symbolid -> last date/ts`` for both daily and intraday. Per symbol we
    classify into two buckets (daily / intraday) and only symbols with an
    actual gap hit the network.

    Freshness semantics -- SAME rule for live and replay:
    intraday is fresh if ``session_date_et(last_ts) >= today`` -- i.e. we
    already have at least one bar from today's ET session. The livestream
    (in live mode) or the replay driver (in replay mode) fills the small
    gap forward from there. A wall-clock threshold makes no sense on a
    delayed feed and triggered full-day refetches on every restart.

    Partition retention (``drop_old_partitions``) and universe loading are
    both handled by ``pipeline.startup`` before this runs -- we just make
    sure the forward-looking partitions exist.
    """
    total = len(symbol_map)
    logger.info("Backfill data check for %d symbols (today= %s, replay_mode= %s)",
                total, today.isoformat(), replay_mode)

    # 1. Ensure partitions exist for the retention window forward.
    intraday_start = today - timedelta(days=settings.INTRADAY_BACKFILL_DAYS)
    intraday_days = [today - timedelta(days=i) for i in range(INTRADAY_RETENTION_DAYS)]
    daily_days = [today - timedelta(days=i) for i in range(DAILY_RETENTION_DAYS)]
    await ensure_partitions_for_dates(pool, intraday_days, daily_days)

    # 2. Batched readiness snapshot -- two aggregate queries, no per-symbol I/O
    daily_map, intraday_map = await load_readiness_snapshot(pool)

    daily_cutoff_date = today - timedelta(days=settings.DAILY_STALE_DAYS)

    daily_todo: list[tuple[str, int]] = []
    intraday_todo: list[tuple[str, int, date]] = []  # (symbol, symbolid, need_from)

    # Target last date for intraday_bars depends on mode:
    #   live   -> yesterday. livestream primes today via REST, historian
    #             stays out of today entirely (baseline material only).
    #   replay -> today (= REPLAY_DAY). Replay driver reads that day out
    #             of intraday_bars, so it must be there.
    fetch_end = today if replay_mode else today - timedelta(days=1)

    # Intraday freshness cutoff: accept any bar from within the last
    # INTRADAY_STALE_DAYS calendar days. This tolerates weekends/holidays
    # where "yesterday" was not a trading day and we correctly have data
    # from the last trading session (e.g. Friday on a Monday restart).
    intraday_stale_cutoff = today - timedelta(days=settings.INTRADAY_STALE_DAYS)

    for symbol, symbolid in symbol_map.items():
        last_d = daily_map.get(symbolid)
        if last_d is None or last_d < daily_cutoff_date:
            daily_todo.append((symbol, symbolid))

        last_ts = intraday_map.get(symbolid)
        if last_ts is None:
            intraday_todo.append((symbol, symbolid, intraday_start))
            continue

        have_date = session_date_et(last_ts)
        # Ready iff have_date is within the staleness window.
        if have_date < intraday_stale_cutoff:
            need_from = have_date if have_date >= intraday_start else intraday_start
            intraday_todo.append((symbol, symbolid, need_from))

    logger.info(
        "Backfill readiness: %d symbols ready, %d need daily, %d need intraday",
        total - max(len(daily_todo), len(intraday_todo)),
        len(daily_todo), len(intraday_todo),
    )

    if not daily_todo and not intraday_todo:
        logger.info("Nothing to backfill -- all symbols has current data")
    else:
        await _run_backfill_workers(
            pool, rest, today, fetch_end, daily_todo, intraday_todo, concurrency,
        )

    # 3. RVOL baseline rebuild -- cheap even when nothing new was fetched,
    #    and guarantees the table reflects whatever is currently in intraday_bars.
    logger.info(
        "[historian] rebuilding rvol_baseline (lookback=%dd, sample_sessions=%d)",
        settings.INTRADAY_BACKFILL_DAYS, settings.RVOL_SAMPLE_SESSIONS,
    )
    await rvol_baseline_db.rebuild(
        pool, end_day=today,
        lookback_days=settings.INTRADAY_BACKFILL_DAYS,
        sample_sessions=settings.RVOL_SAMPLE_SESSIONS,
    )

    logger.info("[historian] backfill complete")


async def _run_backfill_workers(
    pool: asyncpg.Pool,
    rest: RestClient,
    today: date,
    fetch_end: date,
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
                "Backfill progress: %d/%d units (daily_rows= %d intraday_rows= %d errors= %d)",
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
                    rest, symbol, symbolid, need_from, fetch_end,
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
        "Backfilling complete: %d daily rows, %d intraday rows, %d errors",
        counters["daily_rows"], counters["intraday_rows"], counters["errors"],
    )
