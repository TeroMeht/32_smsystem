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

from backend.core.config import settings
from backend.database import rvol_baseline as rvol_baseline_db
from backend.database.partitions import (
    ensure_partition_daily,
    ensure_partition_daily_indicators,
    ensure_partition_intraday,
    ensure_partitions_for_dates,
)
from backend.database.readers import load_daily_for_atr_compute
from backend.database.writers import (
    bulk_insert_daily_bars,
    bulk_insert_daily_indicators,
    bulk_insert_intraday_bars,
    record_backfill_run,
)
from backend.datapipe.calculations import calculate_atr_series

from backend.dependencies import RestClient
from backend.datapipe.sources.datasource import (
    fetch_daily_bars_range,
    fetch_intraday_bars_range,
)
from backend.datapipe.schemas import Bar1m, DailyBar, MonitoredSymbols
from backend.datapipe.time_utils import previous_trading_day, session_date_et

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
    """
    Fetch raw daily OHLCV bars from Polygon.

    Always fetches the full DAILY_BACKFILL_DAYS window (with a 2x calendar
    buffer for weekends/holidays) because the later ATR14 compute pass
    needs 14 trading days of context per symbol.

    Returned DailyBars are RAW ONLY -- no ATR / derived fields. Those
    live in ``daily_indicators`` and are populated by ``_compute_daily_indicators``
    after the raw daily inserts finish.
    """
    start = up_to - timedelta(days=settings.DAILY_BACKFILL_DAYS * 2)
    raw = await fetch_daily_bars_range(rest, symbol, start_day=start, end_day=up_to)
    if not raw:
        return []
    return [
        DailyBar(
            symbol   = symbol,
            symbolid = symbolid,
            d        = datetime.fromtimestamp(b.t / 1000, tz=timezone.utc).date(),
            open     = float(b.o),
            high     = float(b.h),
            low      = float(b.l),
            close    = float(b.c),
            volume   = int(b.v),
        )
        for b in raw
    ]


async def _backfill_intraday_for_symbol(
    rest: RestClient,
    symbol: str,
    symbolid: int,
    start_day: date,
    end_day: date,
) -> list[Bar1m]:
    """
    Fetch aggregation-cadence bars (BAR_MINUTES/minute) directly from
    Polygon. Polygon does the aggregation server-side, so no client
    batching is needed here.
    """
    raw = await fetch_intraday_bars_range(rest, symbol, start_day, end_day)
    return [b.to_bar1m(symbol=symbol, symbolid=symbolid) for b in raw]


# ---------------------------------------------------------------------------
# top-level orchestrator
# ---------------------------------------------------------------------------


async def backfill_all_symbols(
    pool: asyncpg.Pool,
    rest: RestClient,
    today: date,
    symbol_map: MonitoredSymbols,
    *,
    need_daily: bool,
    need_intraday: bool,
    concurrency: int = 10,
    replay_mode: bool = False,
) -> None:
    """
    End-to-end warmup for every active symbol.

    The freshness decision lives in the caller (pipeline.startup): the
    ``need_daily`` / ``need_intraday`` flags tell historian exactly which
    sides to fetch. Passing both False is a no-op (caller should just
    skip the call).

    After the workers finish, this function:
      * rebuilds ``rvol_baseline`` iff new intraday rows landed on disk;
      * inserts one row into ``backfill_status`` recording what ran and
        how many rows each side added, so the next startup can gate
        itself.

    Retention cleanup (``data_cleanup``) and universe loading are
    both handled by ``pipeline.startup`` before this runs -- we just make
    sure the forward-looking partitions exist.
    """
    total = len(symbol_map)
    logger.info(
        "Backfill starting for %d symbols "
        "(today= %s, replay_mode= %s, need_daily= %s, need_intraday= %s)",
        total, today.isoformat(), replay_mode, need_daily, need_intraday,
    )

    # 1. Ensure partitions exist for the retention window forward.
    intraday_start = today - timedelta(days=settings.INTRADAY_BACKFILL_DAYS)
    intraday_days = [today - timedelta(days=i) for i in range(settings.INTRADAY_BACKFILL_DAYS)]
    daily_days    = [today - timedelta(days=i) for i in range(settings.DAILY_BACKFILL_DAYS)]
    await ensure_partitions_for_dates(pool, intraday_days, daily_days)


    intraday_end = today if replay_mode else today - timedelta(days=1)
    daily_end    = previous_trading_day(today)

    daily_todo:    list[tuple[str, int]]       = []
    intraday_todo: list[tuple[str, int, date]] = []
    if need_daily:
        for symbol, symbolid in symbol_map.items():
            daily_todo.append((symbol, symbolid))
    if need_intraday:
        for symbol, symbolid in symbol_map.items():
            intraday_todo.append((symbol, symbolid, intraday_start))

    daily_rows, intraday_rows = await _run_backfill_workers(
        pool, rest, today, daily_end, intraday_end,
        daily_todo, intraday_todo, concurrency,
    )

    # 3a. Daily indicators pass -- read raw daily back, compute ATR14 per
    #     symbol in pandas, upsert into daily_indicators. Only runs when
    #     the daily side actually landed new rows this run.
    if need_daily and daily_rows > 0:
        logger.info("New daily data (%d rows) -- computing daily_indicators", daily_rows)
        await _compute_daily_indicators(pool, today)
    else:
        logger.info(
            "Skipping daily_indicators compute (need_daily=%s daily_rows=%d)",
            need_daily, daily_rows,
        )

    # 3b. RVOL baseline rebuild -- only if intraday brought new data in.
    if need_intraday and intraday_rows > 0:
        logger.info("New intraday data (%d rows) -- rebuilding rvol_baseline", intraday_rows)
        await rvol_baseline_db.rebuild(
            pool, end_day=today,
            lookback_days=settings.INTRADAY_BACKFILL_DAYS,
            sample_sessions=settings.RVOL_SAMPLE_SESSIONS,
        )
    else:
        logger.info(
            "Skipping rvol_baseline rebuild (need_intraday=%s intraday_rows=%d)",
            need_intraday, intraday_rows,
        )

    # 4. Ledger the successful run so the next startup can gate itself.
    now_utc = datetime.now(timezone.utc)
    await record_backfill_run(
        pool,
        daily_last_run    = now_utc if need_daily    else None,
        intraday_last_run = now_utc if need_intraday else None
    )




async def _run_backfill_workers(
    pool: asyncpg.Pool,
    rest: RestClient,
    today: date,
    daily_end: date,
    intraday_end: date,
    daily_todo: list[tuple[str, int]],
    intraday_todo: list[tuple[str, int, date]],
    concurrency: int,
) -> tuple[int, int]:
    """
    Run the two work lists through a bounded semaphore. Each list is
    independent; we schedule both as one flat coroutine set so the
    concurrency limit applies to the total in-flight REST calls.

    ``daily_end`` and ``intraday_end`` are the inclusive upper bounds of
    the REST fetches for each side (see caller for the ``today - 1`` /
    ``previous_trading_day(today)`` rationale).

    Returns ``(daily_rows_added, intraday_rows_added)`` so the caller can
    ledger the run and decide whether an rvol_baseline rebuild is needed.
    """
    if not daily_todo and not intraday_todo:
        return 0, 0

    daily_cutoff    = today - timedelta(days=settings.DAILY_BACKFILL_DAYS    - 1)
    intraday_cutoff = today - timedelta(days=settings.INTRADAY_BACKFILL_DAYS - 1)

    sem = asyncio.Semaphore(concurrency)
    counters = {"daily_rows": 0, "intraday_rows": 0,
                "daily_errors": 0, "intraday_errors": 0}

    # Independent progress counters per side, each with its own ~10% step
    # so the two streams log separately even though they run interleaved.
    daily_total    = len(daily_todo)
    intraday_total = len(intraday_todo)
    daily_done    = 0
    intraday_done = 0
    daily_step    = max(1, daily_total    // 10) if daily_total    else 1
    intraday_step = max(1, intraday_total // 10) if intraday_total else 1

    def _tick_daily() -> None:
        nonlocal daily_done
        daily_done += 1
        if daily_done % daily_step == 0 or daily_done == daily_total:
            logger.info(
                "Daily backfill progress: %d/%d symbols (rows= %d errors= %d)",
                daily_done, daily_total,
                counters["daily_rows"], counters["daily_errors"],
            )

    def _tick_intraday() -> None:
        nonlocal intraday_done
        intraday_done += 1
        if intraday_done % intraday_step == 0 or intraday_done == intraday_total:
            logger.info(
                "Intraday backfill progress: %d/%d symbols (rows= %d errors= %d)",
                intraday_done, intraday_total,
                counters["intraday_rows"], counters["intraday_errors"],
            )

    async def _do_daily(symbol: str, symbolid: int) -> None:
        async with sem:
            try:
                daily_all = await _backfill_daily_for_symbol(rest, symbol, symbolid, daily_end)
                daily = [b for b in daily_all if b.d >= daily_cutoff]
                if daily:
                    for d in {b.d for b in daily}:
                        await ensure_partition_daily(pool, d)
                    await bulk_insert_daily_bars(pool, daily)
                    counters["daily_rows"] += len(daily)
            except Exception:
                counters["daily_errors"] += 1
                logger.exception("%s: daily backfill failed", symbol)
            finally:
                _tick_daily()

    async def _do_intraday(symbol: str, symbolid: int, need_from: date) -> None:
        async with sem:
            try:
                intraday_all = await _backfill_intraday_for_symbol(
                    rest, symbol, symbolid, need_from, intraday_end,
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
                counters["intraday_errors"] += 1
                logger.exception("%s: intraday backfill failed", symbol)
            finally:
                _tick_intraday()

    tasks = (
        [_do_daily(s, sid) for s, sid in daily_todo]
        + [_do_intraday(s, sid, nf) for s, sid, nf in intraday_todo]
    )
    await asyncio.gather(*tasks)

    logger.info(
        "Backfilling complete -- daily: %d rows / %d errors, intraday: %d rows / %d errors",
        counters["daily_rows"],    counters["daily_errors"],
        counters["intraday_rows"], counters["intraday_errors"],
    )
    return counters["daily_rows"], counters["intraday_rows"]


# ---------------------------------------------------------------------------
# daily_indicators compute phase  (raw daily -> ATR14 -> daily_indicators)
# ---------------------------------------------------------------------------


async def _compute_daily_indicators(pool: asyncpg.Pool, today: date) -> None:
    """
    Rebuild ``daily_indicators`` from whatever's currently in ``daily``.

    Reads every raw daily row in the retention window (one batched
    query), groups by symbol in pandas, runs ``compute_atr_series`` per
    group, and bulk-upserts ``(symbolid, date, atr)`` into daily_indicators.

    Runs AFTER the raw daily inserts completed so the compute sees the
    freshest data. Partitions for the target dates are ensured up front
    -- retention drop already ran earlier in startup, so we only need
    the write-side partitions.
    """
    since = today - timedelta(days=settings.DAILY_BACKFILL_DAYS - 1)
    df = await load_daily_for_atr_compute(pool, since)

# Ensure ATR14 is computed per symbol, then bulk-insert the results into
    for _, grp in df.groupby("symbolid", sort=False):
        enriched = calculate_atr_series(grp, span=settings.ATR_SAMPLE_SESSIONS)
        df.loc[grp.index, "atr"] = enriched["atr"]

    df.dropna(subset=["atr"], inplace=True)

    out = list(df[["symbolid", "date", "atr"]].itertuples(index=False, name=None))

    if not out:
        logger.info("daily_indicators: no ATR values produced -- nothing to write")
        return

    # Ensure a daily_indicators partition exists for every target date.
    for d in {row[1] for row in out}:
        await ensure_partition_daily_indicators(pool, d)

    await bulk_insert_daily_indicators(pool, out)
    logger.info(
        "daily_indicators: wrote %d rows across %d symbols",
        len(out), df["symbolid"].nunique(),
    )
