"""
Historian -- REST backfill.

Responsibilities on startup (or whenever the live loop isn't running):

  1. Make sure the ``daily`` table has enough sessions on disk for every
     active monitored symbol (feeds ATR14).
  2. Make sure ``intraday_bars`` has the last N sessions of aggregation-
     cadence bars for every active monitored symbol (feeds RVOL baseline
     + gives strategies recent context).
  3. Rebuild ``rvol_baseline`` when new intraday rows landed.
  4. Compute ``daily_indicators`` (ATR14) when new daily rows landed.
  5. Record the run in ``backfill_status`` so the next startup can gate
     itself (one backfill per side per day).

The freshness decision lives in ``pipeline.startup``; historian just
runs what it's told via ``need_daily`` / ``need_intraday``.

Explicitly does NOT run while the live stream is running (as per
instructions.md). Startup calls this before opening the WS.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, time, timedelta, timezone

import asyncpg
import pandas as pd

from backend.core.config import settings
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
from backend.datapipe.calculations import rvol_baseline as rvol_model
from backend.datapipe.calculations.calculations import calculate_atr_series
from backend.datapipe.schemas import BAR_MINUTES, Bar, DailyBar, MonitoredSymbols
from backend.datapipe.time_utils import previous_trading_day, session_date_et
from data_sources._base import BarSize, HistoryWindow
from data_sources.polygon import PolygonHistoricalSource, PolygonSource

logger = logging.getLogger(__name__)


# ============================================================================
# Per-symbol fetch primitives
# ============================================================================


async def _backfill_daily_for_symbol(
    polygon: PolygonSource,
    symbol: str,
    symbolid: int,
    up_to: date,
) -> list[DailyBar]:
    """
    Fetch raw daily OHLCV bars from Polygon via the source-agnostic
    ``HistoricalSource`` seam.

    Always fetches the full DAILY_BACKFILL_DAYS window (with a 2x
    calendar buffer for weekends/holidays) because the later ATR14
    compute pass needs 14 trading days of context per symbol.

    Returned DailyBars are RAW ONLY -- no ATR / derived fields. Those
    live in ``daily_indicators`` and are populated by
    ``_compute_daily_indicators`` after the raw daily inserts finish.
    """
    lookback = settings.DAILY_BACKFILL_DAYS * 2
    end_dt = datetime.combine(up_to, time(23, 59, 59), tzinfo=timezone.utc)
    window = HistoryWindow(
        bar_size      = BarSize.DAILY,
        lookback_days = lookback,
        end           = end_dt,
    )
    ibs = await PolygonHistoricalSource(polygon).fetch(symbol, window)
    return [DailyBar.from_incoming(b, symbol, symbolid) for b in ibs]


async def _backfill_intraday_for_symbol(
    polygon: PolygonSource,
    symbol: str,
    symbolid: int,
    start_day: date,
    end_day: date,
) -> list[Bar]:
    """
    Fetch aggregation-cadence bars (BAR_MINUTES/minute) via the
    ``HistoricalSource`` seam. Polygon does the aggregation
    server-side, so no client batching is needed here.
    """
    lookback = (end_day - start_day).days + 1
    end_dt = datetime.combine(end_day, time(23, 59, 59), tzinfo=timezone.utc)
    window = HistoryWindow(
        bar_size      = BarSize(f"{BAR_MINUTES}m"),
        lookback_days = lookback,
        end           = end_dt,
    )
    ibs = await PolygonHistoricalSource(polygon).fetch(symbol, window)
    return [Bar.from_incoming(b, symbol, symbolid) for b in ibs]


# ============================================================================
# Bounded-concurrency worker pool
# ============================================================================


async def _run_backfill_workers(
    pool: asyncpg.Pool,
    polygon: PolygonSource,
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

    Returns ``(daily_rows_added, intraday_rows_added)`` so the caller
    can decide whether the derived-data compute passes need to run.
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
    daily_done     = 0
    intraday_done  = 0
    daily_step     = max(1, daily_total    // 10) if daily_total    else 1
    intraday_step  = max(1, intraday_total // 10) if intraday_total else 1

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
                daily_all = await _backfill_daily_for_symbol(polygon, symbol, symbolid, daily_end)
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
                    polygon, symbol, symbolid, need_from, intraday_end,
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


# ============================================================================
# Derived-data compute passes (ATR + RVOL baseline)
# ============================================================================


async def _compute_daily_indicators(pool: asyncpg.Pool, today: date) -> None:
    """
    Rebuild ``daily_indicators`` from whatever's currently in ``daily``.

    Reads every raw daily row in the retention window (one batched
    query), groups by symbol in pandas, runs ``calculate_atr_series``
    per group, and bulk-upserts ``(symbolid, date, atr)`` into
    daily_indicators.

    Runs AFTER the raw daily inserts completed so the compute sees the
    freshest data. Partitions for the target dates are ensured up front.
    """
    since = today - timedelta(days=settings.DAILY_BACKFILL_DAYS - 1)
    df = await load_daily_for_atr_compute(pool, since)

    # ATR14 is stateful per symbol -- compute one group at a time.
    for _, grp in df.groupby("symbolid", sort=False):
        enriched = calculate_atr_series(grp, span=settings.ATR_SAMPLE_SESSIONS)
        df.loc[grp.index, "atr"] = enriched["atr"]

    df.dropna(subset=["atr"], inplace=True)
    out = list(df[["symbolid", "date", "atr"]].itertuples(index=False, name=None))

    if not out:
        logger.info("daily_indicators: no ATR values produced -- nothing to write")
        return

    for d in {row[1] for row in out}:
        await ensure_partition_daily_indicators(pool, d)

    await bulk_insert_daily_indicators(pool, out)
    logger.info(
        "daily_indicators: wrote %d rows across %d symbols",
        len(out), df["symbolid"].nunique(),
    )


async def _rebuild_rvol_model(
    pool: asyncpg.Pool, today: date, need_intraday: bool, intraday_rows: int,
) -> None:
    """Rebuild the RVOL baseline iff intraday backfill actually landed new rows."""
    if need_intraday and intraday_rows > 0:
        logger.info("New intraday data (%d rows) -- rebuilding rvol_baseline", intraday_rows)
        await rvol_model.rebuild_rvol_model(
            pool, end_day=today,
            lookback_days=settings.INTRADAY_BACKFILL_DAYS,
            sample_sessions=settings.RVOL_SAMPLE_SESSIONS,
        )
    else:
        logger.info(
            "Skipping rvol_baseline rebuild (need_intraday=%s intraday_rows=%d)",
            need_intraday, intraday_rows,
        )


# ============================================================================
# Orchestrator (public API)
# ============================================================================


async def backfill_all_symbols(
    pool: asyncpg.Pool,
    polygon: PolygonSource,
    today: date,
    symbol_map: MonitoredSymbols,
    *,
    need_daily: bool,
    need_intraday: bool,
    concurrency: int = 10,
) -> None:
    """
    End-to-end warmup for every active symbol.

    Composition of the phases above. The freshness decision lives in
    the caller (pipeline.startup) via the ``need_daily`` / ``need_intraday``
    flags; passing both False is a no-op.

    Fetch-end semantics:
      * intraday_end -- ``today - 1`` (live path -- today's intraday bars
                        arrive via the WS livestream, not the REST
                        backfill).
      * daily_end    -- ``previous_trading_day(today)`` so ATR14 never
                        folds today's own row into today's RelATR (would
                        leak look-ahead).
    """
    logger.info(
        "Backfill starting for %d symbols "
        "(today= %s, need_daily= %s, need_intraday= %s)",
        len(symbol_map), today.isoformat(), need_daily, need_intraday,
    )

    # 1. Ensure partitions exist for the retention window forward.
    intraday_days = [today - timedelta(days=i) for i in range(settings.INTRADAY_BACKFILL_DAYS)]
    daily_days    = [today - timedelta(days=i) for i in range(settings.DAILY_BACKFILL_DAYS)]
    await ensure_partitions_for_dates(pool, intraday_days, daily_days)

    # 2. Build the todo lists.
    intraday_start = today - timedelta(days=settings.INTRADAY_BACKFILL_DAYS)
    intraday_end   = today - timedelta(days=1)
    daily_end      = previous_trading_day(today)

    daily_todo:    list[tuple[str, int]]       = (
        [(sym, sid) for sym, sid in symbol_map.items()] if need_daily else []
    )
    intraday_todo: list[tuple[str, int, date]] = (
        [(sym, sid, intraday_start) for sym, sid in symbol_map.items()] if need_intraday else []
    )

    # 3. Fetch + insert raw bars.
    daily_rows, intraday_rows = await _run_backfill_workers(
        pool, polygon, today, daily_end, intraday_end,
        daily_todo, intraday_todo, concurrency,
    )

    # 4. Derived-data compute passes (only if new raw data landed).
    if need_daily and daily_rows > 0:
        logger.info("New daily data (%d rows) -- computing daily_indicators", daily_rows)
        await _compute_daily_indicators(pool, today)
    else:
        logger.info(
            "Skipping daily_indicators compute (need_daily=%s daily_rows=%d)",
            need_daily, daily_rows,
        )
    await _rebuild_rvol_model(pool, today, need_intraday, intraday_rows)

    # 5. Ledger the successful run so the next startup can gate itself.
    now_utc = datetime.now(timezone.utc)
    await record_backfill_run(
        pool,
        daily_last_run    = now_utc if need_daily    else None,
        intraday_last_run = now_utc if need_intraday else None,
    )
