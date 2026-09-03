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
from backend.database.readers import load_daily_for_indicators_compute
from backend.database.writers import (
    bulk_insert_daily_bars,
    bulk_insert_daily_indicators,
    bulk_insert_intraday_bars,
    record_backfill_run,
)
from backend.datapipe.calculations import rvol_baseline as rvol_model
from indicators.atr import atr_series
from indicators.sma import sma_series
from backend.datapipe.schemas import (
    BAR_MINUTES,
    CandleRow,
    DailyBar,
    MonitoredSymbols,
    candle_row_from_incoming,
)
from backend.datapipe.time_utils import previous_trading_day, session_date_et
from data_sources._base import BarSize, HistoryWindow
from data_sources.polygon import PolygonHistoricalSource, PolygonSource

logger = logging.getLogger(__name__)


# ============================================================================
# Per-symbol fetch primitives
# ============================================================================


async def _backfill_intraday_for_symbol(
    polygon: PolygonSource,
    symbol: str,
    symbolid: int,
    start_day: date,
    end_day: date,
) -> list[CandleRow]:
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
    return [candle_row_from_incoming(b, symbol, symbolid) for b in ibs]


# ============================================================================
# Grouped-daily bulk seed (deep-history fetch for ``daily``)
# ============================================================================


async def _bulk_backfill_daily_grouped(
    pool: asyncpg.Pool,
    polygon: PolygonSource,
    symbol_map: MonitoredSymbols,
    start_day: date,
    end_day: date,
    concurrency: int,
) -> int:
    """
    Seed the ``daily`` table for every active symbol using Polygon's
    grouped-daily endpoint. One HTTP call per trading day returns
    OHLCV for the ENTIRE US stock market, so a 200-session backfill
    across ~1,500 active symbols costs ~200 HTTP calls (one per
    calendar day in the window, weekends/holidays skipped by
    Polygon returning an empty ``results``) instead of ~1,500 (one
    per symbol at the per-symbol REST path).

    Calendar days in ``[start_day, end_day]`` are all requested;
    non-trading days simply return zero rows and cost one no-op
    call. Rows are filtered to the ``symbol_map`` (only active
    monitored symbols) before insert; the ``T`` (ticker) field is
    resolved to ``symbolid`` via the map.

    Concurrency is capped by ``concurrency`` so we don't stampede
    the polygon session. Returns the total rows inserted across all
    days.
    """
    sym_to_id = symbol_map  # {symbol: symbolid}
    keep      = sym_to_id.keys()

    calendar_days: list[date] = []
    d = start_day
    while d <= end_day:
        calendar_days.append(d)
        d += timedelta(days=1)

    sem      = asyncio.Semaphore(concurrency)
    counters = {"days_with_data": 0, "rows": 0, "errors": 0}
    total    = len(calendar_days)
    done     = 0
    step     = max(1, total // 10)

    src = PolygonHistoricalSource(polygon)

    async def _do_day(day: date) -> None:
        nonlocal done
        async with sem:
            try:
                results = await src.fetch_grouped_day(day)
            except Exception:
                counters["errors"] += 1
                logger.exception("grouped-daily fetch failed for %s", day)
                results = []

            bars: list[DailyBar] = []
            for r in results:
                sym = r.get("T")
                if sym not in keep:
                    continue
                try:
                    bars.append(DailyBar(
                        symbol   = sym,
                        symbolid = sym_to_id[sym],
                        d        = day,
                        open     = float(r["o"]),
                        high     = float(r["h"]),
                        low      = float(r["l"]),
                        close    = float(r["c"]),
                        volume   = int(r["v"]),
                    ))
                except (KeyError, TypeError, ValueError):
                    # Illiquid rows Polygon returns without ohlcv or with weird
                    # types -- skip rather than fail the whole day.
                    continue

            if bars:
                await ensure_partition_daily(pool, day)
                await bulk_insert_daily_bars(pool, bars)
                counters["days_with_data"] += 1
                counters["rows"]           += len(bars)

            done += 1
            if done % step == 0 or done == total:
                logger.info(
                    "Grouped-daily progress: %d/%d days (rows= %d, trading_days= %d, errors= %d)",
                    done, total,
                    counters["rows"], counters["days_with_data"], counters["errors"],
                )

    logger.info(
        "Grouped-daily bulk seed: %d calendar days (%s..%s) for %d symbols",
        total, start_day.isoformat(), end_day.isoformat(), len(sym_to_id),
    )
    await asyncio.gather(*(_do_day(d) for d in calendar_days))
    logger.info(
        "Grouped-daily bulk seed complete -- %d rows over %d trading days (%d errors)",
        counters["rows"], counters["days_with_data"], counters["errors"],
    )
    return counters["rows"]


# ============================================================================
# Bounded-concurrency worker pool
# ============================================================================


async def _run_intraday_workers(
    pool: asyncpg.Pool,
    polygon: PolygonSource,
    today: date,
    intraday_end: date,
    intraday_todo: list[tuple[str, int, date]],
    concurrency: int,
) -> int:
    """
    Per-symbol intraday backfill worker pool.

    Polygon has no grouped endpoint at the intraday cadence, so this
    side stays per-symbol; the daily side moved to the grouped-daily
    bulk seed in ``_bulk_backfill_daily_grouped``.

    Returns the total intraday rows written so the caller can decide
    whether the rvol_baseline rebuild pass needs to run.
    """
    if not intraday_todo:
        return 0

    intraday_cutoff = today - timedelta(days=settings.INTRADAY_BACKFILL_DAYS - 1)
    sem = asyncio.Semaphore(concurrency)
    counters = {"rows": 0, "errors": 0}

    total = len(intraday_todo)
    done  = 0
    step  = max(1, total // 10)

    async def _do_intraday(symbol: str, symbolid: int, need_from: date) -> None:
        nonlocal done
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
                    counters["rows"] += len(intraday)
            except Exception:
                counters["errors"] += 1
                logger.exception("%s: intraday backfill failed", symbol)
            finally:
                done += 1
                if done % step == 0 or done == total:
                    logger.info(
                        "Intraday backfill progress: %d/%d symbols (rows= %d errors= %d)",
                        done, total, counters["rows"], counters["errors"],
                    )

    await asyncio.gather(*(_do_intraday(s, sid, nf) for s, sid, nf in intraday_todo))
    logger.info(
        "Intraday backfilling complete -- %d rows / %d errors",
        counters["rows"], counters["errors"],
    )
    return counters["rows"]


# ============================================================================
# Derived-data compute passes (ATR + RVOL baseline)
# ============================================================================


async def _compute_daily_indicators(pool: asyncpg.Pool, today: date) -> None:
    """
    Rebuild ``daily_indicators`` from whatever's currently in ``daily``.

    Reads every raw daily row in the retention window (one batched
    query), groups by symbol in pandas, and computes:

      * ATR14  via ``indicators.atr.atr_series``  (needs 14 sessions warm-up)
      * SMA200 via ``indicators.sma.sma_series``  (needs 200 sessions warm-up)

    Bulk-upserts ``(symbolid, date, atr, sma200)`` into daily_indicators.
    Either indicator may be ``None`` on a per-row basis when its
    warm-up window isn't satisfied; a row is written as long as at
    least one indicator resolved (so partial history still feeds ATR-
    only strategies while SMA200 is still warming up).

    Runs AFTER the raw daily inserts completed so the compute sees the
    freshest data. Partitions for the target dates are ensured up front.
    """
    since = today - timedelta(days=settings.DAILY_BACKFILL_DAYS - 1)
    df = await load_daily_for_indicators_compute(pool, since)

    if df.empty:
        logger.info("daily_indicators: no daily rows in window -- nothing to compute")
        return

    # ATR14 + SMA200 are both stateful per symbol -- compute one group at a time.
    df["atr"]    = pd.NA
    df["sma200"] = pd.NA
    for _, grp in df.groupby("symbolid", sort=False):
        df.loc[grp.index, "atr"] = atr_series(
            grp["high"], grp["low"], grp["close"],
            span=settings.ATR_SAMPLE_SESSIONS,
        )
        df.loc[grp.index, "sma200"] = sma_series(
            grp["close"],
            period=settings.SMA200_SAMPLE_SESSIONS,
        )

    # Drop rows where BOTH indicators are still warming up -- nothing to persist.
    df = df.dropna(subset=["atr", "sma200"], how="all")
    if df.empty:
        logger.info("daily_indicators: no indicator values produced -- nothing to write")
        return

    # Cast pandas NA -> Python None so asyncpg encodes them as SQL NULL.
    out = [
        (
            int(r.symbolid),
            r.date,
            None if pd.isna(r.atr)    else float(r.atr),
            None if pd.isna(r.sma200) else float(r.sma200),
        )
        for r in df.itertuples(index=False)
    ]

    for d in {row[1] for row in out}:
        await ensure_partition_daily_indicators(pool, d)

    await bulk_insert_daily_indicators(pool, out)
    sma_rows = sum(1 for row in out if row[3] is not None)
    logger.info(
        "daily_indicators: wrote %d rows across %d symbols "
        "(SMA200 populated on %d rows)",
        len(out), df["symbolid"].nunique(), sma_rows,
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
    #    Daily side goes through the grouped-daily bulk seed, so no
    #    per-symbol daily todo list is needed here.
    intraday_start = today - timedelta(days=settings.INTRADAY_BACKFILL_DAYS)
    intraday_end   = today - timedelta(days=1)
    daily_end      = previous_trading_day(today)
    daily_start    = today - timedelta(days=settings.DAILY_BACKFILL_DAYS - 1)

    intraday_todo: list[tuple[str, int, date]] = (
        [(sym, sid, intraday_start) for sym, sid in symbol_map.items()] if need_intraday else []
    )

    # 3a. Daily bulk seed (grouped-daily -- one call per trading day for the
    #     whole US market, ~200 calls to cover ~200 sessions for every
    #     active symbol, vs ~1,500 per-symbol calls at the per-symbol path).
    daily_rows = 0
    if need_daily:
        daily_rows = await _bulk_backfill_daily_grouped(
            pool, polygon, symbol_map,
            start_day=daily_start,
            end_day=daily_end,
            concurrency=concurrency,
        )

    # 3b. Intraday worker pool (still per-symbol -- Polygon has no grouped
    #     endpoint at the intraday cadence).
    intraday_rows = await _run_intraday_workers(
        pool, polygon, today, intraday_end,
        intraday_todo=intraday_todo, concurrency=concurrency,
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
