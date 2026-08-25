"""
Live path orchestrator on top of the ``data_sources.polygon`` seam.

Contract:
  * Seed per-symbol state (ATR + rvol baseline) and REST-prime today's
    already-occurred bars via ``PolygonHistoricalSource``.
  * Bulk-persist the primed bars into ``livestream``.
  * Hand the WS to ``PolygonRealtimeSource.subscribe``; the adapter
    handles connect / auth / subscribe / control-frame filtering, and
    emits one ``IncomingBar`` per AM message to the ``on_bar`` callback
    defined inside ``run_livestream``.
  * ``on_bar`` wraps ``IncomingBar`` -> ``Bar`` via ``Bar.from_incoming``,
    feeds the per-symbol ``BarAggregator``, and forwards each finalized
    N-min bar to ``bar_processor.process_bar``.
  * Session boundary: the historian is called before we open the
    socket, and ``empty_livestream_table`` runs then so livestream =
    today only.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, time, timezone
from typing import Optional

import asyncpg

from backend.datapipe.runtime.aggregation import BarAggregator
from backend.datapipe.runtime.bar_processor import BarSink, process_bar
from backend.datapipe.runtime.priming import (
    bulk_persist_bars,
    enrich_prime_bars,
    seed_session_state,
)
from backend.datapipe.runtime.session_state import SessionStore
from backend.datapipe.schemas import BAR_MINUTES, Bar, MonitoredSymbols
from backend.datapipe.time_utils import session_date_et, to_helsinki
from data_sources._bar import IncomingBar
from data_sources._base import BarSize, HistoryWindow
from data_sources.polygon import (
    PolygonHistoricalSource,
    PolygonRealtimeSource,
    PolygonSource,
)

logger = logging.getLogger(__name__)




async def _fetch_today_intraday_bars(
    polygon: PolygonSource,
    symbol_map: MonitoredSymbols,
    day: date,
    concurrency: int,
) -> list[tuple[str, int, list[Bar]]]:
    """
    REST-fetch today's already-occurred bars for every active symbol
    via the ``HistoricalSource`` seam, with bounded parallelism.
    Returns ``(sym, sid, [bars])`` tuples in the shape
    ``enrich_and_bulk_persist`` expects.

    Polygon serves bars at the aggregation cadence server-side
    (BAR_MINUTES/minute); no client aggregation is needed.
    """
    end_dt = datetime.combine(day, time(23, 59, 59), tzinfo=timezone.utc)
    window = HistoryWindow(
        bar_size      = BarSize(f"{BAR_MINUTES}m"),
        lookback_days = 1,
        end           = end_dt,
    )
    hist = PolygonHistoricalSource(polygon)
    sem = asyncio.Semaphore(concurrency)

    async def _fetch(sym: str, sid: int) -> tuple[str, int, list[Bar]]:
        async with sem:
            ibs = await hist.fetch(sym, window)
            return sym, sid, [Bar.from_incoming(b, sym, sid) for b in ibs]

    return await asyncio.gather(*[
        _fetch(sym, sid) for sym, sid in symbol_map.items()
    ])


async def _initialize_livestream(
    pool: asyncpg.Pool,
    polygon: PolygonSource,
    store: SessionStore,
    symbol_map: MonitoredSymbols,
    concurrency: int = 10,
) -> None:
    """
    Compose the startup steps:
      1. Seed per-symbol state (ATR + rvol baseline) from the DB, AND
         REST-fetch today's already-occurred bars -- these are
         independent I/O against different backends, so they run
         concurrently via ``asyncio.gather``.
      2. Enrich the fetched bars via ``apply_bar`` (needs seeded state)
         and bulk-insert into livestream.
    """
    today_et = session_date_et(datetime.now(timezone.utc))
    _, fetched = await asyncio.gather(
        seed_session_state(pool, store, symbol_map, today_et),
        _fetch_today_intraday_bars(polygon, symbol_map, today_et, concurrency),
    )
    enriched = enrich_prime_bars(store, fetched) # jatkojalostaa tämän päivän baarit
    await bulk_persist_bars(pool, enriched)



async def run_livestream(
    pool: asyncpg.Pool,
    polygon: PolygonSource,
    symbol_map: MonitoredSymbols,
    sink: Optional[BarSink] = None,
) -> None:
    """
    Boot the live path:
      * seed per-symbol state (ATR + rvol baseline) + REST-prime
        today's already-occurred bars in parallel,
      * bulk-persist the primed bars into livestream,
      * hand the socket to ``PolygonRealtimeSource.subscribe``.

    ``on_bar`` (defined below) is the seam between the source-agnostic
    ``IncomingBar`` the adapter emits and 32's canonical ``Bar`` the
    aggregator + ``process_bar`` consume. The N-min ``BarAggregator``
    stays on this side of the seam -- mirrors the IB pattern where the
    adapter emits 5-sec bars and the consumer aggregates.
    """
    store = SessionStore()
    try:
        await _initialize_livestream(pool, polygon, store, symbol_map)

        aggregators: dict[str, BarAggregator] = {}

        async def on_bar(incoming: IncomingBar, symbol: str) -> None:
            sid = symbol_map.get(symbol)
            if sid is None:
                return
            raw_bar = Bar.from_incoming(incoming, symbol, sid)
            agg = aggregators.setdefault(symbol, BarAggregator())
            bar = agg.feed(raw_bar)
            if bar is None:
                return
            try:
                await process_bar(pool, store, bar, sink=sink)
                logger.info(
                    "%s %s | O=%.4f H=%.4f L=%.4f C=%.4f V=%d rvol=%s relatr=%s",
                    bar.symbol,
                    to_helsinki(bar.ts).strftime("%H:%M"),
                    bar.open, bar.high, bar.low, bar.close, bar.volume,
                    f"{bar.rvol_cum:.2f}" if bar.rvol_cum is not None else "None",
                    f"{bar.relatr:.2f}"   if bar.relatr   is not None else "None",
                )
            except Exception:
                logger.exception("process_bar failed for %s @ %s", bar.symbol, bar.ts)

        await PolygonRealtimeSource(polygon).subscribe(
            list(symbol_map.keys()), on_bar,
        )
        logger.warning("Socket closed -- run_livestream returning")

    except asyncio.CancelledError:
        logger.info("Task cancelled -- exiting")
        raise
    except Exception:
        logger.exception("Livestream failed -- not reconnecting, task will exit")
        raise
