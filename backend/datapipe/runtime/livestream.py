"""
Live WS consumer for Massive/Polygon /stocks/AM.

Contract:
  * Connect, auth, subscribe to AM.<sym> for every active monitored symbol.
  * Every incoming message is validated as AggregateMinuteMessage, mapped
    to a symbolid via the in-memory map, and passed through
    ``bar_processor.process_bar``.
  * Reconnect with exponential backoff on transport failures; each
    reconnect re-subscribes to the current active set (in case the
    universe was refreshed while we were down).
  * Session boundary: the historian is called before we open the socket,
    and ``empty_livestream_table`` runs then so livestream = today only.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import date, datetime, timezone
from typing import Optional

import asyncpg
import websockets

from backend.core.config import settings
from backend.datapipe.runtime.aggregation import BarAggregator
from backend.datapipe.runtime.bar_processor import BarSink, process_bar
from backend.datapipe.runtime.priming import (
    bulk_persist_bars,
    enrich_prime_bars,
    seed_session_state,
)
from backend.datapipe.runtime.session_state import SessionStore
from backend.datapipe.schemas import AggregateMinuteMessage, Bar, MonitoredSymbols
from backend.datapipe.sources.datasource import fetch_intraday_bars
from backend.datapipe.time_utils import session_date_et, to_helsinki
from backend.dependencies import RestClient

logger = logging.getLogger(__name__)




async def _fetch_today_intraday_bars(
    rest: RestClient,
    symbol_map: MonitoredSymbols,
    day: date,
    concurrency: int,
) -> list[tuple[str, int, list[Bar]]]:
    """
    REST-fetch today's already-occurred bars for every active symbol
    with bounded parallelism. Returns ``(sym, sid, [bars])`` tuples in
    the shape ``enrich_and_bulk_persist`` expects.

    Polygon serves bars at the aggregation cadence server-side
    (BAR_MINUTES/minute); no client aggregation is needed here.
    """
    sem = asyncio.Semaphore(concurrency)

    async def _fetch(sym: str, sid: int) -> tuple[str, int, list[Bar]]:
        async with sem:
            raw = await fetch_intraday_bars(rest, sym, day)
            return sym, sid, [b.to_bar(symbol=sym, symbolid=sid) for b in raw]

    return await asyncio.gather(*[
        _fetch(sym, sid) for sym, sid in symbol_map.items()
    ])


async def _initialize_livestream(
    pool: asyncpg.Pool,
    rest: RestClient,
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
        _fetch_today_intraday_bars(rest, symbol_map, today_et, concurrency), # Hakee Rest apista tämän päivän jo tapahtuneet barit samaan aikaan kun sessio alustetaan
    )
    enriched = enrich_prime_bars(store, fetched) # jatkojalostaa tämän päivän baarit
    await bulk_persist_bars(pool, enriched)



def _parse_am_event(ev: dict, symbol_map: MonitoredSymbols) -> Optional[Bar]:
    """
    Filter to ``ev == "AM"``, validate the payload, resolve the symbolid,
    return the canonical raw ``Bar``. Anything else returns ``None``.

    Polygon sends non-AM control frames on the same stream:
      * ``{"ev":"status","status":"auth_success",...}`` after auth
      * ``{"ev":"status","status":"success","message":"subscribed to: AM.<sym>"}``
        per subscribed symbol (so 1700+ of these right after subscribe)
    These are handshake noise, not data -- log at DEBUG and skip.
    """
    if ev.get("ev") != "AM":
        logger.debug("control frame: %s", ev)
        return None
    try:
        msg = AggregateMinuteMessage.model_validate(ev)
    except Exception:
        logger.warning("AM validation failed: %s", ev)
        return None
    sid = symbol_map.get(msg.sym)
    if sid is None:
        return None
    return msg.to_bar(symbolid=sid)


async def _consume(
    ws,
    pool: asyncpg.Pool,
    store: SessionStore,
    symbol_map: MonitoredSymbols,
    sink: Optional[BarSink],
) -> None:
    """
    Drain frames from the socket. Each raw 1-min AM bar is aggregated
    per symbol; the enriched N-min bar is enriched, persisted, and logged
    once the window closes.
    """
    aggregators: dict[str, BarAggregator] = {}

    async for raw in ws:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("non-JSON frame dropped: %r", raw[:200])
            continue

        events = payload if isinstance(payload, list) else [payload]
        for ev in events:
            if not isinstance(ev, dict):
                continue

            raw_bar = _parse_am_event(ev, symbol_map)
            if raw_bar is None:
                continue

            agg = aggregators.setdefault(raw_bar.symbol, BarAggregator())
            bar = agg.feed(raw_bar)
            if bar is None:
                # Window still filling -- wait for the next 1-min bar.
                continue

            try:
                await process_bar(pool, store, bar, sink=sink)
                
                logger.info(
                    "%s %s | O=%.4f H=%.4f L=%.4f C=%.4f V=%d rvol=%s relatr=%s",
                    bar.symbol,
                    to_helsinki(bar.ts).strftime("%H:%M"),
                    bar.open, bar.high, bar.low, bar.close, bar.volume,
                    f"{bar.rvol_cum:.2f}",
                    f"{bar.relatr:.2f}"  ,
                )
            except Exception:
                logger.exception("process_bar failed for %s @ %s", bar.symbol, bar.ts)


async def run_livestream(
    pool: asyncpg.Pool,
    rest: RestClient,
    symbol_map: MonitoredSymbols,
    sink: Optional[BarSink] = None,
) -> None:

    store = SessionStore()
    url = settings.POLYGON_WS_URL

    try:
        await _initialize_livestream(pool, rest, store, symbol_map)

        logger.info("Connecting to %s (%d symbols)", url, len(symbol_map))
        async with websockets.connect(url, max_size=8 * 1024 * 1024) as ws:
            await ws.send(json.dumps({"action": "auth", "params": settings.POLYGON_API_KEY}))
            sub_params = ",".join(f"AM.{s}" for s in symbol_map.keys())
            await ws.send(json.dumps({"action": "subscribe", "params": sub_params}))

            await _consume(ws, pool, store, symbol_map, sink)

        logger.warning("Socket closed -- run_livestream returning")

    except asyncio.CancelledError:
        logger.info("Task cancelled -- exiting")
        raise
    except Exception:
        logger.exception("Livestream failed -- not reconnecting, task will exit")
        raise
