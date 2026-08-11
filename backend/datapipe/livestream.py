"""
Live WS consumer for Massive/Polygon /stocks/AM.

Contract:
  * Connect, auth, subscribe to AM.<sym> for every active monitored symbol.
  * Every incoming message is validated as AggregateMinuteMessage, mapped
    to a symbolid via the in-memory map, and passed through
    ``bar_processor.process_bar`` -- the same pipeline replay uses.
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
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import asyncpg
import websockets

from backend.core.config import settings
from backend.database.readers import (
    load_latest_atr_map,
    load_rvol_baseline_for_symbol,
)
from backend.database.writers import bulk_insert_livestream_bars
from backend.datapipe.bar_processor import BarSink, process_bar
from backend.datapipe.calculations import enrich_bar
from backend.datapipe.rest_client import RestClient
from backend.datapipe.schemas import AggregateMinuteMessage, Bar1m, MonitoredSymbols
from backend.datapipe.session_state import SessionStore
from backend.datapipe.time_utils import et_time_slot, session_date_et, to_helsinki

logger = logging.getLogger(__name__)




async def _initialize_livestream(
    pool: asyncpg.Pool,
    rest: RestClient,
    store: SessionStore,
    symbol_map: MonitoredSymbols,
    concurrency: int = 10,
) -> None:
    """
    Bootstrap the livestream with today's already-occurred bars.

    Runs EVERY startup, unconditionally, for EVERY monitored symbol -- so
    any gaps left by a prior WS session (missed minutes, disconnects) get
    filled by a fresh REST snapshot. Livestream is truncated first (by
    pipeline) so we're writing into a clean table.

    For each symbol we fetch today's 1-min bars via REST, walk them
    through ``enrich_bar`` in ts order, and bulk-insert the enriched
    result. In-memory session state is populated in the same pass so the
    WS consumer picks up from a correct accumulator.

    intraday_bars is NOT touched -- historian owns that table.
    """
    logger.info("initializing livestream from REST for %d symbols", len(symbol_map))
    today_et = session_date_et(datetime.now(timezone.utc))

    # ATR + baseline: cheap per-symbol reads, needed before enrichment.
    atr_map = await load_latest_atr_map(pool)
    missing_atr = 0
    missing_baseline = 0
    for sym, sid in symbol_map.items():
        baseline = await load_rvol_baseline_for_symbol(pool, sid)
        st = store.get_or_init(sym, sid, today_et)
        st.atr = atr_map.get(sid)
        st.rvol_baseline = baseline
        if st.atr is None:
            missing_atr += 1
        if not baseline:
            missing_baseline += 1

    # REST fetch today's bars per symbol, bounded parallelism.
    sem = asyncio.Semaphore(concurrency)

    async def _fetch(sym: str, sid: int) -> tuple[str, int, list[Bar1m]]:
        async with sem:
            raw = await rest.fetch_intraday_bars(sym, today_et)
            return sym, sid, [b.to_bar1m(symbol=sym, symbolid=sid) for b in raw]

    fetch_results = await asyncio.gather(*[
        _fetch(sym, sid) for sym, sid in symbol_map.items()
    ])
    logger.info("REST fetch complete -- enriching + writing to livestream")

    # Enrich each symbol's bars in ts order (bars come sorted by REST) and
    # accumulate into memory state. All enriched bars get bulk-inserted.
    all_enriched: list[Bar1m] = []
    for sym, sid, bars in fetch_results:
        if not bars:
            continue
        st = store.get_or_init(sym, sid, today_et)
        for bar in bars:
            slot = et_time_slot(bar.ts)
            slot_avg = st.baseline_for_slot(slot)
            enriched = enrich_bar(
                new_bar=bar,
                history=st.history,
                atr=st.atr,
                baseline_slot_avg=slot_avg,
                baseline_history_sum=st.baseline_history_sum,
            )
            st.history.append(enriched)
            st.baseline_history_sum += slot_avg
            all_enriched.append(enriched)

    if all_enriched:
        await bulk_insert_livestream_bars(pool, all_enriched)

    logger.info(
        "livestream initialized -- session=%s bars=%d missing_ATR=%d missing_baseline=%d",
        today_et.isoformat(), len(all_enriched), missing_atr, missing_baseline,
    )


async def _consume(
    ws,
    pool: asyncpg.Pool,
    store: SessionStore,
    symbol_map: MonitoredSymbols,
    sink: Optional[BarSink],
) -> None:
    """
    Drain frames from the socket. Massive sends arrays of events per frame;
    we filter to ev=="AM" and dispatch each. Anything unparseable is logged
    but never terminates the loop.

    Signals of life:
      * First bar per symbol -- confirms subscription for that ticker.
      * Ignored / dropped events -- warning level.

    Full-fidelity raw-frame audit lives in logs/am_stream.log via am_logger.
    """
    seen_symbols: set[str] = set()

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
            ev_type = ev.get("ev")

            # Polygon sends non-AM control frames on the same stream:
            #   * one {"ev":"status","status":"auth_success",...} after auth
            #   * one {"ev":"status","status":"success","message":"subscribed to: AM.<sym>"}
            #     per subscribed symbol (so 1700+ of these right after subscribe)
            # These are handshake noise, not data -- log at DEBUG and skip.
            if ev_type != "AM":
                logger.debug("control frame: %s", ev)
                continue

            try:
                msg = AggregateMinuteMessage.model_validate(ev)
            except Exception:
                logger.warning("AM validation failed: %s", ev)
                continue

            sid = symbol_map.get(msg.sym)
            if sid is None:
                continue
            bar: Bar1m = msg.to_bar1m(symbolid=sid)
            try:
                await process_bar(pool, store, bar, sink=sink)
                logger.info(
                    "%s %s | O=%.4f H=%.4f L=%.4f C=%.4f V=%d VWAP_bar=%s",
                    bar.symbol,
                    to_helsinki(bar.ts).strftime("%H:%M"),
                    bar.open, bar.high, bar.low, bar.close, bar.volume,
                    f"{bar.vwap_bar:.4f}" if bar.vwap_bar is not None else "-",
                )
            except Exception:
                logger.exception("process_bar failed for %s @ %s", bar.symbol, bar.ts)


async def run_livestream(
    pool: asyncpg.Pool,
    rest: RestClient,
    symbol_map: MonitoredSymbols,
    sink: Optional[BarSink] = None,
) -> None:
    """
    Fetch today's already-occurred bars via REST + enrich + write to
    livestream, then open the WS and extend it minute by minute.

    Caller (pipeline.startup) supplies ``symbol_map`` -- already validated
    non-empty there, so we don't re-check.

    intraday_bars is not touched by this path -- historian owns it.
    NO reconnect: any failure propagates to the caller.
    """
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
