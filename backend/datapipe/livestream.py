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
    and ``truncate_livestream`` runs then so livestream = today only.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Optional

import asyncpg
import websockets

from backend.core.config import settings
from backend.database.readers import (
    load_active_symbol_map,
    load_latest_atr_map,
    load_rvol_baseline_for_symbol,
)
from backend.datapipe.bar_processor import BarSink, process_bar
from backend.datapipe.schemas import AggregateMinuteMessage, Bar1m
from backend.datapipe.session_state import SessionStore

logger = logging.getLogger(__name__)


# Massive WS URL is served under the base host, path /stocks. We accept
# either a full ws:// URL in POLYGON_BASE_URL or an https:// one and
# rewrite the scheme.
def _ws_url() -> str:
    base = settings.POLYGON_BASE_URL.rstrip("/")
    if base.startswith("https://"):
        base = "wss://" + base[len("https://"):]
    elif base.startswith("http://"):
        base = "ws://" + base[len("http://"):]
    return f"{base}/stocks"


async def _prime_state(
    pool: asyncpg.Pool,
    store: SessionStore,
    symbol_map: dict[str, int],
) -> None:
    """Load ATR + rvol baseline into per-symbol session state before we open the socket."""
    logger.info("[livestream] priming per-symbol state (ATR + rvol baseline) for %d symbols", len(symbol_map))
    atr_map = await load_latest_atr_map(pool)
    missing_atr = 0
    missing_baseline = 0
    for sym, sid in symbol_map.items():
        baseline = await load_rvol_baseline_for_symbol(pool, sid)
        from datetime import date
        st = store.get_or_init(sym, sid, date.today())
        st.atr = atr_map.get(sid)
        st.rvol_baseline = baseline
        if st.atr is None:
            missing_atr += 1
        if not baseline:
            missing_baseline += 1
    logger.info(
        "[livestream] state primed -- %d symbols missing ATR, %d missing rvol baseline",
        missing_atr, missing_baseline,
    )


async def _consume(
    ws,
    pool: asyncpg.Pool,
    store: SessionStore,
    symbol_map: dict[str, int],
    sink: Optional[BarSink],
) -> None:
    """
    Drain frames from the socket. Massive sends arrays of events per frame;
    we filter to ev=="AM" and dispatch each. Anything unparseable is logged
    but never terminates the loop.

    Emits three flavors of INFO log so an operator can tell the stream is alive:
      * First bar per symbol -- confirms subscription for that ticker.
      * Every 60s -- rolling bar-rate summary (bars/sec + total).
      * Ignored / dropped events -- warning level.
    """
    bar_count = 0
    drop_count = 0
    seen_symbols: set[str] = set()
    last_summary = time.monotonic()
    SUMMARY_INTERVAL = 60.0  # seconds

    async for raw in ws:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("[livestream] non-JSON frame dropped: %r", raw[:200])
            continue

        events = payload if isinstance(payload, list) else [payload]
        for ev in events:
            if not isinstance(ev, dict):
                continue
            ev_type = ev.get("ev")
            if ev_type != "AM":
                # status / auth_success / etc. -- log at INFO so the operator
                # can confirm auth/subscribe handshakes actually succeeded.
                logger.info("[livestream] control frame: %s", ev)
                continue
            try:
                msg = AggregateMinuteMessage.model_validate(ev)
            except Exception:
                logger.warning("[livestream] AM validation failed: %s", ev)
                drop_count += 1
                continue
            sid = symbol_map.get(msg.sym)
            if sid is None:
                drop_count += 1
                continue
            bar: Bar1m = msg.to_bar1m(symbolid=sid)
            try:
                await process_bar(pool, store, bar, sink=sink)
                bar_count += 1
                if bar.symbol not in seen_symbols:
                    seen_symbols.add(bar.symbol)
                    logger.info(
                        "[livestream] first bar for %s @ %s (close=%.4f vol=%d)",
                        bar.symbol, bar.ts.isoformat(), bar.close, bar.volume,
                    )
            except Exception:
                logger.exception("[livestream] process_bar failed for %s @ %s", bar.symbol, bar.ts)
                drop_count += 1

        now = time.monotonic()
        if now - last_summary >= SUMMARY_INTERVAL:
            rate = bar_count / (now - last_summary)
            logger.info(
                "[livestream] rolling summary: bars=%d rate=%.2f/s dropped=%d subscribed=%d seen=%d",
                bar_count, rate, drop_count, len(symbol_map), len(seen_symbols),
            )
            bar_count = 0
            drop_count = 0
            last_summary = now


async def run_livestream(
    pool: asyncpg.Pool,
    sink: Optional[BarSink] = None,
    reconnect_initial_delay: float = 1.0,
    reconnect_max_delay: float = 30.0,
) -> None:
    """
    Long-running task the FastAPI lifespan spawns. Never returns until
    cancelled. Reconnects with exponential backoff on transport errors;
    validation / per-message errors are non-fatal (see ``_consume``).
    """
    store = SessionStore()
    url = _ws_url()
    delay = reconnect_initial_delay

    logger.info("[livestream] task started (target URL=%s)", url)

    while True:
        try:
            symbol_map = await load_active_symbol_map(pool)
            if not symbol_map:
                logger.warning("[livestream] no active symbols -- sleeping 60s and retrying")
                await asyncio.sleep(60)
                continue

            await _prime_state(pool, store, symbol_map)

            logger.info("[livestream] connecting to %s (%d symbols)", url, len(symbol_map))
            async with websockets.connect(url, max_size=8 * 1024 * 1024) as ws:
                logger.info("[livestream] WS connected -- sending auth")
                await ws.send(json.dumps({"action": "auth", "params": settings.POLYGON_API_KEY}))
                sub_params = ",".join(f"AM.{s}" for s in symbol_map.keys())
                logger.info("[livestream] sending subscribe for %d symbols", len(symbol_map))
                await ws.send(json.dumps({"action": "subscribe", "params": sub_params}))
                logger.info("[livestream] subscribe request sent -- entering consume loop")

                delay = reconnect_initial_delay
                await _consume(ws, pool, store, symbol_map, sink)
                logger.warning("[livestream] socket closed cleanly -- reconnecting")

        except asyncio.CancelledError:
            logger.info("[livestream] task cancelled -- exiting reconnect loop")
            raise
        except Exception:
            logger.exception("[livestream] connection failure -- reconnecting in %.1fs", delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, reconnect_max_delay)
