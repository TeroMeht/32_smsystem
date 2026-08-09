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
    atr_map = await load_latest_atr_map(pool)
    for sym, sid in symbol_map.items():
        # baseline is per-symbol; keep parallel reads bounded implicitly by asyncpg pool size
        baseline = await load_rvol_baseline_for_symbol(pool, sid)
        # session_date is derived on first bar arrival; init a placeholder now
        from datetime import date
        st = store.get_or_init(sym, sid, date.today())
        st.atr = atr_map.get(sid)
        st.rvol_baseline = baseline


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
    """
    async for raw in ws:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("livestream: non-JSON frame dropped: %r", raw[:200])
            continue

        events = payload if isinstance(payload, list) else [payload]
        for ev in events:
            if not isinstance(ev, dict):
                continue
            if ev.get("ev") != "AM":
                # status / auth_success / etc. -- log at debug
                logger.debug("livestream: non-AM event: %s", ev)
                continue
            try:
                msg = AggregateMinuteMessage.model_validate(ev)
            except Exception:
                logger.warning("livestream: AM validation failed: %s", ev)
                continue
            sid = symbol_map.get(msg.sym)
            if sid is None:
                # Symbol not in our active set (universe changed?) -- ignore
                continue
            bar: Bar1m = msg.to_bar1m(symbolid=sid)
            try:
                await process_bar(pool, store, bar, sink=sink)
            except Exception:
                logger.exception("livestream: process_bar failed for %s @ %s", bar.symbol, bar.ts)


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

    while True:
        try:
            symbol_map = await load_active_symbol_map(pool)
            if not symbol_map:
                logger.warning("livestream: no active symbols -- sleeping 60s")
                await asyncio.sleep(60)
                continue

            await _prime_state(pool, store, symbol_map)

            logger.info("livestream: connecting to %s (%d symbols)", url, len(symbol_map))
            async with websockets.connect(url, max_size=8 * 1024 * 1024) as ws:
                # Auth
                await ws.send(json.dumps({"action": "auth", "params": settings.POLYGON_API_KEY}))
                # Subscribe -- comma-separated AM.<sym> params
                sub_params = ",".join(f"AM.{s}" for s in symbol_map.keys())
                await ws.send(json.dumps({"action": "subscribe", "params": sub_params}))
                logger.info("livestream: subscribed to AM stream")

                # reset backoff after a successful connect
                delay = reconnect_initial_delay
                await _consume(ws, pool, store, symbol_map, sink)

        except asyncio.CancelledError:
            logger.info("livestream: cancelled")
            raise
        except Exception:
            logger.exception("livestream: connection failure -- reconnecting in %.1fs", delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, reconnect_max_delay)
