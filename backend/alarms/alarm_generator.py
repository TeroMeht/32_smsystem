"""
Alarm orchestrator: dedupe -> log -> chart + Telegram.

Mirrors 22_WatchlistStreamer/src/alarms/alarm_generator.py in shape,
including the (there commented-out) chart path: load today's bars,
``plot_intraday_chart`` -> PNG via kaleido -> ``send_telegram_picture``.
Here it is live, with the alarm text as the photo caption.

Delivery runs as a background asyncio task. Chart rendering takes a
second or two and the sink is awaited inline by ``process_bar`` -- doing
it inline would stall the WS consumer for every alarm. Dedupe is
recorded synchronously before the task is scheduled, so a burst of
qualifying bars still yields exactly one alarm per cooldown window.

If the chart can't be built (no bars, kaleido/Chrome missing, render
error) the alarm falls back to the plain text message -- an alarm is
never dropped because of the picture.

Called by ``backend.alarms.strategies.*`` after their detection logic
has fired.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

import asyncpg

from indicators.candle_row import CandleRow

from backend.alarms import alarm_config
from backend.alarms.dedupe import record_fire, should_fire
from backend.alarms.send_telegram import (
    format_telegram_message,
    send_telegram_message,
    send_telegram_picture,
)

logger = logging.getLogger(__name__)

# Strong refs to in-flight delivery tasks (asyncio only keeps weak refs).
_pending: set[asyncio.Task] = set()


async def build_chart_png(
    pool: asyncpg.Pool, symbol: str, alarm_ts=None
) -> Optional[bytes]:
    """Today's livestream bars for ``symbol`` -> PNG bytes, or None on any failure."""
    try:
        # Imported lazily so a missing plotly/kaleido install only
        # disables the picture, not the whole alarms module.
        from backend.alarms.alarm_plotchart import (
            bars_to_dataframe,
            plot_intraday_chart,
            render_png,
        )
        from backend.database.readers import load_livestream_bars_for_symbol

        rows = await load_livestream_bars_for_symbol(pool, symbol.upper())
        if not rows:
            logger.warning("chart skipped for %s: no livestream bars", symbol)
            return None
        df = bars_to_dataframe(rows)
        fig = plot_intraday_chart(df, symbol=symbol.upper(), alarm_ts=alarm_ts)
        return await asyncio.to_thread(render_png, fig)
    except Exception:
        logger.exception("chart render failed for %s -- sending text only", symbol)
        return None


async def deliver_alarm(
    *,
    symbol: str,
    ts,
    alarm_message: str,
    pool: Optional[asyncpg.Pool],
    send_chart: bool,
) -> dict:
    """Send one alarm: chart photo with caption, else text message."""
    if send_chart and pool is not None:
        png = await build_chart_png(pool, symbol, alarm_ts=ts)
        if png is not None:
            caption = format_telegram_message(symbol, ts, alarm_message)
            result = await send_telegram_picture(png, caption=caption)
            if result.get("ok"):
                return result
            logger.warning("photo send failed for %s -- falling back to text", symbol)

    return await send_telegram_message(symbol=symbol, ts=ts, alarm_message=alarm_message)


async def _deliver_guarded(**kwargs) -> None:
    try:
        await deliver_alarm(**kwargs)
    except Exception:
        logger.exception("alarm delivery failed for %s", kwargs.get("symbol"))


async def generate_signal_alarm(
    bar: CandleRow,
    signal_name: str,
    pool: Optional[asyncpg.Pool] = None,
    details: str = "",
) -> None:
    """
    Dedupe + dispatch pipeline for one strategy hit.

    Guarded end-to-end so an alarm-side failure (Telegram outage,
    bad config, etc.) can never propagate up into the bar loop.
    """
    try:
        cfg = alarm_config.get()
        if not should_fire(
            symbol=bar.symbol,
            strategy_name=signal_name,
            bar_ts=bar.ts,
            cooldown_minutes=cfg.cooldown_minutes,
        ):
            return

        # Record BEFORE the network call so a slow Telegram response
        # can't let a second bar sneak in and double-fire.
        record_fire(bar.symbol, signal_name, bar.ts)

        logger.info(
            "ALARM %s | %s | close=%.4f relatr=%s rvol=%s",
            signal_name,
            bar.symbol,
            float(bar.close),
            f"{bar.relatr:.2f}" if bar.relatr is not None else "None",
            f"{bar.rvol:.2f}" if bar.rvol is not None else "None",
        )

        message = f"{signal_name}\n{details}" if details else signal_name
        task = asyncio.create_task(
            _deliver_guarded(
                symbol=bar.symbol,
                ts=bar.ts,
                alarm_message=message,
                pool=pool,
                send_chart=cfg.send_chart,
            ),
            name=f"alarm-{bar.symbol}-{signal_name}",
        )
        _pending.add(task)
        task.add_done_callback(_pending.discard)
    except Exception:
        logger.exception(
            "generate_signal_alarm failed for %s / %s", bar.symbol, signal_name
        )


async def drain_pending(timeout: float | None = None) -> None:
    """Await all in-flight deliveries -- tests, smoke script, shutdown."""
    async def _drain() -> None:
        while _pending:
            await asyncio.gather(*list(_pending), return_exceptions=True)

    try:
        await asyncio.wait_for(_drain(), timeout)
    except asyncio.TimeoutError:
        logger.warning("%d alarm deliveries still pending at shutdown", len(_pending))
