"""
Alarm orchestrator: dedupe -> log -> Telegram.

Mirrors 22_WatchlistStreamer/src/alarms/alarm_generator.py in shape.
Trimmed for 32's current scope:

    * No local ``alarms`` DB table yet -- if/when the frontend needs
      to render an alarm history, add ``insert_alarm`` here alongside
      the Telegram call and expose a router that reads it.
    * No fastapi POST fan-out -- 32's dashboard reads the livestream
      table directly; there is no separate portfolio-manager backend
      to notify.

Called by ``backend.alarms.strategies.*`` after their detection logic
has fired. Keeps the strategy modules focused on "did the setup
trigger?" and this module focused on "given that it did, do the
side effects exactly once per cooldown window".
"""

from __future__ import annotations

import logging

from indicators.candle_row import CandleRow

from backend.alarms.dedupe import record_fire, should_fire
from backend.alarms.send_telegram import send_telegram_message
from backend.core.config import settings

logger = logging.getLogger(__name__)


async def generate_signal_alarm(bar: CandleRow, signal_name: str) -> None:
    """
    Dedupe + dispatch pipeline for one strategy hit.

    Guarded end-to-end so an alarm-side failure (Telegram outage,
    bad config, etc.) can never propagate up into the bar loop.
    """
    try:
        if not should_fire(
            symbol=bar.symbol,
            strategy_name=signal_name,
            bar_ts=bar.ts,
            cooldown_minutes=settings.ALARM_COOLDOWN_MINUTES,
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

        await send_telegram_message(
            symbol=bar.symbol,
            ts=bar.ts,
            alarm_message=signal_name,
        )
    except Exception:
        logger.exception(
            "generate_signal_alarm failed for %s / %s", bar.symbol, signal_name
        )
