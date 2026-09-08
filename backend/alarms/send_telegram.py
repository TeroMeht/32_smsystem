"""
Telegram HTTP client for the alarms layer.

Ported from 22_WatchlistStreamer/src/alarms/send_telegram.py -- same
shape (async httpx, short timeout, safe swallow on failure) so the
alarm generator body reads identically across projects. Only the
message formatter is adapted to 32's CandleRow (single tz-aware
``ts`` field instead of split ``date``/``time``).
"""

from __future__ import annotations

import logging

import httpx

from backend.core.config import settings

logger = logging.getLogger(__name__)

# Default timeout for Telegram HTTP calls (seconds). Short on purpose:
# a hung Telegram call must never wedge the bar-processing loop.
_TELEGRAM_TIMEOUT = 10.0


def format_telegram_message(symbol: str, ts, alarm_message: str) -> str:
    """
    Build the human-readable Telegram body. ``ts`` is a tz-aware
    datetime (bar timestamp); we format it in Helsinki local time so
    the line matches the log the user is watching.
    """
    from backend.datapipe.time_utils import to_helsinki

    local = to_helsinki(ts) if ts is not None else None
    time_str = local.strftime("%Y-%m-%d %H:%M") if local is not None else "?"

    return (
        "\U0001F6A8 Alarm triggered \U0001F6A8\n"
        f"Symbol: {symbol}\n"
        f"Time: {time_str}\n"
        f"Message: {alarm_message}"
    )


async def send_telegram_message(symbol: str, ts, alarm_message: str) -> dict:
    """
    Fire one sendMessage POST at the Telegram Bot API. Never raises --
    a Telegram outage must not tear down the bar loop. Returns the
    parsed JSON response (or an ``{"ok": False, "error": ...}`` shape
    on transport failure) so callers can log the outcome.
    """
    message = format_telegram_message(symbol, ts, alarm_message)

    bot_token = settings.TELEGRAM_BOT_TOKEN
    chat_id = settings.TELEGRAM_CHAT_ID

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
    }

    try:
        async with httpx.AsyncClient(timeout=_TELEGRAM_TIMEOUT) as client:
            response = await client.post(url, data=payload)
        result = response.json()
        if result.get("ok"):
            logger.info("Telegram sent: %s | %s", symbol, alarm_message)
        else:
            logger.warning("Telegram API error for %s: %s", symbol, result)
        return result
    except Exception as e:
        logger.error("Telegram send failed for %s: %s", symbol, e)
        return {"ok": False, "error": str(e)}
