"""
Telegram HTTP client for the alarms layer.

Ported from 22_WatchlistStreamer/src/alarms/send_telegram.py -- same
shape (async httpx, short timeout, safe swallow on failure) so the
alarm generator body reads identically across projects. Only the
message formatter is adapted to 32's CandleRow (single tz-aware
``ts`` field instead of split ``date``/``time``).

``send_telegram_picture`` is the 22 sendPhoto call; the alarm
generator uses it to deliver the intraday chart with the alarm text as
the photo caption (one Telegram message instead of two).
"""

from __future__ import annotations

import logging

import httpx

from backend.core.config import settings

logger = logging.getLogger(__name__)

# Default timeout for Telegram HTTP calls (seconds). Short on purpose:
# a hung Telegram call must never wedge the bar-processing loop.
_TELEGRAM_TIMEOUT = 10.0


# Telegram's sendPhoto caption hard limit.
_CAPTION_MAX = 1024


def is_configured() -> bool:
    """True when both bot token and chat id are present in the env file."""
    return bool(settings.TELEGRAM_BOT_TOKEN) and bool(settings.TELEGRAM_CHAT_ID)


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


async def send_telegram_picture(image_bytes: bytes, caption: str) -> dict:
    """
    sendPhoto with an in-memory PNG. Never raises; returns the parsed
    Telegram response or an ``{"ok": False, "error": ...}`` shape.
    """
    if not is_configured():
        logger.warning("Telegram not configured -- picture not sent")
        return {"ok": False, "error": "telegram not configured"}

    url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendPhoto"
    data = {"chat_id": settings.TELEGRAM_CHAT_ID, "caption": caption[:_CAPTION_MAX]}
    files = {"photo": ("chart.png", image_bytes, "image/png")}

    try:
        # Longer timeout than sendMessage: the upload is a few hundred KB.
        async with httpx.AsyncClient(timeout=_TELEGRAM_TIMEOUT * 3) as client:
            response = await client.post(url, data=data, files=files)
        result = response.json()
        if result.get("ok"):
            logger.info("Telegram picture sent: %s", caption.splitlines()[1:2])
        else:
            logger.warning("Telegram sendPhoto API error: %s", result)
        return result
    except Exception as e:
        logger.error("Telegram sendPhoto failed: %s", e)
        return {"ok": False, "error": str(e)}


async def send_telegram_message(symbol: str, ts, alarm_message: str) -> dict:
    """
    Fire one sendMessage POST at the Telegram Bot API. Never raises --
    a Telegram outage must not tear down the bar loop. Returns the
    parsed JSON response (or an ``{"ok": False, "error": ...}`` shape
    on transport failure) so callers can log the outcome.
    """
    message = format_telegram_message(symbol, ts, alarm_message)

    if not is_configured():
        logger.warning("Telegram not configured -- message for %s not sent", symbol)
        return {"ok": False, "error": "telegram not configured"}

    bot_token = settings.TELEGRAM_BOT_TOKEN
    chat_id = settings.TELEGRAM_CHAT_ID

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
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
