"""
Telegram alarm controls for the dashboard.

    GET  /api/alarms/config         current switch + thresholds + telegram status
    POST /api/alarms/config         partial update (switch, Uptrend Reversals
                                    filter values, cooldown, chart on/off,
                                    Reversal Shorts filters under "shorts",
                                    Elevated RVOL filter under "elevated_rvol")
    POST /api/alarms/config/reset/{scope}
                                    restore default filters for one table
                                    (scope = uptrend | shorts | elevated_rvol)
    POST   /api/alarms/blocked/{symbol}  block a ticker until midnight
    DELETE /api/alarms/blocked/{symbol}  unblock it
                                    (blocked = hidden in both tables AND no alarms)
    POST /api/alarms/test/{symbol}  send a test alarm (chart + text) for one
                                    symbol right now -- bypasses filters,
                                    dedupe and the ON/OFF switch

No SQL here -- the test endpoint goes through ``alarm_generator``,
which reads bars via ``backend.database.readers``.
"""

from __future__ import annotations

from datetime import datetime, timezone

import asyncpg
from fastapi import APIRouter, Depends, HTTPException

from backend.alarms import alarm_config
from backend.alarms.alarm_config import AlarmConfigUpdate
from backend.alarms.alarm_generator import deliver_alarm
from backend.alarms.send_telegram import is_configured
from backend.dependencies import get_pool

router = APIRouter(prefix="/api/alarms", tags=["alarms"])


def _payload() -> dict:
    return {
        **alarm_config.get().model_dump(),
        "blocked": alarm_config.blocked_today(),   # expired days filtered out
        "telegram_configured": is_configured(),
    }


@router.get("/config")
async def get_config():
    return _payload()


@router.post("/config")
async def update_config(patch: AlarmConfigUpdate):
    if patch.enabled and not is_configured():
        raise HTTPException(
            status_code=400,
            detail="TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID missing in 32_smsystem.env",
        )
    alarm_config.update(patch)
    return _payload()


@router.post("/config/reset/{scope}")
async def reset_config(scope: str):
    try:
        alarm_config.reset(scope)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return _payload()


@router.post("/blocked/{symbol}")
async def block_symbol(symbol: str):
    alarm_config.block(symbol)
    return _payload()


@router.delete("/blocked/{symbol}")
async def unblock_symbol(symbol: str):
    alarm_config.unblock(symbol)
    return _payload()


@router.post("/test/{symbol}")
async def send_test(symbol: str, pool: asyncpg.Pool = Depends(get_pool)):
    if not is_configured():
        raise HTTPException(
            status_code=400,
            detail="TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID missing in 32_smsystem.env",
        )
    result = await deliver_alarm(
        symbol=symbol.upper(),
        ts=datetime.now(timezone.utc),
        alarm_message="TEST alarm (manual, from dashboard)",
        pool=pool,
        send_chart=True,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=502, detail=f"Telegram: {result}")
    has_photo = bool((result.get("result") or {}).get("photo"))
    return {"ok": True, "chart": has_photo}
