"""
Downside capitulation alarm -- Uptrend Reversals filter set.

Fires the same setup the "Uptrend Reversals" stream in relatr.html
highlights: a stock in an established uptrend that is currently
extending DOWN from VWAP with real volume. Every filter here mirrors
one filter in that stream so the alarm and the dashboard stay in
lockstep -- changing a threshold in the env updates both surfaces
(the frontend has its own inputs; keep the defaults aligned).

Filters, per bar:
    * ``bar.relatr >= ALARM_MIN_RELATR``
        POSITIVE relatr means close extended DOWN from VWAP in ATR
        units. This is THE trigger -- the other three are gates.
    * ``bar.close >  sma200``            (when ALARM_REQUIRE_ABOVE_SMA200)
        Uptrend gate. Missing SMA200 (freshly added symbol) is treated
        as "trend unknown -> drop", same as the frontend does.
    * ``bar.volume     >= ALARM_MIN_VOLUME``
        Per-bar volume floor. Filters out ultra-thin bars.
    * ``cum_volume     >= ALARM_MIN_CUM_VOLUME``
        Session cumulative volume floor. Filters out symbols that
        aren't actively trading today.

Dedupe + Telegram delivery live in ``alarm_generator.generate_signal_alarm``.
"""

from __future__ import annotations

import logging

from backend.alarms.alarm_generator import generate_signal_alarm
from backend.alarms.state import AlarmState
from backend.core.config import settings
from indicators.candle_row import CandleRow

logger = logging.getLogger(__name__)


SIGNAL_NAME: str = "uptrend_reversal"  # matches the frontend's stream name


def _passes_filters(bar: CandleRow, state: AlarmState) -> bool:
    """All Uptrend Reversals gates in one predicate. Order = fastest reject first."""
    # 1) RelATR trigger -- the setup itself.
    if bar.relatr is None or float(bar.relatr) < settings.ALARM_MIN_RELATR:
        return False

    # 2) Per-bar volume floor.
    if bar.volume is None or float(bar.volume) < settings.ALARM_MIN_VOLUME:
        return False

    # 3) Session cumulative volume floor.
    if state.cum_volume(bar) < settings.ALARM_MIN_CUM_VOLUME:
        return False

    # 4) Uptrend gate. Missing SMA200 = "trend unknown -> drop", matching
    #    the frontend's ``ref === null || ref === undefined -> return false``.
    if settings.ALARM_REQUIRE_ABOVE_SMA200:
        sma = state.sma200(bar)
        if sma is None:
            return False
        if bar.close is None or float(bar.close) <= sma:
            return False

    return True


async def run(bar: CandleRow, state: AlarmState) -> None:
    """Bar-level entry point invoked by the alarms dispatcher."""
    if not _passes_filters(bar, state):
        return

    logger.debug(
        "capitulation setup %s @ %s | close=%.4f relatr=%.2f vol=%s cumvol=%s sma200=%s",
        bar.symbol, bar.ts, float(bar.close), float(bar.relatr),
        bar.volume, state.cum_volume(bar), state.sma200(bar),
    )
    await generate_signal_alarm(bar=bar, signal_name=SIGNAL_NAME)
