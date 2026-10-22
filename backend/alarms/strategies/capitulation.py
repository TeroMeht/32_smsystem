"""
Uptrend Reversals alarm -- the only setup that goes to Telegram.

Fires for the same rows the "Uptrend Reversals" stream in relatr.html
shows: a stock (optionally in an established uptrend) that is currently
extending DOWN from VWAP, fast, in a name with real dollar volume.

Thresholds come from ``alarm_config`` -- the dashboard pushes its
Uptrend Reversals filter inputs there, so the alarm and the table use
the same numbers. Nothing is evaluated unless the Telegram switch in
the UI is ON, and symbols blocked from the dashboard ("x") are skipped.

Filters, per bar (same semantics as the frontend filter loop):
    * ``bar.relatr        >= min_relatr``  THE trigger (positive relatr =
                                           close extended DOWN from VWAP)
    * ``cum $ volume      >= min_dollar_volume_m`` x $1M   (0 = off)
                                           session SUM(volume x (h+l+c)/3)
    * ``speed             >= min_speed``   EMA9-anchored %/bar (0 = off);
                                           no active cross-down = no speed
                                           = fails while the gate is on
    * ``bar.rvol          >= min_rvol``    (None fails, as in the UI)
    * ``bar.close         >  sma200``      when above_sma200 is on; missing
                                           SMA200 = "trend unknown -> drop"

Dedupe + chart + Telegram delivery live in ``alarm_generator``.
"""

from __future__ import annotations

import logging
from typing import Optional

from backend.alarms import alarm_config
from backend.alarms.alarm_generator import generate_signal_alarm
from backend.alarms.state import AlarmState
from indicators.candle_row import CandleRow
from indicators.pct_change import next_pct_change

logger = logging.getLogger(__name__)


SIGNAL_NAME: str = "uptrend_reversal"  # matches the frontend's stream name


def _chg_pct(bar: CandleRow, state: AlarmState) -> Optional[float]:
    """Chg% -- last vs previous session close (same as the dashboard)."""
    return next_pct_change(bar.close, state.prev_close(bar))


def _passes_filters(bar: CandleRow, state: AlarmState, cfg=None) -> bool:
    """All Uptrend Reversals gates in one predicate. Order = fastest reject first."""
    cfg = cfg or alarm_config.get()

    # 1) RelATR trigger -- the setup itself.
    if bar.relatr is None or float(bar.relatr) < cfg.min_relatr:
        return False

    # 2) Session cumulative $ volume floor (0 = off).
    if cfg.min_dollar_volume_m > 0:
        if state.cum_dollar_volume(bar) < cfg.min_dollar_volume_m * 1_000_000:
            return False

    # 3) Speed of the move (0 = off). No anchor -> no speed -> fails.
    if cfg.min_speed > 0:
        sp = state.speed(bar).speed
        if sp is None or sp < cfg.min_speed:
            return False

    # 4) RVOL floor -- None fails, matching the UI's ``number`` filter.
    if bar.rvol is None or float(bar.rvol) < cfg.min_rvol:
        return False

    # 5) Uptrend gate. Missing SMA200 = "trend unknown -> drop".
    if cfg.above_sma200:
        sma = state.sma200(bar)
        if sma is None:
            return False
        if bar.close is None or float(bar.close) <= sma:
            return False

    return True


def _details(bar: CandleRow, state: AlarmState) -> str:
    def f(v, d=2):
        return "-" if v is None else f"{float(v):.{d}f}"

    chg = _chg_pct(bar, state)
    sp = state.speed(bar)
    return (
        f"Close: {f(bar.close)}  Chg: {f(chg)}%\n"
        f"RelATR: {f(bar.relatr)}  RVOL: {f(bar.rvol)}\n"
        f"Speed: {f(sp.speed, 3)}%/bar ({sp.bars if sp.bars is not None else '-'} bars)  "
        f"$Vol: {state.cum_dollar_volume(bar) / 1e6:,.0f}M\n"
        f"SMA200: {f(state.sma200(bar))}"
    )


async def run(bar: CandleRow, state: AlarmState) -> None:
    """Bar-level entry point invoked by the alarms dispatcher."""
    cfg = alarm_config.get()
    if not cfg.enabled:
        return
    if alarm_config.is_blocked(bar.symbol):
        return
    if not _passes_filters(bar, state, cfg):
        return

    logger.debug(
        "uptrend reversal %s @ %s | close=%.4f relatr=%.2f speed=%s dvol=%.0f sma200=%s",
        bar.symbol, bar.ts, float(bar.close), float(bar.relatr),
        state.speed(bar).speed, state.cum_dollar_volume(bar), state.sma200(bar),
    )
    await generate_signal_alarm(
        bar=bar,
        signal_name=SIGNAL_NAME,
        pool=state.pool,
        details=_details(bar, state),
    )
