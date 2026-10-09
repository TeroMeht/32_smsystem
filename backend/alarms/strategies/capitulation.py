"""
Uptrend Reversals alarm -- the only setup that goes to Telegram.

Fires for the same rows the "Uptrend Reversals" stream in relatr.html
shows: a stock in an established uptrend that is currently extending
DOWN from VWAP with real volume and a meaningful down day.

Thresholds come from ``alarm_config`` -- the dashboard pushes its
Uptrend Reversals filter inputs there, so the alarm and the table use
the same numbers. Nothing is evaluated unless the Telegram switch in
the UI is ON, and symbols blocked from the dashboard ("x") are skipped.

Filters, per bar (same semantics as the frontend filter loop):
    * ``bar.relatr  >= min_relatr``      THE trigger (positive relatr =
                                         close extended DOWN from VWAP)
    * ``bar.volume  >= min_volume``      per-bar volume floor
    * ``cum_volume  >= min_cum_volume``  session cumulative volume floor
    * ``bar.rvol    >= min_rvol``        (None fails, as in the UI)
    * ``chg_pct     <= max_chg_pct``     vs previous daily close (None fails)
    * ``bar.close   >  sma200``          when above_sma200 is on; missing
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

logger = logging.getLogger(__name__)


SIGNAL_NAME: str = "uptrend_reversal"  # matches the frontend's stream name


def _chg_pct(bar: CandleRow, state: AlarmState) -> Optional[float]:
    """Same formula as readers._chg_pct (last vs previous session close)."""
    ref = state.prev_close(bar)
    if bar.close is None or ref is None or ref == 0:
        return None
    return round((float(bar.close) - ref) / ref * 100.0, 2)


def _passes_filters(bar: CandleRow, state: AlarmState, cfg=None) -> bool:
    """All Uptrend Reversals gates in one predicate. Order = fastest reject first."""
    cfg = cfg or alarm_config.get()

    # 1) RelATR trigger -- the setup itself.
    if bar.relatr is None or float(bar.relatr) < cfg.min_relatr:
        return False

    # 2) Per-bar volume floor.
    if bar.volume is None or float(bar.volume) < cfg.min_volume:
        return False

    # 3) Session cumulative volume floor.
    if state.cum_volume(bar) < cfg.min_cum_volume:
        return False

    # 4) RVOL floor -- None fails, matching the UI's ``number`` filter.
    if bar.rvol is None or float(bar.rvol) < cfg.min_rvol:
        return False

    # 5) Chg% ceiling -- None fails, matching ``maxNumberStrict``.
    chg = _chg_pct(bar, state)
    if chg is None or chg > cfg.max_chg_pct:
        return False

    # 6) Uptrend gate. Missing SMA200 = "trend unknown -> drop".
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
    return (
        f"Close: {f(bar.close)}  Chg: {f(chg)}%\n"
        f"RelATR: {f(bar.relatr)}  RVOL: {f(bar.rvol)}\n"
        f"Vol: {int(bar.volume or 0):,}  CumVol: {state.cum_volume(bar):,}\n"
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
        "uptrend reversal %s @ %s | close=%.4f relatr=%.2f vol=%s cumvol=%s sma200=%s",
        bar.symbol, bar.ts, float(bar.close), float(bar.relatr),
        bar.volume, state.cum_volume(bar), state.sma200(bar),
    )
    await generate_signal_alarm(
        bar=bar,
        signal_name=SIGNAL_NAME,
        pool=state.pool,
        details=_details(bar, state),
    )
