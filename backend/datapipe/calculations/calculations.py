from __future__ import annotations

import numpy as np
import pandas as pd

from backend.datapipe.schemas import Bar


# ---------------------------------------------------------------------------
# VWAP (session-cumulative, OHLC4 weighted)
# ---------------------------------------------------------------------------


def calculate_next_vwap(new_bar: Bar, history: list[Bar]) -> float:
    """
    Session-VWAP for one incoming bar given the prior session bars.

    Adds the new bar to the cumulative price*volume / cumulative volume
    sums. Returns 0.0 for a zero-volume session (mirrors 22).
    """
    rounding = 2
    ohlc4_new = (new_bar.open + new_bar.high + new_bar.low + new_bar.close) / 4.0
    cum_vol = float(new_bar.volume)
    cum_pv = ohlc4_new * float(new_bar.volume)
    for b in history:
        ohlc4 = (b.open + b.high + b.low + b.close) / 4.0
        cum_vol += float(b.volume)
        cum_pv += ohlc4 * float(b.volume)
    if cum_vol <= 0:
        return 0.0
    vwap = round(cum_pv / cum_vol, rounding)
    return vwap

# ---------------------------------------------------------------------------
# EMA9 (close-based)
# ---------------------------------------------------------------------------


def calculate_next_ema(new_bar: Bar, history: list[Bar], period: int = 9) -> float:
    """
    EMA9 for one incoming bar. Uses pandas' ewm on the concatenated close
    series so the result is identical to compute_ema_series on the full
    history + new_bar. Cheap enough since ``history`` is bounded per session.
    """
    rounding = 2
    closes = [b.close for b in history] + [new_bar.close]
    s = pd.Series(closes, dtype=float)
    ema = s.ewm(span=period, adjust=False).mean().iloc[-1]
    ema = round(float(ema), rounding)
    return ema

# ---------------------------------------------------------------------------
# ATR14 (daily)
# ---------------------------------------------------------------------------

def calculate_atr_series(daily_df: pd.DataFrame, span: int) -> pd.DataFrame:
    """
    Add ATR to a daily OHLC DataFrame.

    ATR uses an EWM smoother. The first row's true range is
    High - Low because there is no previous close.
    """
    rounding = 2
    df = daily_df.copy()

    prev_close = df["close"].shift(1)

    hl = df["high"] - df["low"]
    h_pc = (df["high"] - prev_close.fillna(df["high"])).abs()
    l_pc = (df["low"] - prev_close.fillna(df["low"])).abs()

    tr = np.maximum.reduce([hl.to_numpy(), h_pc.to_numpy(), l_pc.to_numpy()])

    df["atr"] = (
        pd.Series(tr, index=df.index)
        .ewm(span=span, adjust=False)
        .mean()
        .round(rounding)
    )

    return df


# ---------------------------------------------------------------------------
# RelATR = (VWAP - Close) / ATR14
# ---------------------------------------------------------------------------

def calculate_next_relatr(vwap: float, close: float, atr: float) -> float:
    rounding = 2
    relatr = ((vwap - close)/atr)
    relatr = round(relatr, rounding)
    return relatr


# ---------------------------------------------------------------------------
# RVOL cumulative -- today's cum volume / baseline cum avg volume (same ET slot)
# ---------------------------------------------------------------------------


def calculate_next_rvol_cum(new_bar_volume: int, history_volume_sum: float, baseline_slot_avg: float, baseline_history_sum: float) -> float:
    rounding = 2
    cum_vol_today = history_volume_sum + float(new_bar_volume)
    cum_baseline = baseline_history_sum + float(baseline_slot_avg or 0.0)
    if cum_baseline <= 0:
        return 0.0
    rvol_cum = round(cum_vol_today / cum_baseline, rounding)
    return rvol_cum





