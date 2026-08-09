"""
Indicator calculations for 1-min bars.

Ported from ``22_WatchlistStreamer/src/common/calculate.py`` with the same
semantics, but expressed as pure functions over the canonical Bar1m /
history-list shape rather than pandas rows. This keeps them trivially unit
testable -- every function here has zero I/O, zero global state.

Two flavors per indicator:

  * ``compute_*_series`` -- vectorized bulk compute over a list of prior
    bars. Used by the historian for warmup and by any test that wants to
    replay a full session at once.
  * ``next_*``            -- single-bar step given the history so far. Used
    by the live path to enrich each incoming bar cheaply.

Both flavors must produce identical values when fed identical inputs -- the
tests in tests/test_calculations.py enforce this.
"""

from __future__ import annotations

from typing import Iterable, Optional

import numpy as np
import pandas as pd

from backend.datapipe.schemas import Bar1m


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _round(v: Optional[float], places: int) -> Optional[float]:
    return None if v is None else round(float(v), places)


def bars_to_frame(bars: Iterable[Bar1m]) -> pd.DataFrame:
    """
    Build a pandas frame from Bar1m objects with the columns downstream
    calculators expect. Column names match 22's convention (Open/High/Low
    /Close/Volume) so the compute_* functions can share logic.
    """
    rows = [
        {
            "ts": b.ts,
            "Symbol": b.symbol,
            "Open": b.open,
            "High": b.high,
            "Low": b.low,
            "Close": b.close,
            "Volume": b.volume,
        }
        for b in bars
    ]
    if not rows:
        return pd.DataFrame(
            columns=["ts", "Symbol", "Open", "High", "Low", "Close", "Volume"]
        )
    df = pd.DataFrame(rows).sort_values("ts").reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# VWAP (session-cumulative, OHLC4 weighted)
# ---------------------------------------------------------------------------


def compute_vwap_series(df: pd.DataFrame) -> pd.Series:
    """
    Cumulative session VWAP using OHLC4 as the price proxy. Matches 22's
    ``calculate_vwap``. Assumes ``df`` already contains only one session
    (caller slices by date before calling).
    """
    ohlc4 = (df["Open"] + df["High"] + df["Low"] + df["Close"]) / 4
    cum_vol = df["Volume"].cumsum()
    cum_pv = (ohlc4 * df["Volume"]).cumsum()
    with np.errstate(divide="ignore", invalid="ignore"):
        vwap = (cum_pv / cum_vol).fillna(0).round(2)
    return vwap


def next_vwap(new_bar: Bar1m, history: list[Bar1m]) -> float:
    """
    Session-VWAP for one incoming bar given the prior session bars.

    Adds the new bar to the cumulative price*volume / cumulative volume
    sums. Returns 0.0 for a zero-volume session (mirrors 22).
    """
    ohlc4_new = (new_bar.open + new_bar.high + new_bar.low + new_bar.close) / 4.0
    cum_vol = float(new_bar.volume)
    cum_pv = ohlc4_new * float(new_bar.volume)
    for b in history:
        ohlc4 = (b.open + b.high + b.low + b.close) / 4.0
        cum_vol += float(b.volume)
        cum_pv += ohlc4 * float(b.volume)
    if cum_vol <= 0:
        return 0.0
    return round(cum_pv / cum_vol, 2)


# ---------------------------------------------------------------------------
# EMA9 (close-based)
# ---------------------------------------------------------------------------


def compute_ema_series(df: pd.DataFrame, period: int = 9) -> pd.Series:
    """EMA of Close, adjust=False -- matches 22's ``calculate_ema``."""
    return df["Close"].ewm(span=period, adjust=False).mean().round(2)


def next_ema(new_bar: Bar1m, history: list[Bar1m], period: int = 9) -> float:
    """
    EMA9 for one incoming bar. Uses pandas' ewm on the concatenated close
    series so the result is identical to compute_ema_series on the full
    history + new_bar. Cheap enough since ``history`` is bounded per session.
    """
    closes = [b.close for b in history] + [new_bar.close]
    s = pd.Series(closes, dtype=float)
    ema = s.ewm(span=period, adjust=False).mean().iloc[-1]
    return round(float(ema), 2)


# ---------------------------------------------------------------------------
# ATR14 (daily)
# ---------------------------------------------------------------------------


def compute_atr_series(daily_df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    14-day ATR on the daily frame. Matches 22's ``calculate_14day_atr_df``:
    first-row true range collapses to (High - Low) because there's no
    previous close yet.
    """
    df = daily_df.copy()
    prev_close = df["Close"].shift(1)
    hl = df["High"] - df["Low"]
    h_pc = (df["High"] - prev_close.fillna(df["High"])).abs()
    l_pc = (df["Low"] - prev_close.fillna(df["Low"])).abs()
    tr = np.maximum.reduce([hl.values, h_pc.values, l_pc.values])
    atr = pd.Series(tr).ewm(span=period, adjust=False).mean().round(4)
    return atr


def latest_atr(daily_df: pd.DataFrame, period: int = 14) -> Optional[float]:
    """Convenience: latest ATR value from a symbol's daily frame."""
    if daily_df.empty:
        return None
    return float(compute_atr_series(daily_df, period).iloc[-1])


# ---------------------------------------------------------------------------
# RelATR = (VWAP - Close) / ATR14
# ---------------------------------------------------------------------------


def next_relatr(vwap: float, close: float, atr: Optional[float]) -> Optional[float]:
    """Undefined when ATR is 0 or missing (division by zero would blow up)."""
    if atr is None or atr == 0:
        return None
    return round((vwap - close) / atr, 4)


# ---------------------------------------------------------------------------
# RVOL cumulative -- today's cum volume / baseline cum avg volume (same ET slot)
# ---------------------------------------------------------------------------


def next_rvol_cum(
    new_bar_volume: int,
    history_volume_sum: float,
    baseline_slot_avg: float,
    baseline_history_sum: float,
) -> float:
    """
    Cumulative RVOL:

        rvol = (today's cum volume through this bar)
             / (sum of per-bar baselines through this slot)

    ``rvol_baseline.avg_volume`` is a PER-BAR average across the last N
    trading sessions. The cumulative denominator is built on the fly by
    summing those per-bar averages as the session progresses -- exactly
    like 22 did. Consequences:

      * A missing slot (no baseline row for that ET minute) contributes
        0 to the running sum, so the denominator stays flat for that
        minute but never drops. RVOL stays defined from that bar forward.
      * At the very start of a session, before any baseline has been
        accumulated, the denominator can be 0 -- in that case we return
        0.0 (matching 22) rather than None. Once the first slot with
        baseline data lands, RVOL is real and stays real.

    Callers pass the running sums so we don't recompute them per bar.
    """
    cum_vol_today = history_volume_sum + float(new_bar_volume)
    cum_baseline = baseline_history_sum + float(baseline_slot_avg or 0.0)
    if cum_baseline <= 0:
        return 0.0
    return round(cum_vol_today / cum_baseline, 4)


# ---------------------------------------------------------------------------
# High-level enrichment -- glues the four indicators onto a Bar1m in one call
# ---------------------------------------------------------------------------


def enrich_bar(
    new_bar: Bar1m,
    history: list[Bar1m],
    atr: Optional[float],
    baseline_slot_avg: float = 0.0,
    baseline_history_sum: float = 0.0,
) -> Bar1m:
    """
    Populate all four indicator slots on ``new_bar`` and return it.

    ``history`` is the list of session bars strictly BEFORE ``new_bar``, in
    chronological order. Callers (livestream / replay) maintain a small
    in-memory deque per symbol so this stays cheap.

    ``baseline_slot_avg`` is the per-bar avg for THIS bar's ET slot.
    ``baseline_history_sum`` is the running sum of per-bar baselines from
    every prior bar this session -- so together they form the cumulative
    denominator for RVOL. See ``next_rvol_cum`` for semantics.
    """
    vwap = next_vwap(new_bar, history)
    new_bar.vwap = vwap
    new_bar.ema9 = next_ema(new_bar, history)
    new_bar.relatr = next_relatr(vwap, new_bar.close, atr)
    hist_vol_sum = float(sum(b.volume for b in history))
    new_bar.rvol_cum = next_rvol_cum(
        new_bar.volume, hist_vol_sum, baseline_slot_avg, baseline_history_sum,
    )
    return new_bar
