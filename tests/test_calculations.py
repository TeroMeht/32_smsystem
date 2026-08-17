"""
Unit tests for backend.datapipe.calculations.

These are the heart of the "testable functions for data feed" goal: every
indicator must be pure, deterministic, and identical between the bulk
(compute_*_series) and incremental (next_*) paths.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from backend.datapipe.calculations.calculations import (
    bars_to_frame,
    compute_atr_series,
    compute_ema_series,
    compute_vwap_series,
    enrich_bar,
    latest_atr,
    next_ema,
    next_relatr,
    next_rvol_cum,
    next_vwap,
)
from backend.datapipe.schemas import Bar


def _mk_bar(i: int, o: float, h: float, l: float, c: float, v: int) -> Bar:
    return Bar(
        symbol="TEST",
        symbolid=1,
        ts=datetime(2026, 8, 7, 13, 30, tzinfo=timezone.utc) + timedelta(minutes=i),
        open=o, high=h, low=l, close=c, volume=v,
    )


@pytest.fixture
def session_bars() -> list[Bar]:
    # 5 bars, monotonic close, varying volume
    specs = [
        (100.0, 100.5, 99.8, 100.3, 1000),
        (100.3, 100.8, 100.1, 100.6, 1500),
        (100.6, 101.0, 100.4, 100.9, 2000),
        (100.9, 101.3, 100.7, 101.1, 800),
        (101.1, 101.5, 100.9, 101.4, 1200),
    ]
    return [_mk_bar(i, *s) for i, s in enumerate(specs)]




def test_vwap_zero_volume_returns_zero():
    bar = _mk_bar(0, 10, 10, 10, 10, 0)
    assert next_vwap(bar, []) == 0.0


# ---------------------------------------------------------------------------
# EMA: bulk == incremental (span=9, adjust=False)
# ---------------------------------------------------------------------------

def test_ema_bulk_matches_incremental(session_bars):
    df = bars_to_frame(session_bars)
    bulk = compute_ema_series(df, period=9).tolist()

    incremental = [next_ema(bar, session_bars[:i]) for i, bar in enumerate(session_bars)]

    assert bulk == pytest.approx(incremental, abs=1e-4)


# ---------------------------------------------------------------------------
# ATR: first row collapses to (High-Low); subsequent uses prev_close
# ---------------------------------------------------------------------------

def test_atr_first_row_is_high_minus_low():
    daily = pd.DataFrame([
        {"Open": 100, "High": 105, "Low": 99, "Close": 104, "Volume": 1_000_000},
    ])
    atr = compute_atr_series(daily).iloc[0]
    # First row: TR collapses to H-L = 6, EMA span=14 first value = 6
    assert atr == pytest.approx(6.0, abs=1e-4)





# ---------------------------------------------------------------------------
# RelATR: safe against zero / None ATR
# ---------------------------------------------------------------------------

def test_relatr_zero_atr_returns_none():
    assert next_relatr(vwap=100.0, close=99.0, atr=0) is None
    assert next_relatr(vwap=100.0, close=99.0, atr=None) is None


def test_relatr_positive_when_vwap_above_close():
    v = next_relatr(vwap=100.0, close=98.0, atr=2.0)
    assert v == pytest.approx(1.0, abs=1e-4)


# ---------------------------------------------------------------------------
# RVOL cumulative: baseline is per-bar; denominator is a running sum of
# per-bar baselines through the current slot. Same shape as 22.
# ---------------------------------------------------------------------------

def test_rvol_zero_baseline_returns_zero():
    # Start of session, no baseline accumulated yet and current slot is 0.
    # 22 returns 0.0 in that case (not None) so downstream can safely divide.
    assert next_rvol_cum(1000, 5000.0, 0.0, 0.0) == 0.0


def test_rvol_ratio_matches_manual_sum():
    # today cum = 5000 (history) + 1000 (this bar) = 6000
    # baseline denominator = 2000 (prior slots summed) + 400 (this slot) = 2400
    # ratio = 6000 / 2400 = 2.5
    r = next_rvol_cum(1000, 5000.0, 400.0, 2000.0)
    assert r == pytest.approx(2.5, abs=1e-4)


def test_rvol_stays_defined_when_current_slot_has_no_baseline():
    # Current ET slot has no baseline data (per-bar avg = 0), but the
    # running sum from prior bars is > 0. RVOL should stay defined.
    # today cum = 5000 + 1000 = 6000; baseline = 2000 + 0 = 2000; r = 3.0
    r = next_rvol_cum(1000, 5000.0, 0.0, 2000.0)
    assert r == pytest.approx(3.0, abs=1e-4)


def test_rvol_none_baseline_treated_as_zero():
    r = next_rvol_cum(1000, 5000.0, None, 2000.0)  # type: ignore[arg-type]
    # baseline sum = 2000, denom = 2000, r = 6000/2000 = 3.0
    assert r == pytest.approx(3.0, abs=1e-4)


# ---------------------------------------------------------------------------
# enrich_bar: end-to-end, all four indicators land on the bar
# ---------------------------------------------------------------------------

def test_enrich_bar_populates_all_indicators(session_bars):
    new_bar = _mk_bar(5, 101.4, 101.7, 101.2, 101.6, 900)
    out = enrich_bar(
        new_bar,
        history=session_bars,
        atr=1.5,
        baseline_slot_avg=1000.0,       # per-bar baseline for THIS slot
        baseline_history_sum=4000.0,    # sum of per-bar baselines for prior slots
    )
    assert out.vwap is not None
    assert out.ema9 is not None
    assert out.relatr is not None
    assert out.rvol_cum is not None
    # RelATR sign check: (VWAP - close) / atr
    assert out.relatr == pytest.approx(round((out.vwap - new_bar.close) / 1.5, 4))
    # RVOL: today's cum vol / (prior baselines + this slot's baseline)
    expected_today_cum = sum(b.volume for b in session_bars) + new_bar.volume
    expected_baseline = 4000.0 + 1000.0
    assert out.rvol_cum == pytest.approx(round(expected_today_cum / expected_baseline, 4))
