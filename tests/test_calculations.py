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

from backend.datapipe.calculations import (
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
from backend.datapipe.schemas import Bar1m


def _mk_bar(i: int, o: float, h: float, l: float, c: float, v: int) -> Bar1m:
    return Bar1m(
        symbol="TEST",
        symbolid=1,
        ts=datetime(2026, 8, 7, 13, 30, tzinfo=timezone.utc) + timedelta(minutes=i),
        open=o, high=h, low=l, close=c, volume=v,
    )


@pytest.fixture
def session_bars() -> list[Bar1m]:
    # 5 bars, monotonic close, varying volume
    specs = [
        (100.0, 100.5, 99.8, 100.3, 1000),
        (100.3, 100.8, 100.1, 100.6, 1500),
        (100.6, 101.0, 100.4, 100.9, 2000),
        (100.9, 101.3, 100.7, 101.1, 800),
        (101.1, 101.5, 100.9, 101.4, 1200),
    ]
    return [_mk_bar(i, *s) for i, s in enumerate(specs)]


# ---------------------------------------------------------------------------
# VWAP: bulk == incremental
# ---------------------------------------------------------------------------

def test_vwap_bulk_matches_incremental(session_bars):
    df = bars_to_frame(session_bars)
    bulk = compute_vwap_series(df).tolist()

    incremental = []
    for i, bar in enumerate(session_bars):
        incremental.append(next_vwap(bar, session_bars[:i]))

    # bulk uses .round(2); incremental too. Compare floats loosely just in case.
    assert bulk == pytest.approx(incremental, abs=1e-4)


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


def test_latest_atr_empty_frame_returns_none():
    assert latest_atr(pd.DataFrame()) is None


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
# RVOL cumulative: None when baseline unknown, else ratio
# ---------------------------------------------------------------------------

def test_rvol_unknown_baseline_returns_none():
    assert next_rvol_cum(1000, 5000.0, 0.0, 0.0) is None


def test_rvol_matches_ratio():
    # today so far = 5000 + new 1000 = 6000
    # baseline so far = 500 (history) + 250 (this slot) = 750
    # ratio = 6000 / 750 = 8.0
    r = next_rvol_cum(1000, 5000.0, 250.0, 500.0)
    assert r == pytest.approx(8.0, abs=1e-4)


# ---------------------------------------------------------------------------
# enrich_bar: end-to-end, all four indicators land on the bar
# ---------------------------------------------------------------------------

def test_enrich_bar_populates_all_indicators(session_bars):
    new_bar = _mk_bar(5, 101.4, 101.7, 101.2, 101.6, 900)
    out = enrich_bar(
        new_bar,
        history=session_bars,
        atr=1.5,
        baseline_slot_avg=1000.0,
        baseline_history_sum=4000.0,
    )
    assert out.vwap is not None
    assert out.ema9 is not None
    assert out.relatr is not None
    assert out.rvol_cum is not None
    # RelATR sign check: VWAP - close over atr
    assert out.relatr == pytest.approx(round((out.vwap - new_bar.close) / 1.5, 4))
