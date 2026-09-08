"""
Unit tests for the capitulation alarm filter set.

Pure predicate tests -- no DB, no network. Each case flips ONE gate
so a regression on any filter surfaces as one named failure.

The bar is a ``SimpleNamespace`` test double rather than a real
``CandleRow``: production code only reads a handful of attributes off
the bar (symbol, symbolid, ts, close, volume, relatr), and stubbing
those decouples the test from CandleRow's exact schema.

The state is a hand-built stub with ``sma200`` / ``cum_volume``
methods returning fixed dict lookups -- mirrors the production
``AlarmState`` API without touching a DB.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from backend.alarms.strategies import capitulation
from backend.core.config import settings


# --- Test fixtures -----------------------------------------------------------


class _StubState:
    """Test double for AlarmState -- fixed sma200 / cum_volume per symbolid."""

    def __init__(self, sma200_by_sid=None, cum_volume_by_sid=None):
        self._sma = sma200_by_sid or {}
        self._cv = cum_volume_by_sid or {}

    def sma200(self, bar):
        return self._sma.get(bar.symbolid)

    def cum_volume(self, bar):
        return self._cv.get(bar.symbolid, 0)


def _bar(*, relatr=1.0, close=100.0, volume=1000, symbolid=1, symbol="AAPL"):
    return SimpleNamespace(
        symbol=symbol,
        symbolid=symbolid,
        ts=datetime(2026, 8, 7, 13, 30, tzinfo=timezone.utc),
        open=close, high=close + 0.1, low=close - 0.1,
        close=close, volume=volume,
        relatr=relatr, rvol=None,
    )


@pytest.fixture(autouse=True)
def _alarm_settings(monkeypatch):
    """Freeze production settings so a live env file can't perturb tests."""
    monkeypatch.setattr(settings, "ALARM_MIN_RELATR", 0.45)
    monkeypatch.setattr(settings, "ALARM_MIN_VOLUME", 100)
    monkeypatch.setattr(settings, "ALARM_MIN_CUM_VOLUME", 100_000)
    monkeypatch.setattr(settings, "ALARM_REQUIRE_ABOVE_SMA200", True)


# --- _passes_filters: one test per gate --------------------------------------


def test_all_gates_pass():
    """Baseline: every gate satisfied -> fires."""
    bar = _bar(relatr=1.5, close=110.0, volume=5000)
    state = _StubState(sma200_by_sid={1: 100.0}, cum_volume_by_sid={1: 500_000})
    assert capitulation._passes_filters(bar, state) is True


def test_relatr_below_threshold_blocks():
    bar = _bar(relatr=0.30)  # < 0.45
    state = _StubState(sma200_by_sid={1: 100.0}, cum_volume_by_sid={1: 500_000})
    assert capitulation._passes_filters(bar, state) is False


def test_relatr_none_blocks():
    bar = _bar(relatr=None)
    state = _StubState(sma200_by_sid={1: 100.0}, cum_volume_by_sid={1: 500_000})
    assert capitulation._passes_filters(bar, state) is False


def test_bar_volume_below_floor_blocks():
    bar = _bar(volume=50)  # < 100
    state = _StubState(sma200_by_sid={1: 100.0}, cum_volume_by_sid={1: 500_000})
    assert capitulation._passes_filters(bar, state) is False


def test_cum_volume_below_floor_blocks():
    bar = _bar()
    state = _StubState(sma200_by_sid={1: 100.0}, cum_volume_by_sid={1: 50_000})
    assert capitulation._passes_filters(bar, state) is False


def test_sma200_missing_blocks_when_gate_on():
    """Missing SMA200 = 'trend unknown' -- must drop, matching frontend."""
    bar = _bar(close=110.0)
    state = _StubState(sma200_by_sid={}, cum_volume_by_sid={1: 500_000})
    assert capitulation._passes_filters(bar, state) is False


def test_close_at_or_below_sma200_blocks():
    """Close must be strictly above SMA200 (uptrend gate)."""
    bar = _bar(close=100.0)
    state = _StubState(sma200_by_sid={1: 100.0}, cum_volume_by_sid={1: 500_000})
    assert capitulation._passes_filters(bar, state) is False


def test_sma200_gate_off_ignores_sma200(monkeypatch):
    """When the gate is disabled, missing SMA200 no longer blocks the fire."""
    monkeypatch.setattr(settings, "ALARM_REQUIRE_ABOVE_SMA200", False)
    bar = _bar(close=90.0)
    state = _StubState(sma200_by_sid={}, cum_volume_by_sid={1: 500_000})
    assert capitulation._passes_filters(bar, state) is True


# --- run(): integration with generate_signal_alarm ---------------------------


@pytest.mark.asyncio
async def test_run_fires_generate_signal_alarm_on_pass(monkeypatch):
    """A passing bar reaches generate_signal_alarm with the right signal name."""
    called = []

    async def _fake_gsa(*, bar, signal_name):
        called.append((bar.symbol, signal_name))

    monkeypatch.setattr(capitulation, "generate_signal_alarm", _fake_gsa)

    bar = _bar(relatr=1.5, close=110.0, volume=5000)
    state = _StubState(sma200_by_sid={1: 100.0}, cum_volume_by_sid={1: 500_000})

    await capitulation.run(bar, state)
    assert called == [("AAPL", capitulation.SIGNAL_NAME)]


@pytest.mark.asyncio
async def test_run_skips_generate_signal_alarm_when_filtered(monkeypatch):
    """A failing bar never reaches generate_signal_alarm."""
    called = []

    async def _fake_gsa(*, bar, signal_name):
        called.append(bar.symbol)

    monkeypatch.setattr(capitulation, "generate_signal_alarm", _fake_gsa)

    bar = _bar(relatr=0.10)  # below trigger threshold
    state = _StubState(sma200_by_sid={1: 100.0}, cum_volume_by_sid={1: 500_000})

    await capitulation.run(bar, state)
    assert called == []
