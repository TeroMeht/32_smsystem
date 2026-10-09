"""
Unit tests for the capitulation alarm filter set.

Pure predicate tests -- no DB, no network. Each case flips ONE gate
so a regression on any filter surfaces as one named failure.

The bar is a ``SimpleNamespace`` test double rather than a real
``CandleRow``: production code only reads a handful of attributes off
the bar (symbol, symbolid, ts, close, volume, relatr), and stubbing
those decouples the test from CandleRow's exact schema.

The state is a hand-built stub with ``sma200`` / ``prev_close`` /
``cum_volume`` methods returning fixed dict lookups -- mirrors the production
``AlarmState`` API without touching a DB.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from backend.alarms import alarm_config
from backend.alarms.alarm_config import AlarmConfig
from backend.alarms.strategies import capitulation


# --- Test fixtures -----------------------------------------------------------


class _StubState:
    """Test double for AlarmState -- fixed sma200 / cum_volume per symbolid."""

    pool = None

    def __init__(self, sma200_by_sid=None, cum_volume_by_sid=None, prev_close_by_sid=None):
        self._sma = sma200_by_sid or {}
        self._cv = cum_volume_by_sid or {}
        # Default prev close 120: close 110 -> -8.3% (passes Chg% <= -5).
        self._pc = prev_close_by_sid if prev_close_by_sid is not None else {1: 120.0}

    def prev_close(self, bar):
        return self._pc.get(bar.symbolid)

    def sma200(self, bar):
        return self._sma.get(bar.symbolid)

    def cum_volume(self, bar):
        return self._cv.get(bar.symbolid, 0)


def _bar(*, relatr=1.0, close=100.0, volume=1000, symbolid=1, symbol="AAPL", rvol=1.2):
    return SimpleNamespace(
        symbol=symbol,
        symbolid=symbolid,
        ts=datetime(2026, 8, 7, 13, 30, tzinfo=timezone.utc),
        open=close, high=close + 0.1, low=close - 0.1,
        close=close, volume=volume,
        relatr=relatr, rvol=rvol,
    )


@pytest.fixture(autouse=True)
def _alarm_settings():
    """Freeze alarm config so the persisted UI config can't perturb tests."""
    alarm_config.set_for_tests(_cfg())


def _cfg(**over):
    base = dict(
        enabled=True, min_relatr=0.45, min_rvol=0.0, max_chg_pct=-5.0,
        above_sma200=True, min_volume=100, min_cum_volume=100_000,
        cooldown_minutes=15, send_chart=False,
    )
    base.update(over)
    return AlarmConfig(**base)


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


def test_rvol_below_floor_blocks():
    alarm_config.set_for_tests(_cfg(min_rvol=1.5))
    bar = _bar(relatr=1.5, close=110.0, volume=5000, rvol=1.0)
    state = _StubState(sma200_by_sid={1: 100.0}, cum_volume_by_sid={1: 500_000})
    assert capitulation._passes_filters(bar, state) is False


def test_rvol_none_blocks():
    """UI's number filter drops a null rvol -- alarm must do the same."""
    bar = _bar(relatr=1.5, close=110.0, volume=5000, rvol=None)
    state = _StubState(sma200_by_sid={1: 100.0}, cum_volume_by_sid={1: 500_000})
    assert capitulation._passes_filters(bar, state) is False


def test_chg_above_ceiling_blocks():
    """-2% day is not deep enough for a -5% ceiling."""
    bar = _bar(relatr=1.5, close=110.0, volume=5000)
    state = _StubState(sma200_by_sid={1: 100.0}, cum_volume_by_sid={1: 500_000},
                       prev_close_by_sid={1: 112.25})
    assert capitulation._passes_filters(bar, state) is False


def test_chg_missing_prev_close_blocks():
    bar = _bar(relatr=1.5, close=110.0, volume=5000)
    state = _StubState(sma200_by_sid={1: 100.0}, cum_volume_by_sid={1: 500_000},
                       prev_close_by_sid={})
    assert capitulation._passes_filters(bar, state) is False


def test_sma200_gate_off_ignores_sma200():
    """When the gate is disabled, missing SMA200 no longer blocks the fire."""
    alarm_config.set_for_tests(_cfg(above_sma200=False))
    bar = _bar(close=90.0)
    state = _StubState(sma200_by_sid={}, cum_volume_by_sid={1: 500_000},
                       prev_close_by_sid={1: 100.0})
    assert capitulation._passes_filters(bar, state) is True


# --- run(): integration with generate_signal_alarm ---------------------------


@pytest.mark.asyncio
async def test_run_fires_generate_signal_alarm_on_pass(monkeypatch):
    """A passing bar reaches generate_signal_alarm with the right signal name."""
    called = []

    async def _fake_gsa(*, bar, signal_name, pool=None, details=""):
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

    async def _fake_gsa(*, bar, signal_name, pool=None, details=""):
        called.append(bar.symbol)

    monkeypatch.setattr(capitulation, "generate_signal_alarm", _fake_gsa)

    bar = _bar(relatr=0.10)  # below trigger threshold
    state = _StubState(sma200_by_sid={1: 100.0}, cum_volume_by_sid={1: 500_000})

    await capitulation.run(bar, state)
    assert called == []


@pytest.mark.asyncio
async def test_run_does_nothing_when_switch_off(monkeypatch):
    alarm_config.set_for_tests(_cfg(enabled=False))
    called = []

    async def _fake_gsa(*, bar, signal_name, pool=None, details=""):
        called.append(bar.symbol)

    monkeypatch.setattr(capitulation, "generate_signal_alarm", _fake_gsa)
    bar = _bar(relatr=1.5, close=110.0, volume=5000)
    state = _StubState(sma200_by_sid={1: 100.0}, cum_volume_by_sid={1: 500_000})
    await capitulation.run(bar, state)
    assert called == []


def test_config_persists_roundtrip(tmp_path, monkeypatch):
    from backend.alarms.alarm_config import AlarmConfigUpdate

    monkeypatch.setattr(alarm_config, "CONFIG_PATH", tmp_path / "alarm_config.json")
    alarm_config.set_for_tests(AlarmConfig())
    alarm_config.update(AlarmConfigUpdate(enabled=True, min_relatr=0.6, cooldown_minutes=20))

    alarm_config._current = None  # simulate restart
    cfg = alarm_config.get()
    assert cfg.enabled is True
    assert cfg.min_relatr == 0.6
    assert cfg.cooldown_minutes == 20
    assert cfg.min_volume == AlarmConfig().min_volume  # untouched default


def test_shorts_filters_partial_update_and_reset(tmp_path, monkeypatch):
    """Shorts filters live in the same config; reset only touches its own table."""
    from backend.alarms.alarm_config import AlarmConfigUpdate, ShortsFiltersUpdate

    monkeypatch.setattr(alarm_config, "CONFIG_PATH", tmp_path / "alarm_config.json")
    alarm_config.set_for_tests(AlarmConfig())

    alarm_config.update(AlarmConfigUpdate(
        enabled=True, min_relatr=0.7,
        shorts=ShortsFiltersUpdate(max_relatr=-0.9),
    ))
    cfg = alarm_config.get()
    assert cfg.shorts.max_relatr == -0.9
    assert cfg.shorts.min_volume == 5_000          # untouched shorts field kept

    alarm_config.reset("shorts")
    cfg = alarm_config.get()
    assert cfg.shorts.max_relatr == -0.40          # shorts back to default
    assert cfg.min_relatr == 0.7                   # uptrend untouched
    assert cfg.enabled is True                     # switch never reset

    alarm_config.reset("uptrend")
    cfg = alarm_config.get()
    assert cfg.min_relatr == AlarmConfig().min_relatr
    assert cfg.enabled is True

    alarm_config._current = None                   # simulate restart
    assert alarm_config.get().model_dump() == cfg.model_dump()


@pytest.mark.asyncio
async def test_blocked_symbol_gets_no_alarm(tmp_path, monkeypatch):
    """Ticker blocked from the dashboard ('x') must never reach the alarm."""
    monkeypatch.setattr(alarm_config, "CONFIG_PATH", tmp_path / "alarm_config.json")
    alarm_config.set_for_tests(_cfg())
    called = []

    async def _fake_gsa(*, bar, signal_name, pool=None, details=""):
        called.append(bar.symbol)

    monkeypatch.setattr(capitulation, "generate_signal_alarm", _fake_gsa)
    bar = _bar(relatr=1.5, close=110.0, volume=5000)
    state = _StubState(sma200_by_sid={1: 100.0}, cum_volume_by_sid={1: 500_000})

    alarm_config.block("aapl")                       # case-insensitive
    await capitulation.run(bar, state)
    assert called == []

    alarm_config.unblock("AAPL")
    await capitulation.run(bar, state)
    assert called == ["AAPL"]


def test_block_expires_next_day_and_persists(tmp_path, monkeypatch):
    monkeypatch.setattr(alarm_config, "CONFIG_PATH", tmp_path / "alarm_config.json")
    alarm_config.set_for_tests(AlarmConfig())

    monkeypatch.setattr(alarm_config, "_today", lambda: "2026-10-08")
    alarm_config.block("TSLA")
    alarm_config._current = None                     # restart: reload from disk
    assert alarm_config.is_blocked("TSLA")
    assert alarm_config.blocked_today() == {"TSLA": "2026-10-08"}

    monkeypatch.setattr(alarm_config, "_today", lambda: "2026-10-09")
    assert not alarm_config.is_blocked("TSLA")       # midnight passed
    assert alarm_config.blocked_today() == {}
    alarm_config.block("NVDA")                       # next write prunes old day
    assert alarm_config.get().blocked == {"NVDA": "2026-10-09"}


def test_filter_reset_keeps_blocked(tmp_path, monkeypatch):
    monkeypatch.setattr(alarm_config, "CONFIG_PATH", tmp_path / "alarm_config.json")
    alarm_config.set_for_tests(AlarmConfig())
    alarm_config.block("AMD")
    alarm_config.reset("uptrend")
    alarm_config.reset("shorts")
    assert alarm_config.is_blocked("AMD")
