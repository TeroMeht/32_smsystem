"""
End-to-end test of the alarms sink.

Wires the real dispatcher, state seed, capitulation strategy, and
alarm generator against a stub asyncpg pool and a monkey-patched
Telegram sender. Exercises the paths the frontend cares about:

    * lazy seed pulls sma200 + prev_close + cum_volume from the (stub) DB
      on first bar,
    * a bar that passes all Uptrend Reversals gates fires Telegram,
    * nothing fires while the UI switch is OFF,
    * chart path: photo with caption, text fallback when the chart fails,
    * a second passing bar inside the cooldown window is deduped,
    * a bar that fails any gate never reaches Telegram,
    * cum_volume increments per bar so a symbol seeded just under the
      floor rises above it as bars flow in.

No live pool, no live Telegram. Assertions are on the spy that
replaces ``send_telegram_message`` at import time. Delivery runs in a
background task, so every test awaits ``alarm_generator.drain_pending()``
before asserting.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from backend.alarms import alarm_config, alarm_generator, dedupe, dispatcher, send_telegram
from backend.alarms.alarm_config import AlarmConfig
from backend.alarms.strategies import capitulation


# --- Stub pool ---------------------------------------------------------------


class _StubConn:
    """
    Answers the three fetch() shapes the alarm state issues:
        * SELECT ... sma200 FROM daily_indicators ...
        * SELECT ... close FROM daily ...            (prev close)
        * SELECT symbolid, SUM(volume) ... FROM livestream ...
    Returned rows are asyncpg-record-like dicts.
    """

    def __init__(self, sma200_rows, cum_volume_rows, prev_close_rows):
        self._sma200_rows = sma200_rows
        self._cum_volume_rows = cum_volume_rows
        self._prev_close_rows = prev_close_rows

    async def fetch(self, sql, *args, **kwargs):
        s = " ".join(sql.split()).lower()
        if "sma200" in s and "daily_indicators" in s:
            return self._sma200_rows
        if "from daily where" in s and "close" in s:
            return self._prev_close_rows
        if "sum(volume)" in s and "livestream" in s:
            return self._cum_volume_rows
        raise AssertionError(f"unexpected query: {sql!r}")


class _StubAcquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _StubPool:
    def __init__(self, sma200_rows=(), cum_volume_rows=(), prev_close_rows=None):
        if prev_close_rows is None:
            # Default prev close 120 -> a close of 110 is a -8.3% day,
            # inside the -5% Chg% ceiling.
            prev_close_rows = [
                {"symbolid": r["symbolid"], "close": 120.0} for r in sma200_rows
            ] or [{"symbolid": 1, "close": 120.0}]
        self._conn = _StubConn(list(sma200_rows), list(cum_volume_rows),
                               list(prev_close_rows))

    def acquire(self):
        return _StubAcquire(self._conn)


# --- Fixtures ----------------------------------------------------------------


def _bar(*, minute=30, relatr=1.5, close=110.0, volume=5000,
         symbolid=1, symbol="AAPL", rvol=1.2):
    return SimpleNamespace(
        symbol=symbol,
        symbolid=symbolid,
        ts=datetime(2026, 8, 7, 13, minute, tzinfo=timezone.utc),
        open=close, high=close + 0.1, low=close - 0.1,
        close=close, volume=volume,
        relatr=relatr, rvol=rvol,
    )


def _cfg(**over):
    base = dict(
        enabled=True, min_relatr=0.45, min_rvol=0.0, max_chg_pct=-5.0,
        above_sma200=True, min_volume=100, min_cum_volume=100_000,
        cooldown_minutes=15, send_chart=False,
    )
    base.update(over)
    return AlarmConfig(**base)


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    """Freeze alarm config, clear dedupe, stub Telegram at every seam it's imported."""
    alarm_config.set_for_tests(_cfg())

    dedupe.reset()

    tg_spy = AsyncMock(return_value={"ok": True})
    # Patch at both the definition module and where alarm_generator
    # imported it, since Python bound the name at import time.
    monkeypatch.setattr(send_telegram, "send_telegram_message", tg_spy)
    monkeypatch.setattr(alarm_generator, "send_telegram_message", tg_spy)
    return tg_spy


async def _run(sink, bar):
    """Feed one bar and wait for its background Telegram delivery."""
    await sink(bar)
    await alarm_generator.drain_pending()


# --- Tests -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_passing_bar_fires_telegram(_isolate):
    pool = _StubPool(
        sma200_rows=[{"symbolid": 1, "sma200": 100.0}],
        cum_volume_rows=[{"symbolid": 1, "cum_volume": 500_000}],
    )
    sink = dispatcher.build_alarm_sink(pool)

    await _run(sink, _bar(relatr=1.5, close=110.0, volume=5000))

    _isolate.assert_awaited_once()
    args, kwargs = _isolate.call_args
    assert kwargs["symbol"] == "AAPL"
    assert kwargs["alarm_message"].startswith(capitulation.SIGNAL_NAME)


@pytest.mark.asyncio
async def test_second_bar_inside_cooldown_is_deduped(_isolate):
    pool = _StubPool(
        sma200_rows=[{"symbolid": 1, "sma200": 100.0}],
        cum_volume_rows=[{"symbolid": 1, "cum_volume": 500_000}],
    )
    sink = dispatcher.build_alarm_sink(pool)

    await _run(sink, _bar(minute=30, relatr=1.5, close=110.0, volume=5000))
    await _run(sink, _bar(minute=32, relatr=1.6, close=109.5, volume=5000))
    await _run(sink, _bar(minute=34, relatr=1.7, close=109.0, volume=5000))

    # All three bars pass filters, but only the first crosses the
    # cooldown boundary.
    assert _isolate.await_count == 1


@pytest.mark.asyncio
async def test_bar_below_sma200_never_hits_telegram(_isolate):
    pool = _StubPool(
        sma200_rows=[{"symbolid": 1, "sma200": 120.0}],
        cum_volume_rows=[{"symbolid": 1, "cum_volume": 500_000}],
    )
    sink = dispatcher.build_alarm_sink(pool)

    # Close is BELOW SMA200 -- Uptrend gate rejects.
    await _run(sink, _bar(relatr=1.5, close=110.0, volume=5000))
    assert _isolate.await_count == 0


@pytest.mark.asyncio
async def test_symbol_missing_sma200_is_dropped_when_gate_on(_isolate):
    pool = _StubPool(
        sma200_rows=[],  # symbolid 1 not present
        cum_volume_rows=[{"symbolid": 1, "cum_volume": 500_000}],
    )
    sink = dispatcher.build_alarm_sink(pool)

    await _run(sink, _bar(relatr=1.5, close=110.0, volume=5000))
    assert _isolate.await_count == 0


@pytest.mark.asyncio
async def test_cum_volume_accumulates_across_bars(_isolate):
    """Symbol seeded just under the CumVol floor rises above it as bars flow."""
    pool = _StubPool(
        sma200_rows=[{"symbolid": 1, "sma200": 100.0}],
        # Seeded at 90k (below 100k floor). One bar of 20k volume pushes
        # the running total to 110k -- above the floor, so the alarm fires.
        cum_volume_rows=[{"symbolid": 1, "cum_volume": 90_000}],
    )
    sink = dispatcher.build_alarm_sink(pool)

    await _run(sink, _bar(relatr=1.5, close=110.0, volume=20_000))
    assert _isolate.await_count == 1


@pytest.mark.asyncio
async def test_state_seed_happens_exactly_once(_isolate):
    """
    First bar triggers the DB seed; subsequent bars must NOT re-query
    (state is authoritative in-memory from that point on).
    """
    pool = _StubPool(
        sma200_rows=[{"symbolid": 1, "sma200": 100.0}],
        cum_volume_rows=[{"symbolid": 1, "cum_volume": 500_000}],
    )
    sink = dispatcher.build_alarm_sink(pool)

    # Count real fetch() calls on the stub conn to prove seed is one-shot.
    conn = pool._conn
    real_fetch = conn.fetch
    fetch_calls = []

    async def _counting_fetch(sql, *a, **k):
        fetch_calls.append(sql)
        return await real_fetch(sql, *a, **k)

    conn.fetch = _counting_fetch  # type: ignore[method-assign]

    await _run(sink, _bar(minute=30))
    await _run(sink, _bar(minute=32))
    await _run(sink, _bar(minute=34))

    # Three SELECTs on first bar (sma200 + prev_close + cum_volume),
    # zero on the rest.
    assert len(fetch_calls) == 3


@pytest.mark.asyncio
async def test_different_symbols_are_independent(_isolate):
    """Cooldown is per-(symbol, strategy) -- MSFT firing doesn't affect AAPL."""
    pool = _StubPool(
        sma200_rows=[
            {"symbolid": 1, "sma200": 100.0},
            {"symbolid": 2, "sma200": 200.0},
        ],
        cum_volume_rows=[
            {"symbolid": 1, "cum_volume": 500_000},
            {"symbolid": 2, "cum_volume": 500_000},
        ],
        prev_close_rows=[
            {"symbolid": 1, "close": 120.0},
            {"symbolid": 2, "close": 230.0},
        ],
    )
    sink = dispatcher.build_alarm_sink(pool)

    await _run(sink, _bar(minute=30, symbol="AAPL", symbolid=1,
                    relatr=1.5, close=110.0, volume=5000))
    await _run(sink, _bar(minute=31, symbol="MSFT", symbolid=2,
                    relatr=1.5, close=210.0, volume=5000))
    # Both fire -- independent cooldowns.
    assert _isolate.await_count == 2


@pytest.mark.asyncio
async def test_switch_off_sends_nothing(_isolate):
    """UI switch OFF -> passing bars never reach Telegram."""
    alarm_config.set_for_tests(_cfg(enabled=False))
    pool = _StubPool(
        sma200_rows=[{"symbolid": 1, "sma200": 100.0}],
        cum_volume_rows=[{"symbolid": 1, "cum_volume": 500_000}],
    )
    sink = dispatcher.build_alarm_sink(pool)
    await _run(sink, _bar(relatr=1.5, close=110.0, volume=5000))
    assert _isolate.await_count == 0


@pytest.mark.asyncio
async def test_chart_sent_as_photo_with_caption(_isolate, monkeypatch):
    alarm_config.set_for_tests(_cfg(send_chart=True))

    async def _fake_png(pool, symbol, alarm_ts=None):
        return b"PNG"

    photo_spy = AsyncMock(return_value={"ok": True, "result": {"photo": [1]}})
    monkeypatch.setattr(alarm_generator, "build_chart_png", _fake_png)
    monkeypatch.setattr(alarm_generator, "send_telegram_picture", photo_spy)

    pool = _StubPool(
        sma200_rows=[{"symbolid": 1, "sma200": 100.0}],
        cum_volume_rows=[{"symbolid": 1, "cum_volume": 500_000}],
    )
    sink = dispatcher.build_alarm_sink(pool)
    await _run(sink, _bar(relatr=1.5, close=110.0, volume=5000))

    photo_spy.assert_awaited_once()
    args, kwargs = photo_spy.call_args
    assert args[0] == b"PNG"
    assert "AAPL" in kwargs["caption"]
    assert capitulation.SIGNAL_NAME in kwargs["caption"]
    # Photo succeeded -> no separate text message.
    assert _isolate.await_count == 0


@pytest.mark.asyncio
async def test_chart_failure_falls_back_to_text(_isolate, monkeypatch):
    alarm_config.set_for_tests(_cfg(send_chart=True))

    async def _no_png(pool, symbol, alarm_ts=None):
        return None

    photo_spy = AsyncMock()
    monkeypatch.setattr(alarm_generator, "build_chart_png", _no_png)
    monkeypatch.setattr(alarm_generator, "send_telegram_picture", photo_spy)

    pool = _StubPool(
        sma200_rows=[{"symbolid": 1, "sma200": 100.0}],
        cum_volume_rows=[{"symbolid": 1, "cum_volume": 500_000}],
    )
    sink = dispatcher.build_alarm_sink(pool)
    await _run(sink, _bar(relatr=1.5, close=110.0, volume=5000))

    photo_spy.assert_not_awaited()
    _isolate.assert_awaited_once()
