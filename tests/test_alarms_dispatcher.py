"""
End-to-end test of the alarms sink.

Wires the real dispatcher, state seed, capitulation strategy, and
alarm generator against a stub asyncpg pool and a monkey-patched
Telegram sender. Exercises the paths the frontend cares about:

    * lazy seed pulls sma200 + cum_volume from the (stub) DB on first bar,
    * a bar that passes all four Uptrend Reversals gates fires Telegram,
    * a second passing bar inside the cooldown window is deduped,
    * a bar that fails any gate never reaches Telegram,
    * cum_volume increments per bar so a symbol seeded just under the
      floor rises above it as bars flow in.

No live pool, no live Telegram. Assertions are on the spy that
replaces ``send_telegram_message`` at import time.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from backend.alarms import alarm_generator, dedupe, dispatcher, send_telegram, state
from backend.alarms.strategies import capitulation
from backend.core.config import settings


# --- Stub pool ---------------------------------------------------------------


class _StubConn:
    """
    Answers the two fetch() shapes the alarm state issues:
        * SELECT ... sma200 FROM daily_indicators ...
        * SELECT symbolid, SUM(volume) ... FROM livestream ...
    Returned rows are asyncpg-record-like dicts.
    """

    def __init__(self, sma200_rows, cum_volume_rows):
        self._sma200_rows = sma200_rows
        self._cum_volume_rows = cum_volume_rows

    async def fetch(self, sql, *args, **kwargs):
        s = " ".join(sql.split()).lower()
        if "sma200" in s and "daily_indicators" in s:
            return self._sma200_rows
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
    def __init__(self, sma200_rows=(), cum_volume_rows=()):
        self._conn = _StubConn(list(sma200_rows), list(cum_volume_rows))

    def acquire(self):
        return _StubAcquire(self._conn)


# --- Fixtures ----------------------------------------------------------------


def _bar(*, minute=30, relatr=1.5, close=110.0, volume=5000,
         symbolid=1, symbol="AAPL"):
    return SimpleNamespace(
        symbol=symbol,
        symbolid=symbolid,
        ts=datetime(2026, 8, 7, 13, minute, tzinfo=timezone.utc),
        open=close, high=close + 0.1, low=close - 0.1,
        close=close, volume=volume,
        relatr=relatr, rvol=None,
    )


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    """Freeze settings, clear dedupe, stub Telegram at every seam it's imported."""
    monkeypatch.setattr(settings, "ALARM_MIN_RELATR", 0.45)
    monkeypatch.setattr(settings, "ALARM_MIN_VOLUME", 100)
    monkeypatch.setattr(settings, "ALARM_MIN_CUM_VOLUME", 100_000)
    monkeypatch.setattr(settings, "ALARM_REQUIRE_ABOVE_SMA200", True)
    monkeypatch.setattr(settings, "ALARM_COOLDOWN_MINUTES", 15)

    dedupe.reset()

    tg_spy = AsyncMock(return_value={"ok": True})
    # Patch at both the definition module and where alarm_generator
    # imported it, since Python bound the name at import time.
    monkeypatch.setattr(send_telegram, "send_telegram_message", tg_spy)
    monkeypatch.setattr(alarm_generator, "send_telegram_message", tg_spy)
    return tg_spy


# --- Tests -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_passing_bar_fires_telegram(_isolate):
    pool = _StubPool(
        sma200_rows=[{"symbolid": 1, "sma200": 100.0}],
        cum_volume_rows=[{"symbolid": 1, "cum_volume": 500_000}],
    )
    sink = dispatcher.build_alarm_sink(pool)

    await sink(_bar(relatr=1.5, close=110.0, volume=5000))

    _isolate.assert_awaited_once()
    args, kwargs = _isolate.call_args
    assert kwargs["symbol"] == "AAPL"
    assert kwargs["alarm_message"] == capitulation.SIGNAL_NAME


@pytest.mark.asyncio
async def test_second_bar_inside_cooldown_is_deduped(_isolate):
    pool = _StubPool(
        sma200_rows=[{"symbolid": 1, "sma200": 100.0}],
        cum_volume_rows=[{"symbolid": 1, "cum_volume": 500_000}],
    )
    sink = dispatcher.build_alarm_sink(pool)

    await sink(_bar(minute=30, relatr=1.5, close=110.0, volume=5000))
    await sink(_bar(minute=32, relatr=1.6, close=109.5, volume=5000))
    await sink(_bar(minute=34, relatr=1.7, close=109.0, volume=5000))

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
    await sink(_bar(relatr=1.5, close=110.0, volume=5000))
    assert _isolate.await_count == 0


@pytest.mark.asyncio
async def test_symbol_missing_sma200_is_dropped_when_gate_on(_isolate):
    pool = _StubPool(
        sma200_rows=[],  # symbolid 1 not present
        cum_volume_rows=[{"symbolid": 1, "cum_volume": 500_000}],
    )
    sink = dispatcher.build_alarm_sink(pool)

    await sink(_bar(relatr=1.5, close=110.0, volume=5000))
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

    await sink(_bar(relatr=1.5, close=110.0, volume=20_000))
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

    await sink(_bar(minute=30))
    await sink(_bar(minute=32))
    await sink(_bar(minute=34))

    # Two SELECTs on first bar (sma200 + cum_volume), zero on the rest.
    assert len(fetch_calls) == 2


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
    )
    sink = dispatcher.build_alarm_sink(pool)

    await sink(_bar(minute=30, symbol="AAPL", symbolid=1,
                    relatr=1.5, close=110.0, volume=5000))
    await sink(_bar(minute=31, symbol="MSFT", symbolid=2,
                    relatr=1.5, close=210.0, volume=5000))
    # Both fire -- independent cooldowns.
    assert _isolate.await_count == 2
