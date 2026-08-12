"""
Smoke test for bar_processor.process_bar.

Uses a stub asyncpg pool so we don't need a live DB. The point is to
verify enrich + persistence + session state + sink callback all wire
together, and that each bar's RVOL is today's cum volume divided by
the accumulated per-bar baseline for the bar's Helsinki-time slot.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from backend.datapipe.bar_processor import process_bar
from backend.datapipe.schemas import Bar1m
from backend.datapipe.session_state import SessionStore


class _StubConn:
    async def execute(self, *args, **kwargs):
        return None


class _StubAcquire:
    async def __aenter__(self):
        return _StubConn()

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _StubPool:
    def acquire(self):
        return _StubAcquire()


def _bar(minute: int, close: float, vol: int) -> Bar1m:
    return Bar1m(
        symbol="AAPL",
        symbolid=1,
        ts=datetime(2026, 8, 7, 13, 30, tzinfo=timezone.utc) + timedelta(minutes=minute),
        open=close, high=close + 0.1, low=close - 0.1, close=close, volume=vol,
    )


@pytest.mark.asyncio
async def test_process_bar_enriches_and_accumulates_history():
    pool = _StubPool()
    store = SessionStore()

    sink = AsyncMock()

    # Per-bar baselines (NOT cumulative). The bar_processor sums them on
    # the fly to form the RVOL denominator.
    #    16:30 slot: per-bar avg vol = 500
    #    16:31 slot: per-bar avg vol = 600
    b0 = _bar(0, 100.0, 1000)
    st = store.get_or_init(b0.symbol, b0.symbolid, b0.ts.date())
    # baseline is keyed by Helsinki slot;
    # 13:30/13:31 UTC == 16:30/16:31 Helsinki (Aug is EEST, UTC+3).
    st.rvol_baseline = {time(16, 30): 500.0, time(16, 31): 600.0}
    st.atr = 1.0

    out = await process_bar(pool, store, b0, sink=sink)
    assert out.vwap is not None
    assert out.ema9 is not None
    assert out.relatr is not None
    # First bar: today cum = 1000, cum baseline = 0 + 500 = 500 -> RVOL = 2.0
    assert out.rvol_cum == pytest.approx(2.0, abs=1e-4)
    sink.assert_awaited_once()

    # Second bar: today cum = 2200, cum baseline = 500 + 600 = 1100
    # -> RVOL = 2200 / 1100 = 2.0
    b1 = _bar(1, 100.5, 1200)
    out2 = await process_bar(pool, store, b1, sink=sink)
    st_after = store.get_or_init(b1.symbol, b1.symbolid, b1.ts.date())
    assert len(st_after.history) == 2
    # baseline_history_sum should be 500 + 600 = 1100 after the second bar
    assert st_after.baseline_history_sum == pytest.approx(1100.0)
    assert out2.rvol_cum == pytest.approx(2.0, abs=1e-4)
