"""
Unit tests for per-(symbol, strategy) alarm cooldown.

Cooldown is keyed on the BAR timestamp so replay behaves identically
to live -- the tests use synthetic tz-aware datetimes to drive that
window directly, no wall-clock sleeps.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from backend.alarms import dedupe


@pytest.fixture(autouse=True)
def _clean_dedupe_state():
    """Isolate every test from every other test's fires."""
    dedupe.reset()
    yield
    dedupe.reset()


def _ts(minute: int) -> datetime:
    return datetime(2026, 8, 7, 13, minute, tzinfo=timezone.utc)


def test_first_fire_allowed():
    assert dedupe.should_fire("AAPL", "capitulation_down", _ts(30), cooldown_minutes=15) is True


def test_second_fire_within_cooldown_blocked():
    dedupe.record_fire("AAPL", "capitulation_down", _ts(30))
    assert dedupe.should_fire("AAPL", "capitulation_down", _ts(40), cooldown_minutes=15) is False


def test_second_fire_after_cooldown_allowed():
    dedupe.record_fire("AAPL", "capitulation_down", _ts(30))
    # 15 minutes later is exactly on the boundary -> allowed (>=).
    assert dedupe.should_fire("AAPL", "capitulation_down", _ts(45), cooldown_minutes=15) is True


def test_should_fire_does_not_mutate_state():
    """A dedupe-refused check must not reset the cooldown."""
    dedupe.record_fire("AAPL", "capitulation_down", _ts(30))
    # Multiple checks inside the window all return False and do NOT
    # extend or reset anything.
    for m in (35, 36, 37):
        assert dedupe.should_fire("AAPL", "capitulation_down", _ts(m), cooldown_minutes=15) is False
    # Boundary crossing is still honored.
    assert dedupe.should_fire("AAPL", "capitulation_down", _ts(45), cooldown_minutes=15) is True


def test_different_symbol_is_independent():
    dedupe.record_fire("AAPL", "capitulation_down", _ts(30))
    assert dedupe.should_fire("MSFT", "capitulation_down", _ts(31), cooldown_minutes=15) is True


def test_different_strategy_is_independent():
    dedupe.record_fire("AAPL", "capitulation_down", _ts(30))
    assert dedupe.should_fire("AAPL", "other_signal", _ts(31), cooldown_minutes=15) is True


def test_symbol_case_insensitive():
    """Symbol is normalized so 'aapl' and 'AAPL' share one cooldown."""
    dedupe.record_fire("AAPL", "capitulation_down", _ts(30))
    assert dedupe.should_fire("aapl", "capitulation_down", _ts(31), cooldown_minutes=15) is False


def test_reset_clears_all_state():
    dedupe.record_fire("AAPL", "capitulation_down", _ts(30))
    dedupe.reset()
    assert dedupe.should_fire("AAPL", "capitulation_down", _ts(31), cooldown_minutes=15) is True
