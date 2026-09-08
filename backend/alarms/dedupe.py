"""
Per-symbol, per-strategy alarm dedupe.

In-memory ``(symbol, strategy_name) -> last_bar_ts`` map. Keyed on the
BAR timestamp (not wall time) so the cooldown behaves identically in
live and replay -- a replay that streams a day's worth of bars in
seconds must not spam Telegram once per bar for a still-capitulating
symbol.

Cooldown window is configured via ``settings.ALARM_COOLDOWN_MINUTES``.
No persistence: on process restart every symbol/strategy pair is
fresh, which is fine for a monitoring layer -- worst case is one extra
alarm right after a restart while the same setup is still valid.

If we later want cross-restart dedupe, drop in a small ``alarms``
table via ``backend.database.writers`` and check it here before the
in-memory map. The public API of this module stays the same.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, Tuple


_last_fire: Dict[Tuple[str, str], datetime] = {}


def should_fire(
    symbol: str,
    strategy_name: str,
    bar_ts: datetime,
    cooldown_minutes: int,
) -> bool:
    """
    Return True if we're outside the cooldown window for this
    (symbol, strategy) pair. Does NOT mutate state -- call
    ``record_fire`` after the alarm has actually been dispatched so a
    dedupe-refused alarm doesn't reset the window.
    """
    key = (symbol.upper(), strategy_name)
    last = _last_fire.get(key)
    if last is None:
        return True
    return bar_ts - last >= timedelta(minutes=cooldown_minutes)


def record_fire(symbol: str, strategy_name: str, bar_ts: datetime) -> None:
    """Mark ``(symbol, strategy_name)`` as fired at ``bar_ts``."""
    _last_fire[(symbol.upper(), strategy_name)] = bar_ts


def reset() -> None:
    """Drop all cooldown state -- session boundary / tests."""
    _last_fire.clear()
