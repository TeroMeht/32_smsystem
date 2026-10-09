"""
Unified scanner + alarm configuration -- the ONE place filter values live.

Holds the Telegram on/off switch, the Uptrend Reversals filter values
(which are also the alarm thresholds) and the Reversal Shorts filter
values. The dashboard has no defaults of its own: it loads everything
from GET /api/alarms/config on page load, writes every edit back with
POST /api/alarms/config, and "Reset" restores the defaults defined in
the models below. Whatever the table shows is what the alarm uses.

Also holds the blocked-ticker list (the dashboard's "x" button): a
blocked symbol is hidden from both tables AND never alarmed, until
local (Helsinki) midnight -- same lifetime the UI always had.

Persisted as a small JSON file (``<repo>/data/alarm_config.json``) so
the switch and thresholds survive a restart. A missing / corrupt file
falls back to the defaults below (alarms OFF), never crashes startup.

Only the "Uptrend Reversals" setup is alarmed. Reversal Shorts and any
other stream are display-only by design.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_PATH = _REPO_ROOT / "data" / "alarm_config.json"


class ShortsFilters(BaseModel):
    """Reversal Shorts table filters (display only -- never alarmed)."""

    max_relatr: float = -0.40         # relatr <= (negative = extended UP from VWAP)
    min_rvol: float = 0.0
    min_volume: int = 5_000
    min_cum_volume: int = 100_000


class ShortsFiltersUpdate(BaseModel):
    max_relatr: float | None = None
    min_rvol: float | None = None
    min_volume: int | None = None
    min_cum_volume: int | None = None


# Field groups for Reset: each table's Reset only restores its own filters.
UPTREND_FILTER_FIELDS = (
    "min_relatr", "min_rvol", "max_chg_pct", "above_sma200",
    "min_volume", "min_cum_volume",
)


class AlarmConfig(BaseModel):
    """
    The defaults below are THE defaults -- the frontend has none of its own.
    """

    # Master switch for Telegram delivery. OFF by default: nothing is
    # sent until the user turns it on from the dashboard.
    enabled: bool = False

    # Uptrend Reversals filter set (same semantics as the UI filters).
    min_relatr: float = 0.40          # relatr >= (positive = extended DOWN from VWAP)
    min_rvol: float = 0.0             # rvol   >= (None fails, like the UI)
    max_chg_pct: float = -5.0         # chg%   <= vs previous close (None fails)
    above_sma200: bool = True         # close  >  latest SMA200 (missing SMA200 fails)
    min_volume: int = 5_000           # per-bar volume >=
    min_cum_volume: int = 100_000     # session cumulative volume >=

    # Per-(symbol, setup) cooldown so a still-qualifying symbol doesn't
    # re-alarm on every finalized 2-min bar.
    cooldown_minutes: int = Field(default=30, ge=1, le=24 * 60)

    # Attach the intraday price chart image to the Telegram alarm.
    send_chart: bool = True

    # Reversal Shorts table filters.
    shorts: ShortsFilters = Field(default_factory=ShortsFilters)

    # Blocked tickers: SYMBOL -> Helsinki date (YYYY-MM-DD) it was blocked.
    # Only entries dated today count; older ones are pruned on next write.
    # Changed only via block() / unblock(), never by update() or reset().
    blocked: dict[str, str] = Field(default_factory=dict)


class AlarmConfigUpdate(BaseModel):
    """Partial update -- every field optional so the UI can PATCH-style POST."""

    enabled: bool | None = None
    min_relatr: float | None = None
    min_rvol: float | None = None
    max_chg_pct: float | None = None
    above_sma200: bool | None = None
    min_volume: int | None = None
    min_cum_volume: int | None = None
    cooldown_minutes: int | None = Field(default=None, ge=1, le=24 * 60)
    send_chart: bool | None = None
    shorts: ShortsFiltersUpdate | None = None


_lock = threading.Lock()
_current: AlarmConfig | None = None


def _load_from_disk() -> AlarmConfig:
    try:
        if CONFIG_PATH.exists():
            return AlarmConfig.model_validate_json(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("alarm config at %s unreadable -- using defaults", CONFIG_PATH)
    return AlarmConfig()


def _save_to_disk(cfg: AlarmConfig) -> None:
    try:
        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = CONFIG_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(cfg.model_dump(), indent=2), encoding="utf-8")
        tmp.replace(CONFIG_PATH)
    except Exception:
        # Persistence is best-effort: the in-memory value still applies.
        logger.exception("failed to persist alarm config to %s", CONFIG_PATH)


def get() -> AlarmConfig:
    """Current config (lazy-loaded from disk on first access)."""
    global _current
    if _current is None:
        with _lock:
            if _current is None:
                _current = _load_from_disk()
                logger.info("Alarm config loaded: %s", _current.model_dump())
    return _current


def update(patch: AlarmConfigUpdate) -> AlarmConfig:
    """Apply a partial update, persist, return the new config."""
    global _current
    base = get()
    changes = patch.model_dump(exclude_none=True)
    shorts_changes = changes.pop("shorts", None) or {}
    with _lock:
        new = base.model_copy(update=changes)
        if shorts_changes:
            new = new.model_copy(
                update={"shorts": base.shorts.model_copy(update=shorts_changes)}
            )
        _current = AlarmConfig.model_validate(new.model_dump())
        _save_to_disk(_current)
    if changes or shorts_changes:
        logger.info("Alarm config updated: %s shorts=%s", changes, shorts_changes)
    return _current


_LOCAL_TZ = ZoneInfo("Europe/Helsinki")


def _today() -> str:
    """Local (Helsinki) date -- the dashboard clears blocks at local midnight."""
    return datetime.now(_LOCAL_TZ).date().isoformat()


def blocked_today() -> dict[str, str]:
    """Symbols blocked for the rest of today."""
    today = _today()
    return {s: d for s, d in get().blocked.items() if d == today}


def is_blocked(symbol: str) -> bool:
    return get().blocked.get(symbol.upper()) == _today()


def _set_blocked(mutate) -> AlarmConfig:
    global _current
    base = get()
    today = _today()
    blocked = {s: d for s, d in base.blocked.items() if d == today}  # prune old days
    mutate(blocked, today)
    with _lock:
        _current = base.model_copy(update={"blocked": blocked})
        _save_to_disk(_current)
    return _current


def block(symbol: str) -> AlarmConfig:
    sym = symbol.upper()
    logger.info("Blocked %s for today (no alarms, hidden in dashboard)", sym)
    return _set_blocked(lambda b, today: b.__setitem__(sym, today))


def unblock(symbol: str) -> AlarmConfig:
    sym = symbol.upper()
    logger.info("Unblocked %s", sym)
    return _set_blocked(lambda b, today: b.pop(sym, None))


def reset(scope: str) -> AlarmConfig:
    """
    Restore defaults for one table's filters: ``uptrend`` or ``shorts``.
    The Telegram switch and cooldown are never touched by a reset.
    """
    global _current
    base = get()
    defaults = AlarmConfig()
    if scope == "uptrend":
        upd = {k: getattr(defaults, k) for k in UPTREND_FILTER_FIELDS}
    elif scope == "shorts":
        upd = {"shorts": ShortsFilters()}
    else:
        raise ValueError(f"unknown reset scope: {scope!r}")
    with _lock:
        _current = base.model_copy(update=upd)
        _save_to_disk(_current)
    logger.info("Alarm config reset: %s", scope)
    return _current


def set_for_tests(cfg: AlarmConfig) -> None:
    """Replace the in-memory config without touching disk (tests only)."""
    global _current
    _current = cfg
