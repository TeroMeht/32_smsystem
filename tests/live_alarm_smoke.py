"""
Live end-to-end alarm smoke -- REALLY sends a Telegram message.

Not a pytest (filename intentionally lacks the ``test_`` prefix so
``pytest`` never collects it). Runs the actual alarms pipeline
against a hand-built bar and hits the real Telegram Bot API using
``TELEGRAM_BOT_TOKEN`` / ``TELEGRAM_CHAT_ID`` from your env file.

What this proves in ONE shot:
    * settings load from ``C:/codebase/env-repo/32_smsystem.env``,
    * dispatcher builds and lazily seeds AlarmState off a stub pool,
    * capitulation strategy accepts a passing bar,
    * generate_signal_alarm dedupe path runs,
    * send_telegram_message reaches Telegram and returns ``ok: true``.

Only the DB pool is stubbed (SMA200 + cum_volume seed rows are
injected). Everything else is production code paths.

--- HOW TO RUN --------------------------------------------------------------

From the repo root:

    .venv\\Scripts\\python tests\\live_alarm_smoke.py

You should see:
    * a log line ``ALARM capitulation_down | SMOKE | ...``,
    * a log line ``Telegram sent: SMOKE | capitulation_down``,
    * a Telegram message in your configured chat within a second or two.

The message is tagged with symbol ``SMOKE`` (never a real ticker) so
you can never confuse a smoke test with a live alarm in the chat log.

--- CAVEATS ---------------------------------------------------------------

* This bypasses per-symbol dedupe on the SMOKE symbol only by using
  a fresh ``dedupe.reset()`` at the start of every run.
* No livestream DB write happens (we're not going through
  ``process_bar``) -- the test hits the sink directly, which is the
  exact seam ``process_bar`` invokes in production.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

# Repo-root bootstrap so a plain ``python tests\live_alarm_smoke.py``
# from the project root works. Without this, only ``tests\`` lands on
# sys.path and ``from backend.* import ...`` fails with
# ModuleNotFoundError. Also lets ``python -m tests.live_alarm_smoke``
# and ``pytest`` (never actually collects this file -- no test_ prefix)
# both continue to work unchanged.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.alarms import dedupe, dispatcher  # noqa: E402
from backend.common.logging_config import setup_app_logging  # noqa: E402
from backend.core.config import settings  # noqa: E402


# --- Stub pool (SMA200 + cum_volume seed only, no writes) --------------------


class _StubConn:
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
    def __init__(self, sma200_rows, cum_volume_rows):
        self._conn = _StubConn(sma200_rows, cum_volume_rows)

    def acquire(self):
        return _StubAcquire(self._conn)


# --- Synthetic bar built to pass every gate ---------------------------------


def _passing_bar():
    """
    Values are chosen to clear every configured threshold with plenty
    of headroom, so the smoke passes even after conservative tuning
    of ALARM_* env values.
    """
    return SimpleNamespace(
        symbol="SMOKE",
        symbolid=999_999,
        ts=datetime.now(timezone.utc),
        open=100.0, high=101.0, low=99.0,
        close=110.0,       # > sma200 (100.0 in the seed rows below)
        volume=10_000,     # >> ALARM_MIN_VOLUME (default 100)
        relatr=2.5,        # >> ALARM_MIN_RELATR (default 0.45)
        rvol=3.0,
    )


async def main() -> int:
    setup_app_logging(log_dir=None)  # stdout-only for the smoke
    log = logging.getLogger("alarm_smoke")

    log.info("=" * 60)
    log.info("Live alarm smoke starting")
    log.info("Chat: %s | Bot token: %s...",
             settings.TELEGRAM_CHAT_ID,
             (settings.TELEGRAM_BOT_TOKEN or "")[:10])
    log.info("Thresholds: relatr>=%.2f vol>=%d cumvol>=%d aboveSMA200=%s cooldown=%dm",
             settings.ALARM_MIN_RELATR, settings.ALARM_MIN_VOLUME,
             settings.ALARM_MIN_CUM_VOLUME, settings.ALARM_REQUIRE_ABOVE_SMA200,
             settings.ALARM_COOLDOWN_MINUTES)
    log.info("=" * 60)

    # Clear any prior fire for SMOKE so re-runs always send a message.
    dedupe.reset()

    # Seed rows chosen so a bar with close=110 passes the SMA200 gate
    # and a bar with volume=10_000 pushes cum_volume well past the
    # ALARM_MIN_CUM_VOLUME default (100k).
    pool = _StubPool(
        sma200_rows=[{"symbolid": 999_999, "sma200": 100.0}],
        cum_volume_rows=[{"symbolid": 999_999, "cum_volume": 200_000}],
    )

    sink = dispatcher.build_alarm_sink(pool)
    await sink(_passing_bar())

    log.info("Smoke complete -- check your Telegram chat.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
