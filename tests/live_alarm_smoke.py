"""
Live end-to-end alarm smoke -- REALLY sends a Telegram message.

Not a pytest (filename intentionally lacks the ``test_`` prefix so
``pytest`` never collects it). Runs the actual alarms pipeline
against a hand-built bar and hits the real Telegram Bot API using
``TELEGRAM_BOT_TOKEN`` / ``TELEGRAM_CHAT_ID`` from your env file.

What this proves in ONE shot:
    * settings load from ``C:/codebase/env-repo/32_smsystem.env``,
    * dispatcher builds and lazily seeds AlarmState off a stub pool,
    * capitulation (Uptrend Reversals) strategy accepts a passing bar,
    * generate_signal_alarm dedupe path runs,
    * the intraday chart renders (plotly + kaleido) from synthetic bars,
    * send_telegram_picture reaches Telegram with the alarm as caption.

Only the DB pool is stubbed (SMA200 / prev close / cum_volume seed rows
and a synthetic session of 2-min bars for the chart). The alarm config
is forced ON in memory for this run only -- the persisted UI setting
in data/alarm_config.json is not touched.

--- HOW TO RUN --------------------------------------------------------------

From the repo root:

    .venv\\Scripts\\python tests\\live_alarm_smoke.py

You should see:
    * a log line ``ALARM uptrend_reversal | SMOKE | ...``,
    * a log line ``Telegram picture sent: ...``,
    * a Telegram photo (chart + alarm caption) within a few seconds.

If the chart can't render you get a text-only message instead and a
``chart render failed`` traceback in the log (usually: no Chrome for
kaleido -- run ``.venv\\Scripts\\python -c "import kaleido; kaleido.get_chrome_sync()"``).

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
import math
from datetime import datetime, timedelta, timezone
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

from backend.alarms import alarm_config, alarm_generator, dedupe, dispatcher  # noqa: E402
from backend.alarms.alarm_config import AlarmConfig  # noqa: E402
from backend.common.logging_config import setup_app_logging  # noqa: E402
from backend.core.config import settings  # noqa: E402


# --- Stub pool (SMA200 + cum_volume seed only, no writes) --------------------


def _synthetic_session(end_ts: datetime, n: int = 120) -> list[dict]:
    """A fake 2-min session that sells off below VWAP -- chart material only."""
    rows, vol_px, vol_sum = [], 0.0, 0
    for i in range(n):
        ts = end_ts - timedelta(minutes=2 * (n - 1 - i))
        base = 120.0 - 10.0 * i / n + 0.6 * math.sin(i / 4)
        o, c = base + 0.15, base - 0.15
        v = 4000 + 300 * (i % 7)
        vol_px += c * v
        vol_sum += v
        vwap = vol_px / vol_sum
        rows.append({
            "ts": ts, "open": o, "high": max(o, c) + 0.2, "low": min(o, c) - 0.2,
            "close": c, "volume": v, "vwap": vwap, "ema9": base,
            "rvol": 1.5, "relatr": (vwap - c) / 2.0, "day_atr_ext": None,
        })
    return rows


class _StubConn:
    def __init__(self, sma200_rows, cum_volume_rows, prev_close_rows, bars):
        self._sma200_rows = sma200_rows
        self._cum_volume_rows = cum_volume_rows
        self._prev_close_rows = prev_close_rows
        self._bars = bars

    async def fetch(self, sql, *args, **kwargs):
        s = " ".join(sql.split()).lower()
        if "sma200" in s and "daily_indicators" in s:
            return self._sma200_rows
        if "from daily where" in s and "close" in s:
            return self._prev_close_rows
        if "from livestream l" in s and "ms.symbol = $1" in s:
            return self._bars
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
    def __init__(self, sma200_rows, cum_volume_rows, prev_close_rows, bars):
        self._conn = _StubConn(sma200_rows, cum_volume_rows, prev_close_rows, bars)

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
        volume=10_000,     # >> min_volume
        relatr=2.5,        # >> min_relatr
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
    log.info("=" * 60)

    # Force alarms ON for this run with the default UI thresholds (in
    # memory only -- data/alarm_config.json is left alone).
    alarm_config.set_for_tests(AlarmConfig(enabled=True, send_chart=True))

    # Clear any prior fire for SMOKE so re-runs always send a message.
    dedupe.reset()

    # Seed rows chosen so a bar with close=110 passes the SMA200 gate
    # and a bar with volume=10_000 pushes cum_volume well past the
    # ALARM_MIN_CUM_VOLUME default (100k).
    bar = _passing_bar()
    pool = _StubPool(
        sma200_rows=[{"symbolid": 999_999, "sma200": 100.0}],
        cum_volume_rows=[{"symbolid": 999_999, "cum_volume": 200_000}],
        # prev close 120 -> close 110 is -8.3%, inside the -5% Chg% ceiling.
        prev_close_rows=[{"symbolid": 999_999, "close": 120.0}],
        bars=_synthetic_session(bar.ts),
    )

    sink = dispatcher.build_alarm_sink(pool)
    await sink(bar)
    await alarm_generator.drain_pending()

    log.info("Smoke complete -- check your Telegram chat.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
