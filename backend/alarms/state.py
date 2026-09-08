"""
In-memory session state for the alarms layer.

Two things the ``CandleRow`` carries no field for but the frontend's
Uptrend Reversals filter set (relatr.html) uses:

    * ``sma200`` -- daily-cadence value from ``daily_indicators``. Loaded
      once from the DB, keyed by ``symbolid``. Missing entry means the
      symbol has fewer than SMA200_SAMPLE_SESSIONS of daily history --
      the frontend treats that as "trend unknown -> drop", we do the
      same when the ``ALARM_REQUIRE_ABOVE_SMA200`` gate is on.

    * ``cum_volume`` -- running sum of ``livestream.volume`` since
      session start. Seeded from the DB on FIRST BAR (lazy), then
      incremented per-bar in the sink so it stays in lockstep with the
      /api/livestream/top payload the frontend reads.

Seeding is deferred to the first bar rather than done at ``main`` /
``pipeline.startup`` time because ``pipeline.startup`` empties
``livestream`` up front and REST-prime + bulk_persist run INSIDE the
background live task -- so at startup time the table is empty and
seeding would yield all zeros. By the time the first WS bar reaches
the sink, ``_initialize_livestream`` has already run and the table
holds today's primed bars.

Only one WS consumer task feeds the sink, so seeding is naturally
serialized with no lock needed.
"""

from __future__ import annotations

import logging
from typing import Optional

import asyncpg

from backend.database.readers import (
    load_cum_volume_map,
    load_latest_sma200_map,
)
from indicators.candle_row import CandleRow

logger = logging.getLogger(__name__)


class AlarmState:
    """Session-scoped state the alarms layer reads on every bar."""

    __slots__ = ("_pool", "_seeded", "_sma200_by_sid", "_cum_volume_by_sid")

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self._seeded = False
        self._sma200_by_sid: dict[int, float] = {}
        self._cum_volume_by_sid: dict[int, int] = {}

    async def ensure_seeded(self) -> None:
        """
        One-shot lazy seed. Called by the sink before it processes each
        bar; the second call onward is a cheap boolean check. Loads
        SMA200 and session cum_volume from the DB.
        """
        if self._seeded:
            return
        self._sma200_by_sid = await load_latest_sma200_map(self._pool)
        self._cum_volume_by_sid = await load_cum_volume_map(self._pool)
        self._seeded = True
        logger.info(
            "AlarmState seeded: %d symbols with sma200, %d symbols with cum_volume",
            len(self._sma200_by_sid), len(self._cum_volume_by_sid),
        )

    def add_bar(self, bar: CandleRow) -> None:
        """Fold this bar's volume into the running per-symbol sum."""
        vol = int(bar.volume) if bar.volume is not None else 0
        self._cum_volume_by_sid[bar.symbolid] = (
            self._cum_volume_by_sid.get(bar.symbolid, 0) + vol
        )

    def sma200(self, bar: CandleRow) -> Optional[float]:
        """Latest SMA200 for this symbol, or ``None`` if warm-up short."""
        return self._sma200_by_sid.get(bar.symbolid)

    def cum_volume(self, bar: CandleRow) -> int:
        """Session cumulative volume for this symbol (0 if unseen)."""
        return self._cum_volume_by_sid.get(bar.symbolid, 0)
