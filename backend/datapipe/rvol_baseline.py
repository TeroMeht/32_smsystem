"""
rvol_baseline table rebuild -- DB in, ``indicators`` math, DB out.

The compute steps (project -> pick recent sessions -> winsorized
per-slot mean + sample_days) live in ``indicators.rvol_baseline``;
this module only loads the inputs and persists the result.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

import asyncpg

from indicators.rvol_baseline import (
    baseline_with_sample_days,
    pick_recent_sessions,
    project_bars,
)

from backend.database.readers import (
    load_active_symbol_map,
    load_intraday_bars_for_rvol,
)
from backend.database.writers import bulk_replace_rvol_baseline
from backend.datapipe.time_utils import ET, HELSINKI

logger = logging.getLogger(__name__)


async def rebuild_rvol_model(
    pool: asyncpg.Pool,
    end_day: date,
    lookback_days: int,
    sample_sessions: int,
) -> None:
    """
    Full rebuild of the ``rvol_baseline`` table from intraday_bars in
    the calendar window ``[end_day - lookback_days, end_day)``.

    ``end_day`` is exclusive -- the baseline must not include the
    target day itself. For live use, pass ``today``.

    Sessions are grouped on the US (ET) calendar date; slots are keyed
    by Helsinki HH:MM to match ``rvol_baseline.bar_time``.
    """
    start_day = end_day - timedelta(days=lookback_days)
    start_utc = datetime.combine(start_day, datetime.min.time())
    end_utc   = datetime.combine(end_day,   datetime.min.time())

    logger.info("Rvol model rebuild: window [%s, %s]", start_day, end_day)

    # 1. Fetch inputs.
    raw_bars   = await load_intraday_bars_for_rvol(pool, start_utc, end_utc)
    symbol_map = await load_active_symbol_map(pool)

    # 2. Compute (indicators package).
    projected = project_bars(raw_bars, session_tz=ET, slot_tz=HELSINKI)
    recent    = pick_recent_sessions(projected, sample_sessions)
    baseline  = baseline_with_sample_days(recent)
    rows = [
        (int(r.symbolid), r.bar_time, float(r.avg_volume), int(r.sample_days))
        for r in baseline.itertuples(index=False)
    ]

    # 3. Persist.
    await bulk_replace_rvol_baseline(pool, rows)

    logger.info(
        "Rvol model rebuild complete -- %d rows across %d symbols "
        "(source intraday bars: %d rows)",
        len(rows), len(symbol_map), len(raw_bars),
    )
