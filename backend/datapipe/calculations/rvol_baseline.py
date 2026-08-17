"""
RVOL baseline model.

One module owns the whole story of the ``rvol_baseline`` table:

    fetch  -> project  -> pick_recent  -> sum_per_slot  -> full_grid  -> write

Each step is a small pure function on pandas so the pipeline reads
top-to-bottom. ``rebuild_rvol_model`` is the orchestrator called by the
historian after intraday backfill lands new rows.

Shape of the persisted table:
  * One row per (symbolid, Helsinki bar_time) for EVERY 2-min slot of
    the 24-hour day (720 rows / active symbol). Slots that had no bars
    in the recent sessions still get a row with ``avg_volume = 0``.
  * ``avg_volume`` = ``vol_sum / sample_sessions`` -- FIXED denominator,
    so a slot that fired on only 3 of the 5 recent sessions still
    divides by 5. Missing days count as zero.
  * ``sample_days`` = how many of those N sessions actually contributed
    a bar to this slot (0..N), kept as an operator diagnostic.

Timezones:
  * session_date grouping is in ET (US session grid is ET-native)
  * bar_time slot key is in Helsinki (matches display everywhere else)

The two knobs both come from settings via the historian:
  * ``lookback_days``   -- calendar days back to search for source data
  * ``sample_sessions`` -- N: trading sessions averaged, AND the
                           denominator for avg_volume
"""

from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from typing import Iterable

import asyncpg
import pandas as pd

from backend.database.readers import (
    load_active_symbol_map,
    load_intraday_bars_for_rvol,
)
from backend.database.writers import bulk_replace_rvol_baseline
from backend.datapipe.time_utils import ET, HELSINKI, helsinki_2min_slots

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# pure compute steps
# ---------------------------------------------------------------------------

def project_bars(raw_bars: Iterable[dict]) -> pd.DataFrame:
    """
    Turn ``[{"symbolid", "ts" (UTC tz-aware), "volume"}]`` into a DataFrame
    tagged with:

      * ``session_date`` -- ET calendar date (US session grid)
      * ``bar_time``     -- Helsinki HH:MM as a ``datetime.time``
      * ``volume``       -- raw volume

    Empty input returns an empty frame with the right columns so the
    downstream chain doesn't need special-case handling.
    """
    rows = list(raw_bars)
    if not rows:
        return pd.DataFrame(columns=["symbolid", "session_date", "bar_time", "volume"])

    df = pd.DataFrame(rows)
    et_ts = df["ts"].dt.tz_convert(ET)
    hki_ts = df["ts"].dt.tz_convert(HELSINKI)
    df["session_date"] = et_ts.dt.date
    df["bar_time"] = [time(t.hour, t.minute) for t in hki_ts]
    return df[["symbolid", "session_date", "bar_time", "volume"]]


def pick_recent_sessions(df: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    Keep only rows whose ``session_date`` is in the N most-recent
    distinct session_dates for that symbol. If a symbol has fewer than
    N session_dates on disk, all of them stay.
    """
    if df.empty:
        return df
    # Rank distinct session_dates per symbol, freshest = 1.
    distinct = df[["symbolid", "session_date"]].drop_duplicates()
    distinct = distinct.sort_values(["symbolid", "session_date"], ascending=[True, False])
    distinct["rank"] = distinct.groupby("symbolid").cumcount() + 1
    recent = distinct[distinct["rank"] <= n][["symbolid", "session_date"]]
    return df.merge(recent, on=["symbolid", "session_date"], how="inner")


def sum_volume_per_slot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group by ``(symbolid, bar_time)`` and produce:
      * ``vol_sum``     -- total volume across the recent sessions
      * ``sample_days`` -- how many distinct sessions actually contributed
                          a bar at that slot (0..N)
    """
    if df.empty:
        return pd.DataFrame(columns=["symbolid", "bar_time", "vol_sum", "sample_days"])
    agg = df.groupby(["symbolid", "bar_time"], as_index=False).agg(
        vol_sum=("volume", "sum"),
        sample_days=("session_date", "nunique"),
    )
    return agg


def full_grid_rows(
    agg: pd.DataFrame,
    symbolids: Iterable[int],
    n_sessions: int,
) -> list[tuple[int, time, float, int]]:
    """
    Cross-join every active symbol with every 2-min Helsinki slot, join
    the aggregated volumes on top, and produce writer-ready tuples:
    ``(symbolid, bar_time, avg_volume, sample_days)``.

    ``avg_volume`` = ``vol_sum / n_sessions`` (FIXED denominator so a
    slot that appeared in only 3 of 5 sessions still divides by 5 --
    missing days count as zero). ``sample_days`` = observed count.
    """
    sids = list(symbolids)
    slots = helsinki_2min_slots()
    grid = pd.MultiIndex.from_product(
        [sids, slots], names=["symbolid", "bar_time"]
    ).to_frame(index=False)
    joined = grid.merge(agg, on=["symbolid", "bar_time"], how="left")
    joined["vol_sum"] = joined["vol_sum"].fillna(0)
    joined["sample_days"] = joined["sample_days"].fillna(0).astype(int)
    joined["avg_volume"] = (joined["vol_sum"] / float(n_sessions)).round(2)
    return [
        (int(r.symbolid), r.bar_time, float(r.avg_volume), int(r.sample_days))
        for r in joined.itertuples(index=False)
    ]


# ---------------------------------------------------------------------------
# top-level orchestrator
# ---------------------------------------------------------------------------


async def rebuild_rvol_model(
    pool: asyncpg.Pool,
    end_day: date,
    lookback_days: int,
    sample_sessions: int,
) -> None:
    """
    Full rebuild of the ``rvol_baseline`` table from intraday_bars in
    the calendar window ``[end_day - lookback_days, end_day)``.

    ``end_day`` is exclusive (matches replay semantics: baseline must
    not include the target day itself). For live use, pass ``today``.

    Reads inputs via ``database.readers``, runs the pure compute
    pipeline defined in this module, then hands the result to
    ``database.writers.bulk_replace_rvol_baseline``.
    """
    start_day = end_day - timedelta(days=lookback_days)
    start_utc = datetime.combine(start_day, datetime.min.time())
    end_utc   = datetime.combine(end_day,   datetime.min.time())

    logger.info("Rvol model rebuild: window [%s, %s]", start_day, end_day)

    # 1. Fetch inputs.
    raw_bars   = await load_intraday_bars_for_rvol(pool, start_utc, end_utc)
    symbol_map = await load_active_symbol_map(pool)
    active_ids = list(symbol_map.values())

    # 2. Pure compute pipeline: project -> pick_recent -> sum_per_slot -> full_grid.
    projected = project_bars(raw_bars)
    recent    = pick_recent_sessions(projected, sample_sessions)
    per_slot  = sum_volume_per_slot(recent)
    rows      = full_grid_rows(per_slot, active_ids, sample_sessions)

    # 3. Persist.
    await bulk_replace_rvol_baseline(pool, rows)

    logger.info(
        "Rvol model rebuild complete -- %d rows across %d symbols "
        "(source intraday bars: %d rows)",
        len(rows), len(active_ids), len(raw_bars),
    )
