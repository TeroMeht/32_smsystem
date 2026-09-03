
from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from typing import Iterable

import asyncpg
import pandas as pd

from indicators.rvol import avg_volume_model

from backend.database.readers import (
    load_active_symbol_map,
    load_intraday_bars_for_rvol,
)
from backend.database.writers import bulk_replace_rvol_baseline
from backend.datapipe.time_utils import ET, HELSINKI

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pure compute steps
# ---------------------------------------------------------------------------


def project_bars(raw_bars: Iterable[dict]) -> pd.DataFrame:
    """
    Turn ``[{"symbolid", "ts" (UTC tz-aware), "volume"}]`` into a
    DataFrame tagged with:

      * ``symbolid``     -- unchanged
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
    et_ts  = df["ts"].dt.tz_convert(ET)
    hki_ts = df["ts"].dt.tz_convert(HELSINKI)
    df["session_date"] = et_ts.dt.date
    df["bar_time"]     = [time(t.hour, t.minute) for t in hki_ts]
    return df[["symbolid", "session_date", "bar_time", "volume"]]


def pick_recent_sessions(df: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    Keep only rows whose ``session_date`` is in the N most-recent
    distinct session_dates for that symbol. If a symbol has fewer than
    N session_dates on disk, all of them stay.
    """
    if df.empty:
        return df
    distinct = df[["symbolid", "session_date"]].drop_duplicates()
    distinct = distinct.sort_values(["symbolid", "session_date"], ascending=[True, False])
    distinct["rank"] = distinct.groupby("symbolid").cumcount() + 1
    recent = distinct[distinct["rank"] <= n][["symbolid", "session_date"]]
    return df.merge(recent, on=["symbolid", "session_date"], how="inner")


def _build_writer_rows(recent: pd.DataFrame) -> list[tuple[int, time, float, int]]:
    """
    Given the recent-sessions frame (columns symbolid / session_date /
    bar_time / volume), produce writer-ready tuples
    ``(symbolid, bar_time, avg_volume, sample_days)``.

    ``avg_volume`` comes from ``indicators.rvol.avg_volume_model``
    (winsorized mean over present sessions).
    ``sample_days`` is computed alongside from the same recent frame --
    kept as an operator diagnostic on the DB row.
    """
    if recent.empty:
        return []

    # Adapt column names to what avg_volume_model expects
    # (symbol/date/time/volume). ``symbolid`` (int) rides in as
    # ``symbol`` -- indicators only uses it as a grouping key, so the
    # int is fine and comes back unchanged.
    adapted = recent.rename(
        columns={"symbolid": "symbol", "session_date": "date", "bar_time": "time"},
    )
    baseline = avg_volume_model(adapted)                # columns: symbol, time, avg_volume
    baseline = baseline.rename(
        columns={"symbol": "symbolid", "time": "bar_time"},
    )

    # Diagnostic: how many distinct session_dates contributed to each slot.
    sample_days = (
        recent.groupby(["symbolid", "bar_time"], as_index=False)
              .agg(sample_days=("session_date", "nunique"))
    )

    joined = baseline.merge(sample_days, on=["symbolid", "bar_time"], how="left")
    joined["sample_days"] = joined["sample_days"].fillna(0).astype(int)

    return [
        (int(r.symbolid), r.bar_time, float(r.avg_volume), int(r.sample_days))
        for r in joined.itertuples(index=False)
    ]


# ---------------------------------------------------------------------------
# Top-level orchestrator
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

    ``end_day`` is exclusive -- the baseline must not include the
    target day itself. For live use, pass ``today``.

    Reads inputs via ``database.readers``, runs the pure compute
    pipeline defined in this module (which delegates the per-slot
    mean to ``indicators.rvol.avg_volume_model``), then hands the
    result to ``database.writers.bulk_replace_rvol_baseline``.
    """
    start_day = end_day - timedelta(days=lookback_days)
    start_utc = datetime.combine(start_day, datetime.min.time())
    end_utc   = datetime.combine(end_day,   datetime.min.time())

    logger.info("Rvol model rebuild: window [%s, %s]", start_day, end_day)

    # 1. Fetch inputs.
    raw_bars   = await load_intraday_bars_for_rvol(pool, start_utc, end_utc)
    symbol_map = await load_active_symbol_map(pool)

    # 2. Pure compute pipeline: project -> pick_recent -> build_avg_volume.
    projected = project_bars(raw_bars)
    recent    = pick_recent_sessions(projected, sample_sessions)
    rows      = _build_writer_rows(recent)

    # 3. Persist.
    await bulk_replace_rvol_baseline(pool, rows)

    logger.info(
        "Rvol model rebuild complete -- %d rows across %d symbols "
        "(source intraday bars: %d rows)",
        len(rows), len(symbol_map), len(raw_bars),
    )
