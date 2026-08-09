"""
The single entrypoint that turns a raw incoming Bar1m into an enriched,
persisted bar. Used by BOTH livestream.py and replay.py so behavior is
identical between them.

Steps:
  1. Look up or create the per-symbol SymbolSessionState.
  2. Compute the ET bar_time slot; grab that slot's rvol baseline
     (already cumulative-through-this-slot from the rvol_baseline table).
  3. Enrich the bar with VWAP/EMA9/RelATR/RVOL cumulative.
  4. Append the enriched bar to session history.
  5. Persist to livestream + intraday_bars (concurrent).
  6. Emit the enriched bar via an optional callback -- this is the hook the
     service layer plugs into to run strategies + push SSE events.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, Optional

import asyncpg

from backend.database.writers import insert_intraday_bar, insert_livestream_bar
from backend.datapipe.calculations import enrich_bar
from backend.datapipe.schemas import Bar1m
from backend.datapipe.session_state import SessionStore, SymbolSessionState
from backend.datapipe.time_utils import et_time_slot, session_date_et

logger = logging.getLogger(__name__)


# Callback signature: async fn taking the enriched Bar1m. Return value ignored.
BarSink = Callable[[Bar1m], Awaitable[None]]


async def process_bar(
    pool: asyncpg.Pool,
    store: SessionStore,
    bar: Bar1m,
    sink: Optional[BarSink] = None,
) -> Bar1m:
    """
    Enrich + persist one incoming bar. Returns the enriched Bar1m so
    callers doing synchronous follow-up work (unit tests, replay) can see
    the same object the sink saw.
    """
    st = store.get_or_init(bar.symbol, bar.symbolid, session_date_et(bar.ts))

    slot = et_time_slot(bar.ts)
    slot_avg = st.baseline_for_slot(slot)

    # Snapshot the RVOL inputs BEFORE enrichment so the log below reflects
    # what actually went into the calculation.
    prior_vol_sum = sum(b.volume for b in st.history)
    prior_baseline_sum = st.baseline_history_sum
    today_cum_vol = prior_vol_sum + bar.volume
    cum_baseline = prior_baseline_sum + slot_avg

    # First bar for this symbol this session -- one-off header log so it's
    # easy to spot in a mixed-symbol stream.
    if not st.history:
        logger.info(
            "[calc] %s: session opened (session_date=%s, atr=%s, baseline_slots=%d)",
            bar.symbol, st.session_date, st.atr, len(st.rvol_baseline),
        )

    enriched = enrich_bar(
        new_bar=bar,
        history=st.history,
        atr=st.atr,
        baseline_slot_avg=slot_avg,
        baseline_history_sum=prior_baseline_sum,
    )

    # Per-bar calculation trace. Two-line format keeps it scannable:
    #   line 1 -- raw inputs the bar arrived with
    #   line 2 -- the four indicators and the RVOL denominator breakdown
    et_ts = et_time_slot(bar.ts)
    logger.info(
        "[calc] %s %s ET | O=%.4f H=%.4f L=%.4f C=%.4f V=%d "
        "| today_cum_vol=%d",
        bar.symbol, et_ts.strftime("%H:%M"),
        bar.open, bar.high, bar.low, bar.close, bar.volume,
        int(today_cum_vol),
    )
    logger.info(
        "[calc] %s %s ET | VWAP=%s EMA9=%s RelATR=%s RVOL=%s "
        "| slot_avg=%.1f prior_sum=%.1f denom=%.1f  atr=%s",
        bar.symbol, et_ts.strftime("%H:%M"),
        _fmt(enriched.vwap), _fmt(enriched.ema9),
        _fmt(enriched.relatr), _fmt(enriched.rvol_cum),
        slot_avg, prior_baseline_sum, cum_baseline, _fmt(st.atr),
    )

    # Persist first; the sink shouldn't see a bar that isn't in the DB yet.
    await asyncio.gather(
        insert_livestream_bar(pool, enriched),
        insert_intraday_bar(pool, enriched),
    )

    st.history.append(enriched)
    # Advance the running baseline sum so the NEXT bar sees this slot's
    # per-bar average included in its denominator.
    st.baseline_history_sum += slot_avg

    if sink is not None:
        try:
            await sink(enriched)
        except Exception:
            logger.exception("bar sink failed for %s @ %s", enriched.symbol, enriched.ts)

    return enriched


def _fmt(v):
    """Compact float formatting for the trace log; keeps None readable."""
    if v is None:
        return "None"
    return f"{v:.4f}"
