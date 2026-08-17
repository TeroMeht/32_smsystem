"""
Thin async client for the Massive/Polygon REST aggregates endpoint.

Only the endpoints the datapipe needs are wrapped:

  * ``fetch_intraday_bars(symbol, day)``    -> aggregation-cadence bars
                                               (BAR_MINUTES/minute) for a
                                               single ET session date.
  * ``fetch_intraday_bars_range(symbol,     -> same, over an inclusive
     start_day, end_day)``                     multi-day window.
  * ``fetch_daily_bars_range(symbol,        -> daily history for ATR14
     start_day, end_day)``                     warmup / incremental catch-up
                                               in the historian.

Intraday cadence is delegated to Polygon (we request ``/range/N/minute/...``
directly with N = BAR_MINUTES), so callers get already-aggregated bars and
no client-side aggregation is required on the historian or REST-prime paths.

Fetch functions own their URL + params, hand them to ``_get_bars`` for the
actual HTTP round-trip, then deliver the bars back to their caller. ``limit``
is sized above the worst-case row count for each window, so a single
request returns everything; if Polygon ever hands back a ``next_url``,
``_get_bars`` raises loudly rather than silently truncating.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import date


from backend.datapipe.schemas import BAR_MINUTES, RestAggregateBar, RestAggregateResponse
from backend.datapipe.time_utils import date_to_iso
from backend.dependencies import RestClient


logger = logging.getLogger(__name__)


async def _get_bars(client: RestClient, url: str, params: dict) -> list[RestAggregateBar]:
    """
    One GET against a Polygon aggregates endpoint. Parses the response
    as ``RestAggregateResponse`` and returns the flat bar list.

    Fails loud (RuntimeError) if the response carries a ``next_url`` --
    callers' ``limit`` is sized to hold the whole window, so pagination
    should never fire; if it does, bump the limit rather than silently
    returning a partial page.

    Logs elapsed time, byte count, and any rate-limit headers so slow
    calls / 429s are visible to operators.
    """
    page_params = {**params, "apiKey": client.api_key}

    t0 = time.monotonic()
    async with client.session.get(url, params=page_params) as resp:
        body_bytes = await resp.read()
        elapsed = time.monotonic() - t0

        # Rate-limit headers vary by provider; log whichever are present.
        rl_hdrs = {
            k: v for k, v in resp.headers.items()
            if k.lower().startswith("x-ratelimit") or k.lower() == "retry-after"
        }

        if resp.status >= 400:
            logger.warning(
                "[rest] <-- %d %s (%.2fs, %d bytes) headers=%s body=%s",
                resp.status, url, elapsed, len(body_bytes),
                rl_hdrs, body_bytes[:400].decode("utf-8", errors="replace"),
            )
            resp.raise_for_status()

        logger.debug(
            "[rest] <-- %d %s (%.2fs, %d bytes) headers=%s",
            resp.status, url, elapsed, len(body_bytes), rl_hdrs,
        )
        if rl_hdrs:
            logger.info("[rest] rate-limit headers on %s: %s", url, rl_hdrs)

        data = json.loads(body_bytes)

    parsed = RestAggregateResponse.model_validate(data)
    if parsed.next_url:
        raise RuntimeError(
            f"[rest] {url} returned next_url -- request exceeded 'limit'. "
            f"Bump the limit or shrink the window."
        )
    return parsed.results


async def fetch_intraday_bars(
    client: RestClient,
    symbol: str,
    day: date,
) -> list[RestAggregateBar]:
    """
    Intraday bars for one ET session date at the aggregation cadence
    (BAR_MINUTES). ``from`` and ``to`` share the date so we don't
    accidentally pull the neighbouring day. Massive treats these as
    calendar days in ET.
    """
    url = (
        f"{client.base_url}/v2/aggs/ticker/{symbol}"
        f"/range/{BAR_MINUTES}/minute/{date_to_iso(day)}/{date_to_iso(day)}"
    )
    params = {"adjusted": "true", "sort": "asc", "limit": 50000}
    bars = await _get_bars(client, url, params)
    return bars


async def fetch_intraday_bars_range(
    client: RestClient,
    symbol: str,
    start_day: date,
    end_day: date,
) -> list[RestAggregateBar]:
    """
    Multi-day intraday history at the aggregation cadence (BAR_MINUTES).
    Feeds the historian; Polygon returns the N-min aggregates directly,
    so no client-side batch aggregation is needed on this path.
    """
    url = (
        f"{client.base_url}/v2/aggs/ticker/{symbol}"
        f"/range/{BAR_MINUTES}/minute/{date_to_iso(start_day)}/{date_to_iso(end_day)}"
    )
    params = {"adjusted": "true", "sort": "asc", "limit": 50000}
    bars = await _get_bars(client, url, params)
    return bars


async def fetch_daily_bars_range(
    client: RestClient,
    symbol: str,
    start_day: date,
    end_day: date,
) -> list[RestAggregateBar]:
    """Daily bars in ``[start_day, end_day]`` -- feeds ATR14 in the historian."""
    url = (
        f"{client.base_url}/v2/aggs/ticker/{symbol}"
        f"/range/1/day/{date_to_iso(start_day)}/{date_to_iso(end_day)}"
    )
    params = {"adjusted": "true", "sort": "asc", "limit": 5000}
    bars = await _get_bars(client, url, params)
    return bars
