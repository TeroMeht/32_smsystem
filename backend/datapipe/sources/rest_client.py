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
next_url pagination is handled here so callers see a flat list.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import date, timedelta
from typing import Optional

import aiohttp

from backend.core.config import settings
from backend.datapipe.schemas import BAR_MINUTES, RestAggregateBar, RestAggregateResponse
from backend.datapipe.time_utils import date_to_iso

logger = logging.getLogger(__name__)


class RestClient:
    """
    One-per-process aiohttp session wrapper. Instantiate at app startup and
    hand around; ``close()`` from lifespan shutdown.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        request_timeout_s: float = 30.0,
    ):
        self._api_key = api_key or settings.POLYGON_API_KEY
        self._base_url = (base_url or settings.POLYGON_BASE_URL).rstrip("/")
        self._session: Optional[aiohttp.ClientSession] = None
        # A hard per-request timeout is critical -- without it a single
        # slow/hung symbol can pin a worker slot forever (the historian's
        # bounded semaphore then wedges the whole backfill).
        self._timeout = aiohttp.ClientTimeout(total=request_timeout_s)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    # -----------------------------------------------------------------------
    # aggregates
    # -----------------------------------------------------------------------

    async def _paged_get(self, url: str, params: dict) -> list[RestAggregateBar]:
        """
        GET url + follow next_url until exhausted. next_url comes back
        pre-formed and already carries the cursor -- we only need to add the
        apiKey each hop.

        Logs every request/response so operators can diagnose rate limits,
        pagination loops, and slow endpoints. Rate-limit headers if present
        (X-RateLimit-*, Retry-After) are surfaced at INFO on any non-2xx.
        """
        session = await self._get_session()
        out: list[RestAggregateBar] = []
        next_url: Optional[str] = None
        pages = 0

        while True:
            target = next_url or url
            page_params = {"apiKey": self._api_key} if next_url else {**params, "apiKey": self._api_key}
            safe_params = {k: v for k, v in page_params.items() if k != "apiKey"}
            logger.debug("[rest] --> GET %s params=%s (page=%d)", target, safe_params, pages + 1)

            t0 = time.monotonic()
            async with session.get(target, params=page_params) as resp:
                body_bytes = await resp.read()
                elapsed = time.monotonic() - t0

                # Rate-limit headers vary by provider; log whichever are present
                rl_hdrs = {
                    k: v for k, v in resp.headers.items()
                    if k.lower().startswith("x-ratelimit") or k.lower() == "retry-after"
                }

                if resp.status >= 400:
                    logger.warning(
                        "[rest] <-- %d %s (%.2fs, %d bytes) headers=%s body=%s",
                        resp.status, target, elapsed, len(body_bytes),
                        rl_hdrs, body_bytes[:400].decode("utf-8", errors="replace"),
                    )
                    resp.raise_for_status()
                else:
                    logger.debug(
                        "[rest] <-- %d %s (%.2fs, %d bytes) headers=%s",
                        resp.status, target, elapsed, len(body_bytes), rl_hdrs,
                    )
                    if rl_hdrs:
                        logger.info("[rest] rate-limit headers on %s: %s", target, rl_hdrs)

                data = json.loads(body_bytes)

            parsed = RestAggregateResponse.model_validate(data)
            out.extend(parsed.results)
            pages += 1
            if pages > 20:
                logger.error("[rest] pagination cutoff at page 20 for %s -- aborting", url)
                break
            if not parsed.next_url:
                break
            next_url = parsed.next_url

        if pages > 1:
            logger.info("[rest] %s completed after %d pages, %d rows", url, pages, len(out))
        return out

    async def fetch_intraday_bars(
        self,
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
            f"{self._base_url}/v2/aggs/ticker/{symbol}"
            f"/range/{BAR_MINUTES}/minute/{date_to_iso(day)}/{date_to_iso(day)}"
        )
        return await self._paged_get(url, {"adjusted": "true", "sort": "asc", "limit": 50000})

    async def fetch_intraday_bars_range(
        self,
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
            f"{self._base_url}/v2/aggs/ticker/{symbol}"
            f"/range/{BAR_MINUTES}/minute/{date_to_iso(start_day)}/{date_to_iso(end_day)}"
        )
        return await self._paged_get(url, {"adjusted": "true", "sort": "asc", "limit": 50000})

    async def fetch_daily_bars_range(
        self,
        symbol: str,
        start_day: date,
        end_day: date,
    ) -> list[RestAggregateBar]:
        """Daily bars in ``[start_day, end_day]`` -- feeds ATR14 in the historian."""
        url = (
            f"{self._base_url}/v2/aggs/ticker/{symbol}"
            f"/range/1/day/{date_to_iso(start_day)}/{date_to_iso(end_day)}"
        )
        return await self._paged_get(url, {"adjusted": "true", "sort": "asc", "limit": 5000})
