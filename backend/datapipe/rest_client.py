"""
Thin async client for the Massive/Polygon REST aggregates endpoint.

Only the endpoints the datapipe needs are wrapped:

  * ``fetch_intraday_bars(symbol, day)``   -> 1-min bars for a single ET
                                              trading date, all sessions
                                              (pre/regular/after).
  * ``fetch_daily_bars(symbol, days)``     -> N-day daily history (for ATR14
                                              warmup in the historian).

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
from backend.datapipe.schemas import RestAggregateBar, RestAggregateResponse
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
                    # Always INFO on 429 even though raise_for_status caught it
                    # would be nice, but 200s should be quieter. Rate-limit
                    # headers at INFO when the provider signals we're close.
                    logger.debug(
                        "[rest] <-- %d %s (%.2fs, %d bytes) headers=%s",
                        resp.status, target, elapsed, len(body_bytes), rl_hdrs,
                    )
                    if rl_hdrs:
                        # Surface rate-limit metadata at INFO once per request
                        # so throttling is visible even without DEBUG.
                        logger.info("[rest] rate-limit headers on %s: %s", target, rl_hdrs)

                data = json.loads(body_bytes)

            parsed = RestAggregateResponse.model_validate(data)
            out.extend(parsed.results)
            pages += 1
            if pages > 20:
                # Defensive: no legitimate query for our windows needs >20 pages.
                # This catches a pathological next_url loop.
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
        1-min bars for one ET session date. ``from`` and ``to`` share the
        date so we don't accidentally pull the neighbouring day.
        Massive treats these as calendar days in ET.
        """
        url = (
            f"{self._base_url}/v2/aggs/ticker/{symbol}"
            f"/range/1/minute/{date_to_iso(day)}/{date_to_iso(day)}"
        )
        return await self._paged_get(url, {"adjusted": "true", "sort": "asc", "limit": 50000})

    async def fetch_intraday_bars_range(
        self,
        symbol: str,
        start_day: date,
        end_day: date,
    ) -> list[RestAggregateBar]:
        """5-day intraday history for the RVOL baseline warmup path."""
        url = (
            f"{self._base_url}/v2/aggs/ticker/{symbol}"
            f"/range/1/minute/{date_to_iso(start_day)}/{date_to_iso(end_day)}"
        )
        return await self._paged_get(url, {"adjusted": "true", "sort": "asc", "limit": 50000})

    async def fetch_daily_bars(
        self,
        symbol: str,
        end_day: date,
        lookback_days: int = 20,
    ) -> list[RestAggregateBar]:
        """Daily bars ending on ``end_day`` -- feeds ATR14 in the historian."""
        start = end_day - timedelta(days=lookback_days * 2)  # buffer for weekends/holidays
        url = (
            f"{self._base_url}/v2/aggs/ticker/{symbol}"
            f"/range/1/day/{date_to_iso(start)}/{date_to_iso(end_day)}"
        )
        return await self._paged_get(url, {"adjusted": "true", "sort": "asc", "limit": 5000})
