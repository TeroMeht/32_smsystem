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
import logging
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

    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None):
        self._api_key = api_key or settings.POLYGON_API_KEY
        self._base_url = (base_url or settings.POLYGON_BASE_URL).rstrip("/")
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
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
        """
        session = await self._get_session()
        out: list[RestAggregateBar] = []
        next_url: Optional[str] = None

        while True:
            if next_url is None:
                async with session.get(url, params={**params, "apiKey": self._api_key}) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
            else:
                async with session.get(next_url, params={"apiKey": self._api_key}) as resp:
                    resp.raise_for_status()
                    data = await resp.json()

            parsed = RestAggregateResponse.model_validate(data)
            out.extend(parsed.results)
            if not parsed.next_url:
                break
            next_url = parsed.next_url

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
