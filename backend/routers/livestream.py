"""
Routes serving the /relatr dashboard's polling API.

Every persistence call is delegated to ``backend.database.readers`` -- no
SQL lives here.
"""

from __future__ import annotations

from fastapi import APIRouter

from backend.dependencies import get_pool
from backend.database.readers import (
    load_latest_livestream_per_symbol,
    load_livestream_bars_for_symbol,
)

router = APIRouter(prefix="/api/livestream", tags=["livestream"])


@router.get("/top")
async def top():
    """
    Latest livestream row per symbol -- ALL rows, unfiltered, unsorted.

    Display concerns (volume floor, RVOL floor, RelATR floor, sort order,
    row cap) live entirely in the frontend so they can be tweaked without
    touching backend code.
    """
    pool = get_pool()
    rows = await load_latest_livestream_per_symbol(pool)
    return {"rows": rows}


@router.get("/bars/{symbol}")
async def bars(symbol: str):
    """
    Every livestream row currently on disk for the symbol, ordered by ts.
    Since livestream is truncated at session start, this is the current
    session in progress -- feeds the frontend candlestick chart on hover.
    """
    pool = get_pool()
    rows = await load_livestream_bars_for_symbol(pool, symbol.upper())
    return {"symbol": symbol.upper(), "bars": rows}
