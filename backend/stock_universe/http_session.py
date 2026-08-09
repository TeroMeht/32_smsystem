"""
Shared Polygon HTTP session with a properly sized connection pool.

Every stock_universe script needs a `requests.Session` configured with
the API key and a pool large enough for its worker count — this keeps
that setup in one place.
"""

import requests
from requests.adapters import HTTPAdapter

from backend.core.config import settings


def build_session(pool_size: int) -> requests.Session:
    """
    Create a Session pre-loaded with the Polygon API key and a
    connection pool sized for `pool_size` concurrent workers.
    """
    session = requests.Session()
    session.params = {                             # type: ignore[assignment]
        "apiKey": settings.POLYGON_API_KEY,
        "adjusted": "true",
    }
    adapter = HTTPAdapter(
        pool_connections=pool_size,
        pool_maxsize=pool_size,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session
