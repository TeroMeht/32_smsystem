"""
datapipe -- 1-min bar ingestion for 32_smsystem.

Two entry points, one canonical bar shape:

  * historian.backfill_symbols(...)  -- REST warmup on startup
  * livestream.run_livestream(...)   -- WS /stocks/AM consumer

Both funnel bars through the same schemas.Bar dataclass and the same
calculations layer (calculations.py) before persisting via
backend.database.writers.

All SQL / DB access lives in backend.database.* -- this package contains
zero raw SQL and no direct asyncpg calls.
"""
