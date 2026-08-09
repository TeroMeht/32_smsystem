"""
datapipe -- 1-min bar ingestion for 32_smsystem.

Three entry points, one canonical bar shape:

  * historian.backfill_symbols(...)  -- REST warmup on startup
  * livestream.run_livestream(...)   -- WS /stocks/AM consumer
  * replay.run_replay(...)           -- REST-driven step-through of a past day

All three funnel bars through the same schemas.Bar1m dataclass and the
same calculations layer (calculations.py) before persisting via
backend.database.writers. This keeps live and replay behaviorally identical.

All SQL / DB access lives in backend.database.* -- this package contains
zero raw SQL and no direct asyncpg calls.
"""
