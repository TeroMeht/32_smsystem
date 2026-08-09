"""
Central configuration for 32_smsystem.

Follows the same pattern as 22_WatchlistStreamer: pydantic-settings loads
from a centralized env-repo file so each project has one place for its
secrets, and local .env files stay out of the repo.

Every setting is REQUIRED — no defaults live here. Startup fails loudly
if the env file is missing a key, which is preferable to silently
inheriting a value nobody wrote down.

Env file location:  C:/codebase/env-repo/32_smsystem.env
"""

from datetime import date
from pathlib import Path

from pydantic import model_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # --- Database ---
    DATABASE_URL: str

    # --- Polygon.io ---
    POLYGON_API_KEY: str
    POLYGON_BASE_URL: str

    # --- Stock universe filter thresholds ---
    UNIVERSE_MIN_PRICE:       float
    UNIVERSE_MIN_MARKET_CAP:  int
    UNIVERSE_MIN_ADV_DOLLAR:  int
    UNIVERSE_LOOKBACK_DAYS:   int
    UNIVERSE_MIN_SAMPLE_DAYS: int

    # --- Concurrency for HTTP calls ---
    HTTP_WORKERS_TICKER_DETAILS: int
    HTTP_WORKERS_GROUPED_DAILY:  int

    # --- Runtime mode ---
    # MODE = "live"    : historian aligns to wall-clock today, WS livestream
    #                    opens in the background.
    # MODE = "replay"  : historian treats REPLAY_DAY as "today", RVOL baseline
    #                    is built from sessions strictly before REPLAY_DAY,
    #                    and startup runs the replay driver INSTEAD of the WS.
    # REPLAY_* fields are still required in live mode -- pick placeholder
    # values you're comfortable with, they simply aren't consulted.
    MODE: str
    REPLAY_DAY: date
    REPLAY_SPEED: float
    REPLAY_LOOKBACK_DAYS: int
    REPLAY_SAMPLE_SESSIONS: int

    @model_validator(mode="after")
    def _normalize_mode(self) -> "Settings":
        mode = self.MODE.lower()
        if mode not in ("live", "replay"):
            raise ValueError(f"MODE must be 'live' or 'replay' (got {self.MODE!r})")
        self.MODE = mode
        return self

    class Config:
        ENV_REPO = Path("C:/codebase/env-repo")
        env_file = ENV_REPO / "32_smsystem.env"
        env_file_encoding = "utf-8"
        case_sensitive = True


settings = Settings()
