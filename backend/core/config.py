"""
Central configuration for 32_smsystem.

Follows the same pattern as 22_WatchlistStreamer: pydantic-settings loads
from a centralized env-repo file so each project has one place for its
secrets, and local .env files stay out of the repo.

Every setting is REQUIRED -- no defaults live here. Startup fails loudly
if the env file is missing a key, which is preferable to silently
inheriting a value nobody wrote down.

Env file location:  C:/codebase/env-repo/32_smsystem.env
"""

from pathlib import Path

from pydantic_settings import BaseSettings
from data_sources.polygon._config import PolygonSourceConfig

class Settings(PolygonSourceConfig, BaseSettings):
    # --- Database ---
    DATABASE_URL: str

    # --- Stock universe filter thresholds ---
    UNIVERSE_MIN_PRICE:       float
    UNIVERSE_MIN_MARKET_CAP:  int
    UNIVERSE_MIN_ADV_DOLLAR:  int
    UNIVERSE_LOOKBACK_DAYS:   int
    UNIVERSE_MIN_SAMPLE_DAYS: int

    # --- Concurrency for HTTP calls ---
    HTTP_WORKERS_TICKER_DETAILS: int
    HTTP_WORKERS_GROUPED_DAILY:  int


    INTRADAY_BACKFILL_DAYS: int
    DAILY_BACKFILL_DAYS:    int
    RVOL_SAMPLE_SESSIONS:   int
    ATR_SAMPLE_SESSIONS:    int
    SMA200_SAMPLE_SESSIONS: int

    # # --- Alarms / Telegram ---
    # # Same env-var names as 22_WatchlistStreamer so the bot token can be
    # # copied across projects without renaming keys.
    # TELEGRAM_BOT_TOKEN: str
    # TELEGRAM_CHAT_ID:   str

    # # Uptrend Reversals filter set -- mirrors the same-named filters in

    # ALARM_MIN_RELATR:           float
    # ALARM_MIN_VOLUME:           int
    # ALARM_MIN_CUM_VOLUME:       int
    # ALARM_REQUIRE_ABOVE_SMA200: bool

    # # Per-(symbol, strategy) cooldown so a still-capitulating symbol
    # # doesn't spam Telegram once per finalized bar.
    # ALARM_COOLDOWN_MINUTES: int

    class Config:
        ENV_REPO = Path("C:/codebase/env-repo")
        env_file = ENV_REPO / "32_smsystem.env"
        env_file_encoding = "utf-8"
        case_sensitive = True



settings = Settings()
