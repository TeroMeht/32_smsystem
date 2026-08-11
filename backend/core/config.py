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
    # POLYGON_BASE_URL -> REST         e.g. https://api.polygon.io
    # POLYGON_WS_URL   -> WebSocket    e.g. wss://socket.polygon.io/stocks
    # Polygon serves the two over DIFFERENT hosts, so we can't derive one
    # from the other -- both are explicit env values.
    POLYGON_API_KEY: str
    POLYGON_BASE_URL: str
    POLYGON_WS_URL: str

    # --- Stock universe filter thresholds ---
    UNIVERSE_MIN_PRICE:       float
    UNIVERSE_MIN_MARKET_CAP:  int
    UNIVERSE_MIN_ADV_DOLLAR:  int
    UNIVERSE_LOOKBACK_DAYS:   int
    UNIVERSE_MIN_SAMPLE_DAYS: int

    # --- Concurrency for HTTP calls ---
    HTTP_WORKERS_TICKER_DETAILS: int
    HTTP_WORKERS_GROUPED_DAILY:  int

    # --- Historian / baseline knobs ---
    # INTRADAY_BACKFILL_DAYS : calendar-day window fetched into intraday_bars
    #                          (also the calendar window used for the RVOL
    #                          baseline lookback -- 8 days guarantees >=5
    #                          trading sessions after weekends/holidays).
    # DAILY_BACKFILL_DAYS    : calendar-day window fetched into daily
    #                          (20 days guarantees >=14 trading days for
    #                          ATR14).
    # RVOL_SAMPLE_SESSIONS   : trading sessions averaged into the baseline.
    # DAILY_STALE_DAYS       : freshness threshold for daily -- older than
    #                          this triggers a refetch (3 covers Fri->Mon).
    # INTRADAY_STALE_DAYS    : same idea for intraday.
    INTRADAY_BACKFILL_DAYS: int
    DAILY_BACKFILL_DAYS:    int
    RVOL_SAMPLE_SESSIONS:   int
    DAILY_STALE_DAYS:       int
    INTRADAY_STALE_DAYS:    int

    # --- Runtime mode ---
    # MODE = "live"    : historian aligns to wall-clock today, WS livestream
    #                    opens in the background.
    # MODE = "replay"  : historian treats REPLAY_DAY as "today", RVOL baseline
    #                    is built from sessions strictly before REPLAY_DAY,
    #                    and startup runs the replay driver INSTEAD of the WS.
    # REPLAY_START_TIME  : "" -> start from beginning of the replay day.
    #                      "HH:MM" 24-hour, interpreted as HELSINKI local time
    #                      on REPLAY_DAY; bars before this instant are skipped.
    #                      (e.g. "16:30", NOT "4:30 PM".)
    # REPLAY_* fields are still required in live mode -- pick placeholder
    # values you're comfortable with, they simply aren't consulted.
    MODE: str
    REPLAY_DAY: date
    REPLAY_START_TIME: str
    REPLAY_SPEED: float

    @model_validator(mode="after")
    def _normalize_mode(self) -> "Settings":
        mode = self.MODE.lower()
        if mode not in ("live", "replay"):
            raise ValueError(f"MODE must be 'live' or 'replay' (got {self.MODE!r})")
        self.MODE = mode

        # REPLAY_START_TIME must be empty or strict 24-hour "HH:MM".
        # Reject 12-hour formats like "4:30 PM" -- users have to think in
        # the same convention as everything else on this system.
        s = (self.REPLAY_START_TIME or "").strip()
        if s:
            ok = False
            parts = s.split(":")
            if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                hh, mm = int(parts[0]), int(parts[1])
                if 0 <= hh <= 23 and 0 <= mm <= 59:
                    ok = True
            if not ok:
                raise ValueError(
                    "REPLAY_START_TIME must be empty or strict 24-hour 'HH:MM' "
                    f"(e.g. '16:30'), got {self.REPLAY_START_TIME!r}"
                )
        return self

    class Config:
        ENV_REPO = Path("C:/codebase/env-repo")
        env_file = ENV_REPO / "32_smsystem.env"
        env_file_encoding = "utf-8"
        case_sensitive = True


settings = Settings()
