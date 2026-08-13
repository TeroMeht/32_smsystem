-- =========================================================
-- Database: 32_smsystem
-- Stock monitoring system — 1-min bars, cumulative RVOL
--
-- Data source: Massive/Polygon WS /stocks/AM (1-min aggregates) for live
-- and REST /v2/aggs/.../range/1/minute/... for historian + replay backfill.
-- All indicator baselines (VWAP, EMA9, RVOL, RelATR) are computed on the
-- 1-min cadence; the DB schema is deliberately timespan-agnostic (just OHLCV
-- rows keyed by ts) so the same tables serve live and replay identically.
-- =========================================================

-- 1. monitored_symbols
CREATE TABLE monitored_symbols (
    symbolid     serial      PRIMARY KEY,
    symbol       text        NOT NULL UNIQUE,
    exchange     text,                              -- 'NASDAQ' / 'NYSE' / ... (TradingView prefix)
    market_cap   bigint,
    adv_dollar   bigint,
    last_refresh timestamptz,
    active       boolean     NOT NULL DEFAULT true,
    added        timestamptz NOT NULL DEFAULT now()
);


-- 2. livestream (today only, ephemeral)
CREATE UNLOGGED TABLE livestream (
    symbolid  integer      NOT NULL REFERENCES monitored_symbols(symbolid),
    ts        timestamptz  NOT NULL,
    open      numeric(12,4),
    high      numeric(12,4),
    low       numeric(12,4),
    close     numeric(12,4),
    volume    bigint,
    vwap      numeric(12,4),
    ema9      numeric(12,4),
    rvol_cum  numeric(8,4),
    relatr    numeric(8,4),
    PRIMARY KEY (symbolid, ts)
);


-- 3. intraday_bars (1-min history, partitioned by day, 8-day retention)
--    Retention is 8 calendar days so the RVOL baseline can always find
--    at least 5 trading sessions on disk (survives weekends + one holiday).
CREATE TABLE intraday_bars (
    symbolid  integer      NOT NULL REFERENCES monitored_symbols(symbolid),
    ts        timestamptz  NOT NULL,
    open      numeric(12,4),
    high      numeric(12,4),
    low       numeric(12,4),
    close     numeric(12,4),
    volume    bigint,
    PRIMARY KEY (symbolid, ts)
) PARTITION BY RANGE (ts);

-- First partition (adjust date to whatever session you start with)
CREATE TABLE intraday_bars_20260807 PARTITION OF intraday_bars
    FOR VALUES FROM ('2026-08-07') TO ('2026-08-08');


-- 4. daily (partitioned by day, 14-day retention)
-- Raw-only: exactly what Polygon returned. Calculated fields (ATR, etc.)
-- live in daily_indicators so the incoming/calculated boundary is explicit.
CREATE TABLE daily (
    symbolid  integer       NOT NULL REFERENCES monitored_symbols(symbolid),
    date      date          NOT NULL,
    open      numeric(12,4),
    high      numeric(12,4),
    low       numeric(12,4),
    close     numeric(12,4),
    volume    bigint,
    PRIMARY KEY (symbolid, date)
) PARTITION BY RANGE (date);

-- First partition (adjust date to whatever session you start with)
CREATE TABLE daily_20260807 PARTITION OF daily
    FOR VALUES FROM ('2026-08-07') TO ('2026-08-08');


-- 4b. daily_indicators (calculated fields keyed to the daily grid)
-- Same partitioning + retention as ``daily``; historian computes ATR14 in
-- a separate pass (read daily -> pandas -> insert daily_indicators) so
-- ``daily`` stays raw-only.
CREATE TABLE daily_indicators (
    symbolid  integer      NOT NULL REFERENCES monitored_symbols(symbolid),
    date      date         NOT NULL,
    atr       numeric(12,4),
    updated   timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (symbolid, date)
) PARTITION BY RANGE (date);

-- First partition (adjust date to whatever session you start with)
CREATE TABLE daily_indicators_20260807 PARTITION OF daily_indicators
    FOR VALUES FROM ('2026-08-07') TO ('2026-08-08');


-- 5. rvol_baseline (per-bar average volume per ticker × 1-min time-of-day slot)
-- bar_time is in Helsinki (matches the display used everywhere else).
-- session_date bucketing (in the rebuild SQL) stays in ET because US
-- sessions are inherently defined on the ET calendar.
-- avg_volume is the average of that minute's RAW (per-bar) volume across the
-- N most-recent trading sessions in the rebuild window -- NOT cumulative.
-- The live path builds the cumulative denominator on the fly by summing
-- these per-bar averages across slots as the session progresses; that
-- way a missing slot contributes 0 but the running sum never drops back
-- to zero, so RVOL stays defined for the rest of the session.
CREATE TABLE rvol_baseline (
    symbolid     integer       NOT NULL REFERENCES monitored_symbols(symbolid),
    -- ``timetz`` stamps the Helsinki UTC offset alongside the time so it's
    -- unambiguous at a glance (04:00:00+03, 09:30:00+03, ...). Rebuild
    -- runs daily -- the offset always reflects "now" (+03 in EEST, +02
    -- in EET), so values self-heal across DST boundaries. Python side
    -- treats the value as a naive time (tz stripped on read).
    bar_time     timetz        NOT NULL,
    avg_volume   numeric(16,2) NOT NULL,
    sample_days  smallint      NOT NULL,
    updated      timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (symbolid, bar_time)
);


-- 6. backfill_status  (persistent freshness ledger)
--
-- One row per SUCCESSFUL historian run. Each row records WHEN the daily
-- and/or intraday portions completed, plus how many rows they added.
--
--   * daily_last_run    -- timestamp of this run's daily portion; NULL
--                          if the daily side was skipped in this run.
--   * intraday_last_run -- same for intraday.
--   * *_rows_added      -- 0 if nothing new landed on disk.
--
-- The historian consults ``MAX(daily_last_run)`` / ``MAX(intraday_last_run)``
-- on startup: if the most recent successful run already happened today,
-- REST fetches are skipped entirely. The rvol_baseline rebuild is
-- similarly conditional -- only runs when intraday_rows_added > 0.
CREATE TABLE backfill_status (
    id                  serial      PRIMARY KEY,
    daily_last_run      timestamptz,
    intraday_last_run   timestamptz
);

CREATE INDEX idx_backfill_status_daily_last_run
    ON backfill_status (daily_last_run DESC NULLS LAST);
CREATE INDEX idx_backfill_status_intraday_last_run
    ON backfill_status (intraday_last_run DESC NULLS LAST);


-- 7. Helpful lookup indexes for the live path
-- livestream latest-per-symbol reads happen constantly; keep ts DESC lookups cheap.
CREATE INDEX IF NOT EXISTS idx_livestream_symbolid_ts_desc
    ON livestream (symbolid, ts DESC);

-- daily lookups by symbol newest-first (last close)
CREATE INDEX IF NOT EXISTS idx_daily_symbolid_date_desc
    ON daily (symbolid, date DESC);

-- daily_indicators lookups by symbol newest-first (latest ATR feeds RelATR)
CREATE INDEX IF NOT EXISTS idx_daily_indicators_symbolid_date_desc
    ON daily_indicators (symbolid, date DESC);
