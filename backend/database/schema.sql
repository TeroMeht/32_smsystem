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
CREATE TABLE daily (
    symbolid  integer       NOT NULL REFERENCES monitored_symbols(symbolid),
    date      date          NOT NULL,
    open      numeric(12,4),
    high      numeric(12,4),
    low       numeric(12,4),
    close     numeric(12,4),
    volume    bigint,
    atr       numeric(12,4),
    PRIMARY KEY (symbolid, date)
) PARTITION BY RANGE (date);

-- First partition (adjust date to whatever session you start with)
CREATE TABLE daily_20260807 PARTITION OF daily
    FOR VALUES FROM ('2026-08-07') TO ('2026-08-08');


-- 5. rvol_baseline (per-bar average volume per ticker × 1-min time-of-day slot)
-- bar_time is in ET (America/New_York) since the session grid is ET-based.
-- avg_volume is the average of that minute's RAW (per-bar) volume across the
-- N most-recent trading sessions in the rebuild window -- NOT cumulative.
-- The live path builds the cumulative denominator on the fly by summing
-- these per-bar averages across slots as the session progresses; that
-- way a missing slot contributes 0 but the running sum never drops back
-- to zero, so RVOL stays defined for the rest of the session.
CREATE TABLE rvol_baseline (
    symbolid     integer  NOT NULL REFERENCES monitored_symbols(symbolid),
    bar_time     time     NOT NULL,
    avg_volume   numeric(16,2) NOT NULL,
    sample_days  smallint NOT NULL,
    updated      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (symbolid, bar_time)
);


-- 6. Helpful lookup indexes for the live path
-- livestream latest-per-symbol reads happen constantly; keep ts DESC lookups cheap.
CREATE INDEX IF NOT EXISTS idx_livestream_symbolid_ts_desc
    ON livestream (symbolid, ts DESC);

-- daily lookups by symbol newest-first (last close, latest ATR)
CREATE INDEX IF NOT EXISTS idx_daily_symbolid_date_desc
    ON daily (symbolid, date DESC);
