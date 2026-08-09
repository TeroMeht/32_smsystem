"""
Steps 2-4 — price + market cap filter using Polygon.

Cascade:
    - Snapshot endpoint gives last close for every US ticker in ONE call
    - Filter price > UNIVERSE_MIN_PRICE
    - Fetch ticker details (market cap) in parallel for survivors
    - Filter market cap > UNIVERSE_MIN_MARKET_CAP

Inputs:  DATA_DIR / universe_raw.csv
Outputs: DATA_DIR / universe_filtered.csv    (survivors)
         DATA_DIR / universe_dropped.csv     (with reason)
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from backend.common.logging_config import setup_logging
from backend.core.config import settings
from backend.stock_universe.http_session import build_session
from backend.stock_universe.paths import DATA_DIR, LOGS_DIR

log = setup_logging("filter_universe", LOGS_DIR)

SESSION = build_session(settings.HTTP_WORKERS_TICKER_DETAILS)
BASE = settings.POLYGON_BASE_URL


def _pct(part: int, whole: int) -> str:
    return f"{100 * part / whole:5.1f}%" if whole else "  n/a"


def _log_drop_sample(df: pd.DataFrame, reason: str, cols: list[str], n: int = 5) -> None:
    if df.empty:
        return
    log.info("  sample dropped (%s):", reason)
    for row in df.head(n).to_dict(orient="records"):
        log.info("    %s", "  ".join(f"{c}={row.get(c)!r}" for c in cols))


def fetch_snapshot_prices() -> dict[str, float]:
    url = f"{BASE}/v2/snapshot/locale/us/markets/stocks/tickers"
    r = SESSION.get(url, timeout=60)
    r.raise_for_status()
    data = r.json()

    prices: dict[str, float] = {}
    skipped = 0
    for t in data.get("tickers", []):
        px = (t.get("day") or {}).get("c") or (t.get("prevDay") or {}).get("c")
        if px:
            prices[t["ticker"]] = float(px)
        else:
            skipped += 1
    log.info("Snapshot: %d tickers with price, %d without", len(prices), skipped)
    return prices


def fetch_market_cap(symbol: str) -> tuple[str, float | None, str | None]:
    url = f"{BASE}/v3/reference/tickers/{symbol}"
    try:
        r = SESSION.get(url, timeout=15)
        if r.status_code == 404:
            return symbol, None, "404_not_found"
        r.raise_for_status()
        mc = r.json().get("results", {}).get("market_cap")
        if mc is None:
            return symbol, None, "no_market_cap_field"
        return symbol, float(mc), None
    except Exception as e:
        return symbol, None, f"error:{type(e).__name__}"


def main() -> pd.DataFrame:
    in_path      = DATA_DIR / "universe_raw.csv"
    out_path     = DATA_DIR / "universe_filtered.csv"
    dropped_path = DATA_DIR / "universe_dropped.csv"

    log.info("=" * 60)
    log.info("Step 2-4: price + market cap filter")
    log.info("Thresholds: price > $%.2f, market_cap > $%s",
             settings.UNIVERSE_MIN_PRICE,
             f"{settings.UNIVERSE_MIN_MARKET_CAP:,}")
    log.info("=" * 60)

    raw = pd.read_csv(in_path)
    log.info("Loaded %d raw tickers from %s", len(raw), in_path)
    for exch, cnt in raw["exchange"].value_counts().items():
        log.info("  %-15s %5d", exch, cnt)

    dropped_rows: list[dict] = []

    # ---- STAGE 1: snapshot prices ----
    log.info("")
    log.info("--- STAGE 1: fetch snapshot prices ---")
    t0 = time.time()
    prices = fetch_snapshot_prices()
    log.info("Snapshot fetched in %.1fs", time.time() - t0)

    raw["last_price"] = raw["symbol"].map(prices)
    no_price = raw[raw["last_price"].isna()]
    log.info("Dropped: no price in snapshot         %5d  (%s)",
             len(no_price), _pct(len(no_price), len(raw)))
    _log_drop_sample(no_price, "no_price", ["symbol", "name", "exchange"])
    for r in no_price.itertuples():
        dropped_rows.append({"symbol": r.symbol, "name": r.name,
                             "exchange": r.exchange, "reason": "no_price"})
    priced = raw.dropna(subset=["last_price"]).copy()

    # ---- STAGE 2: price filter ----
    log.info("")
    log.info("--- STAGE 2: price filter (> $%.2f) ---", settings.UNIVERSE_MIN_PRICE)
    below_price = priced[priced["last_price"] <= settings.UNIVERSE_MIN_PRICE]
    log.info("Dropped: price <= $%.2f              %5d  (%s of priced)",
             settings.UNIVERSE_MIN_PRICE, len(below_price),
             _pct(len(below_price), len(priced)))
    _log_drop_sample(below_price, "low_price",
                     ["symbol", "name", "last_price", "exchange"])
    for r in below_price.itertuples():
        dropped_rows.append({"symbol": r.symbol, "name": r.name,
                             "exchange": r.exchange, "reason": "low_price",
                             "last_price": r.last_price})
    priced = priced[priced["last_price"] > settings.UNIVERSE_MIN_PRICE].copy()
    log.info("Survivors after price filter:         %5d", len(priced))

    # ---- STAGE 3: parallel market cap fetch ----
    workers = settings.HTTP_WORKERS_TICKER_DETAILS
    log.info("")
    log.info("--- STAGE 3: fetch market caps (%d workers) ---", workers)
    t0 = time.time()
    caps: dict[str, float | None] = {}
    errors: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch_market_cap, s): s for s in priced["symbol"]}
        for i, fut in enumerate(as_completed(futures), 1):
            sym, mc, err = fut.result()
            caps[sym] = mc
            if err:
                errors[sym] = err
            if i % 500 == 0:
                log.info("  progress: %4d/%d market caps fetched", i, len(futures))
    log.info("Market caps fetched in %.1fs", time.time() - t0)
    if errors:
        err_counts: dict[str, int] = {}
        for reason in errors.values():
            err_counts[reason] = err_counts.get(reason, 0) + 1
        log.info("Fetch errors / missing data: %d", len(errors))
        for reason, cnt in sorted(err_counts.items(), key=lambda x: -x[1]):
            log.info("    %-25s %5d", reason, cnt)

    priced["market_cap"] = priced["symbol"].map(caps)

    no_cap = priced[priced["market_cap"].isna()]
    log.info("Dropped: market_cap unavailable       %5d  (%s of priced)",
             len(no_cap), _pct(len(no_cap), len(priced)))
    _log_drop_sample(no_cap, "no_market_cap",
                     ["symbol", "name", "last_price", "exchange"])
    for r in no_cap.itertuples():
        dropped_rows.append({"symbol": r.symbol, "name": r.name,
                             "exchange": r.exchange, "reason": "no_market_cap",
                             "last_price": r.last_price})
    with_cap = priced.dropna(subset=["market_cap"]).copy()

    # ---- STAGE 4: market cap filter ----
    log.info("")
    log.info("--- STAGE 4: market cap filter (> $%s) ---",
             f"{settings.UNIVERSE_MIN_MARKET_CAP:,}")
    below_cap = with_cap[with_cap["market_cap"] <= settings.UNIVERSE_MIN_MARKET_CAP]
    log.info("Dropped: small cap                    %5d  (%s of priced+capped)",
             len(below_cap), _pct(len(below_cap), len(with_cap)))
    _log_drop_sample(below_cap, "small_cap",
                     ["symbol", "name", "last_price", "market_cap"])
    for r in below_cap.itertuples():
        dropped_rows.append({"symbol": r.symbol, "name": r.name,
                             "exchange": r.exchange, "reason": "small_cap",
                             "last_price": r.last_price,
                             "market_cap": r.market_cap})

    final = with_cap[with_cap["market_cap"] > settings.UNIVERSE_MIN_MARKET_CAP].copy()
    final = final.sort_values("market_cap", ascending=False).reset_index(drop=True)

    # ---- SUMMARY ----
    log.info("")
    log.info("=" * 60)
    log.info("SUMMARY")
    log.info("=" * 60)
    log.info("Raw universe:                         %5d", len(raw))
    log.info("  - no price in snapshot:             %5d", len(no_price))
    log.info("  - price <= $%.2f:                   %5d",
             settings.UNIVERSE_MIN_PRICE, len(below_price))
    log.info("  - market_cap missing:               %5d", len(no_cap))
    log.info("  - market_cap <= $%s:       %5d",
             f"{settings.UNIVERSE_MIN_MARKET_CAP:,}", len(below_cap))
    log.info("Final survivors:                      %5d  (%s of raw)",
             len(final), _pct(len(final), len(raw)))

    final.to_csv(out_path, index=False)
    log.info("Wrote %s (%d rows)", out_path, len(final))
    pd.DataFrame(dropped_rows).to_csv(dropped_path, index=False)
    log.info("Wrote %s (%d rows)", dropped_path, len(dropped_rows))

    log.info("")
    log.info("Top 10 by market cap:")
    for r in final.head(10).itertuples():
        log.info("  %-6s  %-40s  $%12.2f  cap=$%s",
                 r.symbol, r.name[:40], r.last_price, f"{int(r.market_cap):,}")

    return final


if __name__ == "__main__":
    main()
