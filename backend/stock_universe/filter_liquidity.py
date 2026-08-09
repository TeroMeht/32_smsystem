"""
Step 5 — liquidity filter (20-day average dollar volume).

Uses Polygon's grouped-daily endpoint (one call per date returns OHLCV
for every US stock) so total calls are ~20 instead of ~40,000.

Inputs:  DATA_DIR / universe_filtered.csv
Outputs: DATA_DIR / universe_liquid.csv               (final tradeable list)
         DATA_DIR / universe_dropped_liquidity.csv    (with reason)
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import pandas as pd

from backend.common.logging_config import setup_logging
from backend.core.config import settings
from backend.stock_universe.http_session import build_session
from backend.stock_universe.paths import DATA_DIR, LOGS_DIR

log = setup_logging("filter_liquidity", LOGS_DIR)

SESSION = build_session(settings.HTTP_WORKERS_GROUPED_DAILY)
BASE = settings.POLYGON_BASE_URL


def _pct(part: int, whole: int) -> str:
    return f"{100 * part / whole:5.1f}%" if whole else "  n/a"


def _log_sample(df: pd.DataFrame, label: str, cols: list[str], n: int = 5) -> None:
    if df.empty:
        return
    log.info("  sample (%s):", label)
    for row in df.head(n).to_dict(orient="records"):
        log.info("    %s", "  ".join(f"{c}={row.get(c)!r}" for c in cols))


def fetch_grouped_day(day: date) -> tuple[date, list[dict]]:
    url = f"{BASE}/v2/aggs/grouped/locale/us/market/stocks/{day.isoformat()}"
    try:
        r = SESSION.get(url, timeout=30)
        r.raise_for_status()
        return day, r.json().get("results", []) or []
    except Exception as e:
        log.warning("grouped fetch failed for %s: %s", day, e)
        return day, []


def main() -> pd.DataFrame:
    in_path      = DATA_DIR / "universe_filtered.csv"
    out_path     = DATA_DIR / "universe_liquid.csv"
    dropped_path = DATA_DIR / "universe_dropped_liquidity.csv"

    log.info("=" * 60)
    log.info("Step 5: liquidity filter")
    log.info("Threshold: ADV$ > $%s (20-day trailing)",
             f"{settings.UNIVERSE_MIN_ADV_DOLLAR:,}")
    log.info("=" * 60)

    universe = pd.read_csv(in_path)
    log.info("Loaded %d tickers from %s", len(universe), in_path)
    keep = set(universe["symbol"])

    # ---- STAGE A: bulk fetch daily bars ----
    today = date.today()
    days = [today - timedelta(days=i)
            for i in range(1, settings.UNIVERSE_LOOKBACK_DAYS + 1)]
    log.info("")
    log.info("--- STAGE A: fetch grouped daily bars for %d calendar days ---",
             len(days))

    t0 = time.time()
    bars: dict[str, list[tuple[date, float, int]]] = {}
    trading_days_found = 0
    workers = settings.HTTP_WORKERS_GROUPED_DAILY

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch_grouped_day, d): d for d in days}
        for fut in as_completed(futures):
            d, results = fut.result()
            if not results:
                continue
            trading_days_found += 1
            for row in results:
                sym = row.get("T")
                if sym not in keep:
                    continue
                c = row.get("c")
                v = row.get("v")
                if c and v:
                    bars.setdefault(sym, []).append((d, float(c), int(v)))

    log.info("Grouped fetch complete in %.1fs", time.time() - t0)
    log.info("Trading days with data: %d", trading_days_found)
    log.info("Tickers with any daily bars: %d / %d", len(bars), len(universe))

    # ---- STAGE B: compute ADV$ ----
    log.info("")
    log.info("--- STAGE B: compute 20-day average dollar volume ---")

    def adv_dollar(sym: str) -> tuple[float | None, int]:
        rows = bars.get(sym, [])
        rows_sorted = sorted(rows, key=lambda x: x[0], reverse=True)[:20]
        if len(rows_sorted) < settings.UNIVERSE_MIN_SAMPLE_DAYS:
            return None, len(rows_sorted)
        dv = sum(c * v for _, c, v in rows_sorted) / len(rows_sorted)
        return dv, len(rows_sorted)

    adv_results = {s: adv_dollar(s) for s in universe["symbol"]}
    universe["adv_dollar"]  = universe["symbol"].map(lambda s: adv_results[s][0])
    universe["sample_days"] = universe["symbol"].map(lambda s: adv_results[s][1])

    # ---- STAGE C: apply filter ----
    log.info("")
    log.info("--- STAGE C: apply liquidity filter ---")
    dropped_rows: list[dict] = []

    insufficient = universe[universe["adv_dollar"].isna()]
    log.info("Dropped: insufficient history (< %d days)   %5d  (%s)",
             settings.UNIVERSE_MIN_SAMPLE_DAYS, len(insufficient),
             _pct(len(insufficient), len(universe)))
    _log_sample(insufficient, "insufficient_history",
                ["symbol", "name", "sample_days"])
    for r in insufficient.itertuples():
        dropped_rows.append({"symbol": r.symbol, "name": r.name,
                             "reason": "insufficient_history",
                             "sample_days": r.sample_days})

    with_adv = universe.dropna(subset=["adv_dollar"]).copy()
    below = with_adv[with_adv["adv_dollar"] <= settings.UNIVERSE_MIN_ADV_DOLLAR]
    log.info("Dropped: ADV$ <= $%s              %5d  (%s of tickers with history)",
             f"{settings.UNIVERSE_MIN_ADV_DOLLAR:,}", len(below),
             _pct(len(below), len(with_adv)))
    _log_sample(below, "illiquid",
                ["symbol", "name", "last_price", "adv_dollar"])
    for r in below.itertuples():
        dropped_rows.append({"symbol": r.symbol, "name": r.name,
                             "reason": "illiquid",
                             "adv_dollar": r.adv_dollar,
                             "sample_days": r.sample_days})

    final = with_adv[with_adv["adv_dollar"] > settings.UNIVERSE_MIN_ADV_DOLLAR].copy()
    final = final.sort_values("adv_dollar", ascending=False).reset_index(drop=True)

    # ---- SUMMARY ----
    log.info("")
    log.info("=" * 60)
    log.info("SUMMARY")
    log.info("=" * 60)
    log.info("Input tickers:                        %5d", len(universe))
    log.info("  - insufficient history:             %5d", len(insufficient))
    log.info("  - ADV$ <= $%s:            %5d",
             f"{settings.UNIVERSE_MIN_ADV_DOLLAR:,}", len(below))
    log.info("Final tradeable universe:             %5d  (%s of input)",
             len(final), _pct(len(final), len(universe)))

    final.to_csv(out_path, index=False)
    log.info("Wrote %s (%d rows)", out_path, len(final))
    pd.DataFrame(dropped_rows).to_csv(dropped_path, index=False)
    log.info("Wrote %s (%d rows)", dropped_path, len(dropped_rows))

    log.info("")
    log.info("Top 15 by ADV$:")
    for r in final.head(15).itertuples():
        log.info("  %-6s  %-40s  price=$%8.2f  ADV$=$%s",
                 r.symbol, r.name[:40], r.last_price,
                 f"{int(r.adv_dollar):,}")

    return final


if __name__ == "__main__":
    main()
