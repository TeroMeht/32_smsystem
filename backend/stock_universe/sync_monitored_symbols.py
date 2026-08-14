"""
Step 6 - sync universe_liquid.csv into the monitored_symbols table.

Behaviour:
  - INSERT new tickers (auto-assigns symbolid)
  - UPDATE existing tickers' exchange / market_cap / adv_dollar /
    last_refresh, force active=true (auto-reactivates returning ones)
  - DEACTIVATE previously-active tickers no longer in the universe
    (active=false; never DELETE, so intraday_bars/daily FKs stay intact)

Input: DATA_DIR / universe_liquid.csv

This module is pure orchestration + logging. All SQL lives in
``backend.database.universe``.
"""

from datetime import datetime, timezone

import pandas as pd

from backend.common.logging_config import setup_logging
from backend.database.connection import connect
from backend.database.universe import (
    count_symbols,
    deactivate_symbols,
    fetch_symbol_active_map,
    upsert_symbols,
)
from backend.stock_universe.paths import DATA_DIR, LOGS_DIR

log = setup_logging("sync_monitored_symbols", LOGS_DIR)


def _sample(rows: list[str], n: int = 10) -> str:
    if not rows:
        return "(none)"
    shown = ", ".join(sorted(rows)[:n])
    more = f" ... (+{len(rows) - n} more)" if len(rows) > n else ""
    return shown + more


def main() -> None:
    in_path = DATA_DIR / "universe_liquid.csv"

    log.info("=" * 60)
    log.info("Step 6: sync monitored_symbols")
    log.info("=" * 60)

    df = pd.read_csv(in_path)
    log.info("Loaded %d tickers from %s", len(df), in_path)
    df["market_cap"] = df["market_cap"].astype("int64")
    df["adv_dollar"] = df["adv_dollar"].astype("int64")
    new_symbols = set(df["symbol"])
    now = datetime.now(timezone.utc)

    with connect() as conn:
        current = fetch_symbol_active_map(conn)
        log.info("Current table state: %d rows (%d active)",
                 len(current), sum(current.values()))

        existing = set(current)
        currently_active = {s for s, a in current.items() if a}

        to_insert     = new_symbols - existing
        to_update     = new_symbols & existing
        to_deactivate = currently_active - new_symbols
        to_reactivate = (new_symbols & existing) - currently_active

        log.info("")
        log.info("Planned changes:")
        log.info("  INSERT new tickers:        %5d", len(to_insert))
        log.info("  UPDATE existing tickers:   %5d", len(to_update))
        log.info("    - reactivating:          %5d", len(to_reactivate))
        log.info("  DEACTIVATE removed:        %5d", len(to_deactivate))
        log.info("")
        log.info("  new:          %s", _sample(list(to_insert)))
        log.info("  reactivated:  %s", _sample(list(to_reactivate)))
        log.info("  deactivated:  %s", _sample(list(to_deactivate)))

        rows = [
            (r.symbol, r.exchange, int(r.market_cap), int(r.adv_dollar), now, True)
            for r in df.itertuples()
        ]
        upsert_symbols(conn, rows)
        log.info("Upserted %d rows", len(rows))

        deactivated = deactivate_symbols(conn, list(to_deactivate), now)
        if deactivated:
            log.info("Deactivated %d rows", deactivated)

        conn.commit()
        log.info("Transaction committed")

        active_cnt, total_cnt = count_symbols(conn)
        log.info("Final table state: %d active / %d total", active_cnt, total_cnt)


if __name__ == "__main__":
    main()
