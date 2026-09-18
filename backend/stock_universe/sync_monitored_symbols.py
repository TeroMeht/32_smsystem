"""
Step 6 - sync universe_liquid.csv into the monitored_symbols table.

Behaviour:
  - INSERT new tickers (auto-assigns symbolid)
  - UPDATE existing tickers' exchange / market_cap / adv_dollar /
    sic_code / sic_description / last_refresh, force active=true
    (auto-reactivates returning ones)
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


def _nullable(v) -> str | None:
    """Turn None / pandas NA / NaN / blank strings into SQL NULL; keep the rest as str.

    ``pd.isna`` covers None, pd.NA and np.nan alike -- important because a
    CSV column read as pandas ``string`` dtype yields ``pd.NA`` (not float
    NaN) for missing values, so a ``isinstance(v, float)`` check would miss
    it and pass the literal string ``"<NA>"`` to Postgres.
    """
    if v is None or pd.isna(v):
        return None
    s = str(v).strip()
    return s or None


def main() -> None:
    in_path = DATA_DIR / "universe_liquid.csv"

    log.info("=" * 60)
    log.info("Step 6: sync monitored_symbols")
    log.info("=" * 60)

    # sic_code is read as string to preserve leading zeros and to match the
    # DB column type; missing values stay as pandas NA rather than "nan".
    df = pd.read_csv(
        in_path,
        dtype={"sic_code": "string", "sic_description": "string"},
    )
    log.info("Loaded %d tickers from %s", len(df), in_path)
    df["market_cap"] = df["market_cap"].astype("int64")
    df["adv_dollar"] = df["adv_dollar"].astype("int64")

    sic_present = int(df["sic_code"].notna().sum()) if "sic_code" in df.columns else 0
    log.info("SIC classification present for %d / %d tickers",
             sic_present, len(df))

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
            (
                r.symbol,
                r.exchange,
                int(r.market_cap),
                int(r.adv_dollar),
                _nullable(getattr(r, "sic_code", None)),
                _nullable(getattr(r, "sic_description", None)),
                now,
                True,
            )
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
