"""
Step 6 — sync universe_liquid.csv into the monitored_symbols table.

Behaviour:
  - INSERT new tickers (auto-assigns symbolid)
  - UPDATE existing tickers' market_cap, adv_dollar, last_refresh, active=true
  - DEACTIVATE previously-active tickers no longer in the universe
    (active=false — never DELETE, so intraday_bars/daily FKs stay intact
    and history is preserved)

Input: DATA_DIR / universe_liquid.csv
"""

from datetime import datetime, timezone

import pandas as pd
import psycopg2.extras

from backend.common.logging_config import setup_logging
from backend.database.connection import connect
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
        with conn.cursor() as cur:
            cur.execute("SELECT symbol, active FROM monitored_symbols")
            current = {sym: active for sym, active in cur.fetchall()}
            log.info("Current table state: %d rows (%d active)",
                     len(current), sum(current.values()))

            existing = set(current)
            currently_active = {s for s, a in current.items() if a}

            to_insert       = new_symbols - existing
            to_update       = new_symbols & existing
            to_deactivate   = currently_active - new_symbols
            to_reactivate   = (new_symbols & existing) - currently_active

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
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO monitored_symbols
                    (symbol, exchange, market_cap, adv_dollar, last_refresh, active)
                VALUES %s
                ON CONFLICT (symbol) DO UPDATE SET
                    exchange     = EXCLUDED.exchange,
                    market_cap   = EXCLUDED.market_cap,
                    adv_dollar   = EXCLUDED.adv_dollar,
                    last_refresh = EXCLUDED.last_refresh,
                    active       = true
                """,
                rows,
                page_size=500,
            )
            log.info("Upserted %d rows", len(rows))

            if to_deactivate:
                cur.execute(
                    """
                    UPDATE monitored_symbols
                       SET active = false, last_refresh = %s
                     WHERE symbol = ANY(%s) AND active = true
                    """,
                    (now, list(to_deactivate)),
                )
                log.info("Deactivated %d rows", cur.rowcount)

            conn.commit()
            log.info("Transaction committed")

            cur.execute(
                "SELECT COUNT(*) FILTER (WHERE active), COUNT(*) FROM monitored_symbols"
            )
            active_cnt, total_cnt = cur.fetchone()
            log.info("Final table state: %d active / %d total", active_cnt, total_cnt)


if __name__ == "__main__":
    main()
