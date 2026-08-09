"""
Weekly universe refresh — runs the full cascade in one command.

    python -m backend.stock_universe.run

Steps:
    1. fetch_universe        NASDAQ Trader → universe_raw.csv
    2. filter_universe       price + market cap → universe_filtered.csv
    3. filter_liquidity      20-day ADV$      → universe_liquid.csv
    4. sync_monitored_symbols write to Postgres

Each sub-step also has its own log file under LOGS_DIR; this script
writes a top-level `run.log` summarising the whole cascade.
"""

import time

from backend.common.logging_config import setup_logging
from backend.stock_universe import (
    fetch_universe,
    filter_universe,
    filter_liquidity,
    sync_monitored_symbols,
)
from backend.stock_universe.paths import LOGS_DIR

log = setup_logging("run", LOGS_DIR)


def _stage(name: str, func) -> None:
    log.info("")
    log.info("################################################################")
    log.info("# %s", name)
    log.info("################################################################")
    t0 = time.time()
    func()
    log.info("%s finished in %.1fs", name, time.time() - t0)


def main() -> None:
    log.info("Weekly stock universe refresh starting")
    t0 = time.time()

    _stage("Step 1: fetch raw universe",       fetch_universe.main)
    _stage("Step 2-4: price + market cap",     filter_universe.main)
    _stage("Step 5: liquidity",                filter_liquidity.main)
    _stage("Step 6: sync monitored_symbols",   sync_monitored_symbols.main)

    log.info("")
    log.info("Full cascade finished in %.1fs", time.time() - t0)


if __name__ == "__main__":
    main()
