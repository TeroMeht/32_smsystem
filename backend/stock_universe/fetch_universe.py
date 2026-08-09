"""
Step 1 — fetch the widest possible US common-stock universe.

Source (free, updated nightly):
    https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt
    https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt

Output: DATA_DIR / universe_raw.csv  with columns [symbol, name, exchange]
"""

from io import StringIO

import pandas as pd
import requests

from backend.common.logging_config import setup_logging
from backend.stock_universe.paths import DATA_DIR, LOGS_DIR

log = setup_logging("fetch_universe", LOGS_DIR)

NASDAQ_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_URL  = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"

EXCHANGE_MAP = {
    "N": "NYSE",
    "A": "NYSE American",
    # "P": "NYSE Arca"  — mostly ETFs, skip
    # "Z": "Cboe BZX"   — mostly ETFs, skip
}


def _download(url: str) -> pd.DataFrame:
    log.info("Downloading %s", url)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    df = pd.read_csv(StringIO(resp.text), sep="|", dtype=str)
    # Strip "File Creation Time" footer row
    df = df[~df.iloc[:, 0].str.startswith("File Creation Time", na=False)]
    return df


def fetch_nasdaq() -> pd.DataFrame:
    df = _download(NASDAQ_URL)
    df = df[
        (df["Test Issue"] == "N")
        & (df["ETF"] == "N")
        & (df["Financial Status"] == "N")
    ]
    return pd.DataFrame({
        "symbol":   df["Symbol"],
        "name":     df["Security Name"],
        "exchange": "NASDAQ",
    })


def fetch_other() -> pd.DataFrame:
    df = _download(OTHER_URL)
    df = df[
        (df["Test Issue"] == "N")
        & (df["ETF"] == "N")
        & (df["Exchange"].isin(EXCHANGE_MAP.keys()))
    ]
    return pd.DataFrame({
        "symbol":   df["ACT Symbol"],
        "name":     df["Security Name"],
        "exchange": df["Exchange"].map(EXCHANGE_MAP),
    })


def fetch_universe() -> pd.DataFrame:
    df = pd.concat([fetch_nasdaq(), fetch_other()], ignore_index=True)
    df = df.dropna(subset=["symbol", "name"])
    df = df[df["symbol"].str.match(r"^[A-Z]+$", na=False)]

    bad_name_patterns = (
        r"\b(?:Warrant|Warrants|Right|Rights|Unit|Units|Preferred|"
        r"Notes?|Debenture|Trust Preferred|Depositary)\b"
    )
    df = df[~df["name"].str.contains(bad_name_patterns, case=False, regex=True, na=False)]

    return (
        df.drop_duplicates(subset=["symbol"])
          .sort_values("symbol")
          .reset_index(drop=True)
    )


def main() -> pd.DataFrame:
    log.info("=" * 60)
    log.info("Step 1: fetch raw universe")
    log.info("=" * 60)

    universe = fetch_universe()
    log.info("Total common stocks: %d", len(universe))
    for exch, cnt in universe["exchange"].value_counts().items():
        log.info("  %-15s %5d", exch, cnt)

    out_path = DATA_DIR / "universe_raw.csv"
    universe.to_csv(out_path, index=False)
    log.info("Saved %d rows to %s", len(universe), out_path)
    return universe


if __name__ == "__main__":
    main()
