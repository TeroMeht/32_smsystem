"""
Filesystem paths owned by the stock_universe subsystem.

Kept local to this package so live monitoring / backtester / etc. don't
share log or data directories with it.
"""

from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent

DATA_DIR = PACKAGE_DIR / "data"
LOGS_DIR = PACKAGE_DIR / "logs"

DATA_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)
