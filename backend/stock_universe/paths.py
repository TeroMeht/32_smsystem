"""
Filesystem paths owned by the stock_universe subsystem.

Kept local to this package so live monitoring / backtester / etc. don't
share data directories with it. Logs go to the shared logs/smsystem.log.
"""

from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent

DATA_DIR = PACKAGE_DIR / "data"

DATA_DIR.mkdir(parents=True, exist_ok=True)
