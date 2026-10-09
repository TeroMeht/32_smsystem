"""
Project-wide logging setup -- ONE log file for everything.

Every part of 32_smsystem writes to the same file, ``<repo>/logs/smsystem.log``:

  * the FastAPI app (all ``backend.*`` / ``data_sources.*`` loggers + uvicorn's
    own startup/error lines)                     -> ``setup_app_logging()``
  * the weekly stock_universe batch scripts      -> ``setup_logging(name)``
  * the PowerShell service wrapper               -> ``scripts/run_service.ps1``
    (logger name ``service``)

Every line has the same shape, so the file reads as one timeline:

    [2026-10-08 14:04:50] INFO backend.main: 32_smsystem starting up

Size: the per-bar livestream INFO lines are the bulk of the volume (they used
to blow ``app.log`` up to gigabytes), so the app rotates the file at
``LOG_MAX_BYTES`` and keeps ``LOG_BACKUPS`` old copies (``smsystem.log.1`` ...).
Only the long-running app rotates; the batch scripts and the wrapper just
append. On Windows a rollover can't rename the file while another process
has it open (e.g. the weekly batch mid-run); the handler then keeps writing
to the current file and retries on the next line, so nothing is lost.

Console: both entry points also log to stdout (start.bat shows it live).
The service wrapper sets ``SMSYSTEM_LOG_CONSOLE=0`` because there is no
console there, and the file already has every line.
"""

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = REPO_ROOT / "logs"
LOG_FILE = LOG_DIR / "smsystem.log"

LOG_MAX_BYTES = 50 * 1024 * 1024   # rotate at 50 MB
LOG_BACKUPS = 3                    # keep smsystem.log.1 .. .3

_FMT = logging.Formatter(
    "[%(asctime)s] %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def _console_enabled() -> bool:
    return os.environ.get("SMSYSTEM_LOG_CONSOLE", "1") != "0"


def _mark(h: logging.Handler) -> logging.Handler:
    h.setFormatter(_FMT)
    h._sm_owned = True  # type: ignore[attr-defined]
    return h


def setup_logging(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Logger for a batch script (stock_universe). Appends to the shared
    ``logs/smsystem.log`` and echoes to stdout. No rotation here -- the
    app owns rotation.

    Idempotent -- safe to call multiple times.
    """
    logger = logging.getLogger(name)
    if getattr(logger, "_sm_configured", False):
        return logger

    logger.setLevel(level)
    logger.propagate = False

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger.addHandler(_mark(logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")))
    logger.addHandler(_mark(logging.StreamHandler(sys.stdout)))

    for noisy in ("urllib3", "requests"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    logger._sm_configured = True  # type: ignore[attr-defined]
    return logger


def setup_app_logging(level: int = logging.INFO) -> None:
    """
    Configure the ROOT logger for the FastAPI app: everything goes to
    ``logs/smsystem.log`` (rotating) and, unless disabled, to stdout.

    Uvicorn installs its own stderr/stdout handlers before importing the
    app; those are removed here and uvicorn's loggers propagate to root, so
    its lines land in the same file with the same format. Call this at
    import time of ``backend.main`` so uvicorn's "Started server process"
    and "Application startup" lines are captured too.

    Idempotent -- replaces handlers installed by a previous call.
    """
    root = logging.getLogger()
    root.setLevel(level)

    for h in list(root.handlers):
        if getattr(h, "_sm_owned", False):
            root.removeHandler(h)
            h.close()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    root.addHandler(_mark(RotatingFileHandler(
        LOG_FILE, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUPS, encoding="utf-8",
    )))
    if _console_enabled():
        root.addHandler(_mark(logging.StreamHandler(sys.stdout)))

    # Quiet the loudest third-party libraries so our INFO signal isn't buried
    for noisy in ("urllib3", "requests", "aiohttp.access", "asyncio", "websockets.client"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    # Route uvicorn through root (one file, one format).
    for uv in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        lg = logging.getLogger(uv)
        lg.handlers.clear()
        lg.propagate = True
        lg.setLevel(logging.INFO)
    # Access log = one line per HTTP request; the dashboard polls
    # /api/livestream/top every second, so keep only warnings+.
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
