"""
Project-wide logging setup.

Each named logger gets its own file handler (log_dir/<name>.log) plus a
console handler. Handlers are attached to the *named* logger with
propagate=False, so multiple loggers in the same process (e.g. when
run.py orchestrates several stages) each write to their own file without
interfering with each other.

Log directories are scoped per subsystem — caller passes `log_dir`
(e.g. stock_universe/paths.LOGS_DIR).
"""

import logging
import sys
from pathlib import Path

_FMT = logging.Formatter(
    "[%(asctime)s] %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def setup_logging(
    name: str,
    log_dir: Path,
    level: int = logging.INFO,
) -> logging.Logger:
    """
    Return a logger named `name` writing to `log_dir / f"{name}.log"`
    and also to stdout.

    Idempotent — safe to call multiple times.
    """
    logger = logging.getLogger(name)
    if getattr(logger, "_sm_configured", False):
        return logger

    logger.setLevel(level)
    logger.propagate = False

    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{name}.log"
    fh = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    fh.setFormatter(_FMT)
    logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(_FMT)
    logger.addHandler(ch)

    for noisy in ("urllib3", "requests"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    logger._sm_configured = True  # type: ignore[attr-defined]
    return logger
