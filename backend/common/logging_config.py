"""
Project-wide logging setup.

Two entry points:

  * ``setup_logging(name, log_dir)`` -- per-subsystem logger used by
    batch scripts (e.g. stock_universe). Writes to ``log_dir/<name>.log``
    with propagate=False so multiple named subsystems in the same
    process don't step on each other.

  * ``setup_app_logging(log_dir)``   -- configures the ROOT logger for the
    FastAPI process. Every module-level ``logger = logging.getLogger(__name__)``
    (backend.datapipe.historian, backend.datapipe.livestream, etc.) inherits
    from root, so a single call here surfaces everything to stdout AND to a
    rolling ``app.log``.

The FastAPI lifespan calls ``setup_app_logging`` exactly once at startup.
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
    and also to stdout. Used by batch scripts.

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


def setup_app_logging(
    log_dir: Path,
    level: int = logging.INFO,
) -> None:
    """
    Configure the ROOT logger for the FastAPI app. All module loggers
    (backend.*) inherit from root, so this one call routes every log line
    from the whole app to stdout.

    Stdout only -- no file handler. The per-bar INFO lines used to blow
    ``app.log`` up to gigabytes over a session, so file logging is off.
    ``log_dir`` is kept in the signature for compatibility but no longer
    written to.

    Idempotent -- replaces any handlers we've attached before so it's
    safe to call from the lifespan on every reload.
    """
    root = logging.getLogger()
    root.setLevel(level)

    # Remove any handlers we installed on a prior call (survives --reload)
    for h in list(root.handlers):
        if getattr(h, "_sm_owned", False):
            root.removeHandler(h)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(_FMT)
    ch._sm_owned = True  # type: ignore[attr-defined]
    root.addHandler(ch)

    # Quiet the loudest third-party libraries so our INFO signal isn't buried
    for noisy in ("urllib3", "requests", "aiohttp.access", "asyncio", "websockets.client"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    # Uvicorn's own loggers stay at INFO; make sure they render alongside ours
    for uv in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(uv).setLevel(logging.INFO)
