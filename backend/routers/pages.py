"""
HTML pages served directly by FastAPI (as opposed to static files mounted
under /ui/). Kept separate from the API router so route surfaces don't
get tangled.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse


# Resolve <repo>/frontend from this file's location so the path works no
# matter what CWD uvicorn was launched from.
FRONTEND_DIR = Path(__file__).resolve().parent.parent.parent / "frontend"

router = APIRouter(tags=["pages"])


@router.get("/relatr")
async def relatr_page():
    path = FRONTEND_DIR / "relatr.html"
    if not path.exists():
        raise HTTPException(status_code=500, detail=f"UI file missing: {path}")
    return FileResponse(
        path, media_type="text/html",
        headers={"Cache-Control": "no-store"},
    )
