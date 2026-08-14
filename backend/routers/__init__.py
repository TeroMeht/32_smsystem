"""
HTTP route modules for the FastAPI app. Each submodule exports a
``router: APIRouter`` that ``main.py`` composes with ``include_router``.
Route bodies MUST NOT contain SQL -- all persistence goes through
``backend.database``.
"""
