@echo off
REM ---------------------------------------------------------------
REM 32_smsystem -- launch the FastAPI + datapipe app
REM
REM Runs from wherever this .bat file lives (%~dp0), so shortcuts,
REM double-click from Explorer, or `start.bat` in another cwd all
REM work. Binds to localhost only (per the current deployment
REM decision -- reachable from a browser on this machine, not from
REM the network).
REM ---------------------------------------------------------------

cd /d "%~dp0"

REM Fail loud if Python isn't on PATH -- otherwise uvicorn errors
REM are noisy and misleading.
where python >nul 2>&1
if errorlevel 1 (
    echo [start.bat] python not found on PATH. Install Python 3.13 or
    echo             add it to PATH, then re-run this script.
    pause
    exit /b 1
)

echo [start.bat] launching uvicorn on http://127.0.0.1:8001
echo [start.bat] Ctrl+C to stop.
echo.

python -m uvicorn backend.main:app --host 127.0.0.1 --port 8001

REM Keep the window open if uvicorn exits (crash / port in use)
REM so the traceback is readable instead of the window slamming shut.
echo.
echo [start.bat] uvicorn exited (code %errorlevel%).
pause
