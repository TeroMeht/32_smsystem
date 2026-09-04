@echo off
REM ---------------------------------------------------------------
REM 32_smsystem -- launch the FastAPI + datapipe app.
REM Uses the venv's python directly, so it's immune to whatever
REM Python is on the system PATH.
REM ---------------------------------------------------------------

cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
    echo [start.bat] .venv not found.
    echo [start.bat] Run .\deploy.ps1 first, or the first-time setup in DEPLOY.md.
    pause
    exit /b 1
)

echo [start.bat] launching uvicorn on http://127.0.0.1:8001
echo [start.bat] Ctrl+C to stop.
echo.

.venv\Scripts\python.exe -m uvicorn backend.main:app --host 127.0.0.1 --port 8001

echo.
echo [start.bat] uvicorn exited with code %errorlevel%.
pause