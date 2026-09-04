@echo off
REM ---------------------------------------------------------------
REM 32_smsystem -- launch the FastAPI + datapipe app.
REM Uses the venv's python directly, so it's immune to whatever
REM Python is on the system PATH.
REM
REM After the server is up on port 8001, opens the /relatr dashboard
REM in the default browser automatically. The opener is a background
REM PowerShell that polls the port with a .NET TCP connect (fast,
REM sub-second), so we don't race uvicorn's startup.
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

REM Fire the browser opener in the background BEFORE starting uvicorn.
REM It waits until 127.0.0.1:8001 accepts a TCP connection, then opens
REM /relatr in the default browser. Runs hidden; no extra window pops up.
start "" /B powershell -NoProfile -WindowStyle Hidden -Command ^
  "while ($true) { try { (New-Object Net.Sockets.TcpClient).Connect('127.0.0.1',8001); break } catch { Start-Sleep -Milliseconds 250 } }; Start-Process 'http://127.0.0.1:8001/relatr'"

.venv\Scripts\python.exe -m uvicorn backend.main:app --host 127.0.0.1 --port 8001

echo.
echo [start.bat] uvicorn exited with code %errorlevel%.
pause
