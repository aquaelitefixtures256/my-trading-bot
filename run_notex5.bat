@echo off
REM ---------- diagnostic + robust run_notex5.bat ----------
REM Start in the batch file folder
pushd "%~dp0"
echo Working folder: %CD%

REM 1) Check python venv
if exist "%~dp0venv\Scripts\activate.bat" (
  echo venv activate found: %~dp0venv\Scripts\activate.bat
) else (
  echo ERROR: venv activate NOT found at "%~dp0venv\Scripts\activate.bat"
)

if exist "%~dp0venv\Scripts\python.exe" (
  echo venv python found: %~dp0venv\Scripts\python.exe
) else (
  echo ERROR: venv python NOT found at "%~dp0venv\Scripts\python.exe"
)

REM 2) Ensure logs folder exists (create if missing)
if not exist "%~dp0logs" (
  echo logs folder missing — creating logs folder
  mkdir "%~dp0logs"
) else (
  echo logs folder exists
)

REM 3) MT5 path (edit below if your MT5 is in a different path)
set MT5_PATH=C:\Program Files\MetaTrader 5\terminal64.exe
echo Checking MT5 path: "%MT5_PATH%"
if exist "%MT5_PATH%" (
  echo MT5 terminal found.
) else (
  echo ERROR: MT5 terminal NOT found at "%MT5_PATH%"
)

REM 4) Optional: show full directory listing for quick inspection
echo ---- Directory listing (top) ----
dir "%~dp0" /b
echo ----------------------------------

REM 5) Start MT5 if not running
tasklist /FI "IMAGENAME eq terminal64.exe" | find /I "terminal64.exe" >nul
if errorlevel 1 (
  echo MT5 not running — attempting to start "%MT5_PATH%"
  start "" "%MT5_PATH%"
  timeout /t 6 /nobreak >nul
) else (
  echo MT5 already running.
)

REM 6) Activate venv (will print error if not found)
if exist "%~dp0venv\Scripts\activate.bat" (
  call "%~dp0venv\Scripts\activate.bat"
) else (
  echo ERROR: cannot activate venv (file missing)
  pause
  popd
  exit /b 1
)

REM 7) Print python version from venv
where python
python --version

REM 8) Final check: Notex5.py present?
if exist "%~dp0Notex5.py" (
  echo Found Notex5.py
) else (
  echo ERROR: Notex5.py NOT found in "%~dp0"
  pause
  popd
  exit /b 1
)

REM 9) Run the bot and append logs (this will show any runtime error)
echo Starting Notex5.py — writing logs to "%~dp0logs\notex5.log"
python "%~dp0Notex5.py" >> "%~dp0logs\notex5.log" 2>&1

echo Bot finished (or crashed). See logs\notex5.log for details.
echo Press any key to exit...
pause

popd
