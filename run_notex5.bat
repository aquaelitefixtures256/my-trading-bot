@echo off
REM --- robust run_notex5.bat ---
REM ensure the script runs from its containing folder
pushd "%~dp0"

REM ==== adjust these values if needed ====
REM Path to MT5 terminal executable (adjust if different)
set MT5_PATH=C:\Program Files\MetaTrader 5\terminal64.exe

REM Session-only env (edit values on EC2)
set MT5_LOGIN=298105279
set MT5_PASSWORD=Haja12@#
set MT5_SERVER=Exness-MT5Trial9
set CONFIRM_AUTO=I UNDERSTAND THE RISKS
set TELEGRAM_BOT_TOKEN=8441599191:AAHK5NjJ7glha2DoyAvdZImDyYb-fYWtMps
set TELEGRAM_CHAT_ID=8025669850
set DECISION_SLEEP=60
REM If you want live trading, uncomment the next line (only when ready)
REM set CONFIRM_AUTO=I UNDERSTAND THE RISKS

REM Start MT5 if not already running (safe check)
tasklist /FI "IMAGENAME eq terminal64.exe" | find /I "terminal64.exe" >nul
if errorlevel 1 (
  echo Starting MT5 terminal...
  start "" "%MT5_PATH%"
  REM wait a little for terminal to initialize (adjust seconds if needed)
  timeout /t 6 /nobreak >nul
) else (
  echo MT5 already running
)

REM Activate venv and run bot
if exist "%~dp0venv\Scripts\activate.bat" (
  call "%~dp0venv\Scripts\activate.bat"
) else (
  echo ERROR: venv activate script not found at "%~dp0venv\Scripts\activate.bat"
  echo Press any key to exit...
  pause >nul
  popd
  exit /b 1
)

REM Run the bot and append log
python "%~dp0Notex5.py" >> "%~dp0logs\notex5.log" 2>&1

REM Done
popd
