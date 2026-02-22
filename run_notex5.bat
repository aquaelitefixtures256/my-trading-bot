@echo off
cd /d %~dp0

REM -- If terminal not running, start it (adjust path if needed) --
tasklist /FI "IMAGENAME eq terminal64.exe" | find /I "terminal64.exe" >nul || start "" "C:\Program Files\MetaTrader 5\terminal64.exe"

REM -- session env vars (edit values on EC2 only) --
set MT5_PATH=C:\Program Files\MetaTrader 5\terminal64.exe
set MT5_LOGIN=298105279
set MT5_PASSWORD=Haja12@#
set MT5_SERVER=Exness-MT5Trial9
set CONFIRM_AUTO=I UNDERSTAND THE RISKS
set TELEGRAM_BOT_TOKEN=8441599191:AAHK5NjJ7glha2DoyAvdZImDyYb-fYWtMps
set TELEGRAM_CHAT_ID=8025669850
set DECISION_SLEEP=60

call venv\Scripts\activate.bat
python Notex5.py >> logs\notex5.log 2>&1
