@echo off
cd /d %~dp0
REM ===== session-only env vars (edit values here on EC2 only) =====
set MT5_LOGIN=298105279
set MT5_PASSWORD=Haja12@#
set MT5_SERVER=Exness-MT5Trial9
set TELEGRAM_BOT_TOKEN=8441599191:AAHK5NjJ7glha2DoyAvdZImDyYb-fYWtMps
set TELEGRAM_CHAT_ID=8025669850
set DECISION_SLEEP=60
REM ===== activate venv and run bot =====
call venv\Scripts\activate.bat
REM run and append logs
python Notex5.py >> logs\notex5.log 2>&1
