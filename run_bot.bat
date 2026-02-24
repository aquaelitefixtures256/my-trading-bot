@echo off
REM Ignore sklearn ConvergenceWarning
set PYTHONWARNINGS=ignore::sklearn.exceptions.ConvergenceWarning

REM Run your bot
python Ultra_instinct7.0.py

pause
