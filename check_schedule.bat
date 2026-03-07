@echo off
cd /d "%~dp0"
if exist ".venv\Scripts\python.exe" (
    .venv\Scripts\python.exe check_schedule.py
) else (
    python check_schedule.py
)
pause
