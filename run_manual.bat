@echo off
cd /d "%~dp0"
if exist ".venv\Scripts\python.exe" (
    .venv\Scripts\python.exe bin\_manual_runner.py
) else (
    python bin\_manual_runner.py
)
pause
