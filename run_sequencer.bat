@echo off
cd /d "%~dp0"
if exist ".venv\Scripts\python.exe" (
    .venv\Scripts\python.exe sequencer.py --daemon
) else (
    python sequencer.py --daemon
)
