@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

set UV=.\bin\uv.exe
set HELPER=.\bin\_prep_helper.py

if not exist "%UV%" (
    echo [Error] bin\uv.exe not found.
    echo Download it from https://github.com/astral-sh/uv/releases
    echo and place it in the bin\ folder.
    exit /b 1
)

:: Initialize project if pyproject.toml is missing
if not exist "pyproject.toml" (
    echo Initializing project...
    "%UV%" init
)

:: Parse root Python version
for /f "usebackq" %%V in (`python %HELPER% root-version`) do set "PYVER=%%V"
if not defined PYVER (
    echo [Error] Could not parse requires-python from pyproject.toml
    exit /b 1
)
echo Root Python version: %PYVER%

:: If arguments were passed, add them as dependencies
if not "%~1"=="" (
    echo Adding dependencies: %*
    "%UV%" add %*
)

:: Ask if user wants a full clean reinstall
echo.
set "CLEAN=N"
set /p "CLEAN=Clean reinstall? Removes old Python versions + wheels before reinstalling [y/N]: "
if /i "%CLEAN%"=="y" (
    echo.
    echo Cleaning bin\python...
    if exist ".\bin\python" rd /s /q ".\bin\python"
    echo Cleaning vendor wheels...
    if exist "vendor" del /q vendor\*.whl 2>nul
    for /f "usebackq" %%D in (`python %HELPER% subprojects`) do (
        if exist "%%D\vendor" del /q "%%D\vendor\*.whl" 2>nul
    )
    echo Done cleaning.
)

:: Ensure all required Python versions are installed (skips if already present)
echo.
echo Checking portable Python versions...
for /f "usebackq" %%V in (`python %HELPER% all-versions`) do (
    echo   Ensuring Python %%V...
    "%UV%" python install %%V --install-dir .\bin\python
)

:: Vendor wheels for root project (download missing, clean stale)
if not exist "vendor" mkdir vendor
echo.
echo Vendoring wheels for root project ^(Python %PYVER%^)...
"%UV%" pip compile pyproject.toml -o _requirements.txt
pip download -d .\vendor --only-binary=:all: --python-version %PYVER% --platform win_amd64 -r _requirements.txt
python %HELPER% clean-vendor vendor _requirements.txt
del _requirements.txt

:: Vendor wheels for each subproject
for /f "usebackq" %%D in (`python %HELPER% subprojects`) do (
    for /f "usebackq" %%V in (`python %HELPER% subproject-version %%D`) do set "SUBVER=%%V"
    if not defined SUBVER set "SUBVER=%PYVER%"
    echo.
    echo Vendoring wheels for %%D ^(Python !SUBVER!^)...
    if not exist "%%D\vendor" mkdir "%%D\vendor"
    "%UV%" pip compile "%%D\pyproject.toml" -o _requirements.txt
    pip download -d "%%D\vendor" --only-binary=:all: --python-version !SUBVER! --platform win_amd64 -r _requirements.txt
    python %HELPER% clean-vendor "%%D\vendor" _requirements.txt
    del _requirements.txt
)

echo.
echo Done. All wheels vendored.
endlocal
