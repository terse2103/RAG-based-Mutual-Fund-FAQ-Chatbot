@echo off
:: ============================================================
::  setup_task_scheduler.bat
::  Registers a Windows Task Scheduler job that runs the
::  data refresh pipeline every day at 10:00 AM IST.
::
::  Run this file ONCE (as Administrator) to install the task.
::  To remove the task later:
::      schtasks /Delete /TN "MFChatbotDailyRefresh" /F
:: ============================================================

setlocal EnableDelayedExpansion

:: ── Resolve the project root (directory containing this .bat) ──────────────
set "PROJECT_ROOT=%~dp0"
:: Strip trailing backslash
if "%PROJECT_ROOT:~-1%"=="\" set "PROJECT_ROOT=%PROJECT_ROOT:~0,-1%"

:: ── Find the Python executable ─────────────────────────────────────────────
where python >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python not found on PATH. Please install Python 3.11+ and add it to PATH.
    pause
    exit /b 1
)
for /f "delims=" %%i in ('where python') do set "PYTHON_EXE=%%i" & goto :found_python
:found_python

echo.
echo ============================================================
echo   Mutual Fund Chatbot — Daily Refresh Task Setup
echo ============================================================
echo   Project root : %PROJECT_ROOT%
echo   Python       : %PYTHON_EXE%
echo   Schedule     : Daily at 10:00 AM (IST)
echo   Script       : run_refresh.py
echo ============================================================
echo.

:: ── Register the task ──────────────────────────────────────────────────────
schtasks /Create ^
    /TN "MFChatbotDailyRefresh" ^
    /TR "\"%PYTHON_EXE%\" \"%PROJECT_ROOT%\run_refresh.py\"" ^
    /SC DAILY ^
    /ST 10:00 ^
    /RU "%USERNAME%" ^
    /RL HIGHEST ^
    /F ^
    /SD 01/01/2026

if errorlevel 1 (
    echo.
    echo [ERROR] Failed to create the scheduled task.
    echo         Try running this script as Administrator.
    pause
    exit /b 1
)

echo.
echo [OK] Task "MFChatbotDailyRefresh" created successfully!
echo      It will run every day at 10:00 AM using your Windows login.
echo.
echo To verify: Open Task Scheduler ^> Task Scheduler Library ^> MFChatbotDailyRefresh
echo To remove: schtasks /Delete /TN "MFChatbotDailyRefresh" /F
echo.
pause
