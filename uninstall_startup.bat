@echo off
set DST=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\RAT2_Agent.vbs
if exist "%DST%" (
    del "%DST%"
    echo RAT2 Agent removed from startup.
) else (
    echo RAT2 Agent was not installed in startup.
)
pause
