@echo off
cd /d "%~dp0"
title RAT2 Manager
start "" python -u rat2.py
timeout /t 2 /nobreak >nul

set BRAVE=%LOCALAPPDATA%\BraveSoftware\Brave-Browser\Application\brave.exe
set BRAVE2=%PROGRAMFILES%\BraveSoftware\Brave-Browser\Application\brave.exe
set BRAVE3=%PROGRAMFILES(X86)%\BraveSoftware\Brave-Browser\Application\brave.exe
set URL=http://localhost:8080
set ARGS=--new-window --window-size=1400,900 --window-position=100,80

if exist "%BRAVE%"  ( start "" "%BRAVE%"  %ARGS% "%URL%" & goto :eof )
if exist "%BRAVE2%" ( start "" "%BRAVE2%" %ARGS% "%URL%" & goto :eof )
if exist "%BRAVE3%" ( start "" "%BRAVE3%" %ARGS% "%URL%" & goto :eof )

start msedge  %ARGS% "%URL%" 2>nul || ^
start chrome  %ARGS% "%URL%" 2>nul || ^
start "" "%URL%"
