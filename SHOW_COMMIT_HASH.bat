@echo off
cd /d "%~dp0"
git rev-parse HEAD
if errorlevel 1 echo Git is not yet initialized or Git is not installed. Publish through GitHub Desktop first.
pause
