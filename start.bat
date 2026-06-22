@echo off
title Airdrop Hunter
echo.
echo   ===================================
echo     Airdrop Hunter v1.0
echo     http://127.0.0.1:8899
echo   ===================================
echo.
echo   Starting server...
start "" python "%~dp0backend\server.py"
timeout /t 2 >nul
start http://127.0.0.1:8899
echo.
echo   Server running. Close this window to stop.
echo.
pause
