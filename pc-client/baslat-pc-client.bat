@echo off
chcp 65001 > nul
set "CLIENT_URL=http://localhost:8080/pc/"
if not "%~1"=="" set "CLIENT_URL=%~1"

echo ===================================================
echo   IPTV P2P Masaustu Istemcisi Baslatiliyor...
echo   Adres: %CLIENT_URL%
echo ===================================================

where msedge >nul 2>&1
if %errorlevel%==0 (
    start "" msedge --app="%CLIENT_URL%" --window-size=1440,860
    exit /b 0
)

if exist "%ProgramFiles(x86)%\Microsoft\Edge\Application\msedge.exe" (
    start "" "%ProgramFiles(x86)%\Microsoft\Edge\Application\msedge.exe" --app="%CLIENT_URL%" --window-size=1440,860
    exit /b 0
)

if exist "%ProgramFiles%\Google\Chrome\Application\chrome.exe" (
    start "" "%ProgramFiles%\Google\Chrome\Application\chrome.exe" --app="%CLIENT_URL%" --window-size=1440,860
    exit /b 0
)

start "" "%CLIENT_URL%"
