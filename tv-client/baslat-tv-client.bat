@echo off
chcp 65001 > nul
set "TV_URL=http://localhost:8080/tv/"
if not "%~1"=="" set "TV_URL=%~1"

echo ===================================================
echo   IPTV P2P Smart TV Istemcisi (Tam Ekran) Aciliyor...
echo   Adres: %TV_URL%
echo ===================================================

where msedge >nul 2>&1
if %errorlevel%==0 (
    start "" msedge --app="%TV_URL%" --start-fullscreen
    exit /b 0
)

if exist "%ProgramFiles(x86)%\Microsoft\Edge\Application\msedge.exe" (
    start "" "%ProgramFiles(x86)%\Microsoft\Edge\Application\msedge.exe" --app="%TV_URL%" --start-fullscreen
    exit /b 0
)

if exist "%ProgramFiles%\Google\Chrome\Application\chrome.exe" (
    start "" "%ProgramFiles%\Google\Chrome\Application\chrome.exe" --app="%TV_URL%" --start-fullscreen
    exit /b 0
)

start "" "%TV_URL%"
