@echo off
chcp 65001 > nul
echo ===================================================
echo   IPTV P2P Sunucusu Baslatiliyor...
echo ===================================================
echo [1/2] Go kütüphaneleri indiriliyor (go mod tidy)...
go mod tidy
echo.
echo [2/2] Admin Paneli Tarayıcıda Açılıyor: http://localhost:8080/admin
start http://localhost:8080/admin
echo.
go run cmd/server/main.go
pause
