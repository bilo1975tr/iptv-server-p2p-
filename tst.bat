@echo off
chcp 65001 > nul
echo ===================================================
echo   IPTV P2P Web Test Oynatıcısı Açılıyor...
echo ===================================================
start http://localhost:8080/player.html?channel_id=ch_spor1
echo Oynatıcı Tarayıcıda Açıldı: http://localhost:8080/player.html?channel_id=ch_spor1
