@echo off
title Spotify Big Data Analytics Dashboard (2017 - 2023)
cls
echo =============================================================================
echo    TRUONG DAI HOC CONG THUONG TP.HCM (HUIT)
echo    DO AN BIG DATA: PHAN TICH XU HUONG AM NHAC SPOTIFY (2017 - 2023)
echo    Nhom SV: NGUYEN HA TRONG PHUC - LE DONG TRIEU - NGUYEN VU NGOC NAM
echo =============================================================================
echo.
echo [*] Dang kiem tra va giai phong cong 8501 neu bi trung...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr /r ":8501 "') do (
    taskkill /f /pid %%a >nul 2>&1
)

echo [*] Dang khoi dong Streamlit Web Dashboard...
echo [*] Trinh duyet se tu dong mo tai: http://localhost:8501
echo.

"C:\Users\tphuc\anaconda3\python.exe" -m streamlit run app.py --server.port 8501 --server.headless false --browser.gatherUsageStats false

echo.
pause
