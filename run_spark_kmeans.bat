@echo off
title PySpark RFM and KMeans Clustering - Spotify Big Data
cls
echo =============================================================================
echo    TRUONG DAI HOC CONG THUONG TP.HCM (HUIT)
echo    DO AN BIG DATA: PHAN TICH XU HUONG AM NHAC SPOTIFY (2017 - 2023)
echo    Nhom SV: NGUYEN HA TRONG PHUC - LE DONG TRIEU - NGUYEN VU NGOC NAM
echo    BUOC 6 VA 7: PYSPARK RFM VA K-MEANS MACHINE LEARNING
echo =============================================================================
echo.
echo [*] Dang khoi chay PySpark Job tren tap du lieu Spotify...
echo.

"C:\Users\tphuc\anaconda3\python.exe" pyspark_rfm_kmeans.py --input spotify_cleaned.parquet --output ./kmeans_results.parquet

echo.
echo =============================================================================
echo [*] Hoan tat! Ket qua phan cum da duoc luu tai kmeans_results.parquet
echo =============================================================================
pause
