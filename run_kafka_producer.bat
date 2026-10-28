@echo off
title Kafka Streaming Producer - Spotify Real-time Ingestion
cls
echo =============================================================================
echo    TRUONG DAI HOC CONG THUONG TP.HCM (HUIT)
echo    DO AN BIG DATA: PHAN TICH XU HUONG AM NHAC SPOTIFY (2017 - 2023)
echo    Nhom SV: NGUYEN HA TRONG PHUC - LE DONG TRIEU - NGUYEN VU NGOC NAM
echo    BUOC 8: KAFKA STREAMING INGESTION (REAL-TIME SIMULATION)
echo =============================================================================
echo.
echo [*] Khoi dong Kafka Streaming Producer...
echo [*] Broker dich: localhost:9092
echo [*] Topic dich: spotify_streaming
echo.

"C:\Users\tphuc\anaconda3\python.exe" kafka_producer.py --file spotify_top200_sample.csv --min-delay 1.0 --max-delay 2.0

echo.
pause
