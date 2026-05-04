# Deployment Guide

## Local Development
1. Salin `.env.example` menjadi `.env` dan sesuaikan konfigurasi.
2. Jalankan node:
   - Lock Manager: `NODE_ROLE=lock_manager python -m src.main`
   - Queue Node: `NODE_ROLE=queue_node python -m src.main`
   - Cache Node: `NODE_ROLE=cache_node python -m src.main`

Tidak diperlukan database eksternal — semua data disimpan secara lokal di memori dan file WAL.

## Docker Compose
1. Jalankan `docker compose -f docker/docker-compose.yml up --build`.
2. Akses endpoint health masing-masing node (contoh: http://localhost:7001/health).

## Troubleshooting
- Jika node tidak menemukan leader, cek log dan pastikan semua peer dapat diakses.
- Jika queue tidak mengembalikan pesan, pastikan visibility timeout cukup besar dan cek file WAL di direktori `./data/`.
- Untuk cache, pastikan semua node cache bisa saling berkomunikasi melalui HTTP (periksa PEERS env var).
