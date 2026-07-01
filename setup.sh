#!/bin/bash
# setup.sh — Inisialisasi project AQI Watch Jakarta
# Jalankan sekali setelah clone repo: bash setup.sh

set -e
echo "======================================"
echo " AQI Watch Jakarta — Setup"
echo "======================================"

# 1. Cek .env ada
if [ ! -f .env ]; then
  echo "ERROR: file .env tidak ditemukan!"
  echo "Copy .env.example ke .env dan isi nilainya dulu."
  exit 1
fi

# 2. Generate Airflow Fernet Key kalau belum diisi
if grep -q "GANTI_DENGAN_FERNET_KEY" .env; then
  echo ""
  echo "Generating Airflow Fernet Key..."
  FERNET=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
  sed -i "s|GANTI_DENGAN_FERNET_KEY_HASIL_GENERATE|$FERNET|g" .env
  echo "Fernet Key berhasil di-generate dan disimpan ke .env"
fi

# 3. Pull semua image
echo ""
echo "Pulling Docker images..."
docker compose pull

# 4. Jalankan stack
echo ""
echo "Menjalankan semua service..."
docker compose up -d

# 5. Tunggu PostgreSQL siap
echo ""
echo "Menunggu PostgreSQL siap..."
until docker compose exec -T postgres pg_isready -U aqi_user -d aqi_db > /dev/null 2>&1; do
  echo "  PostgreSQL belum siap, tunggu 3 detik..."
  sleep 3
done
echo "PostgreSQL siap!"

# 6. Tunggu Kafka siap
echo ""
echo "Menunggu Kafka siap..."
sleep 15
echo "Kafka siap!"

# 7. Summary akses
echo ""
echo "======================================"
echo " Setup selesai! Akses via browser:"
echo "======================================"
echo " Airflow       -> http://localhost:8080"
echo " Grafana       -> http://localhost:3000"
echo " MinIO Console -> http://localhost:9001"
echo " Spark UI      -> http://localhost:8081"
 echo " Prometheus    -> http://localhost:9090"
 echo " MLflow        -> http://localhost:5000"
echo ""
echo " Jalankan API Ingestor (Streaming):"
echo " cd producer && uv pip install -r requirements.txt && python api_ingestor.py"
echo "======================================"