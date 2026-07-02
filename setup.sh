#!/bin/bash
# setup.sh — Inisialisasi project AQI Watch Surakarta
# Jalankan sekali setelah clone repo: bash setup.sh

set -e
echo "======================================"
echo " AQI Watch Surakarta — Setup"
echo "======================================"

# Deteksi Python interpreter (prefer uv, fallback python3/python)
PYTHON=""
if command -v uv &>/dev/null; then
  PYTHON="uv run python"
elif command -v python3 &>/dev/null; then
  PYTHON="python3"
elif command -v python &>/dev/null; then
  PYTHON="python"
else
  echo "ERROR: Python interpreter tidak ditemukan."
  echo "Install uv (https://docs.astral.sh/uv/) atau Python."
  exit 1
fi
echo "  Python: $PYTHON"

# Deteksi Docker command
DOCKER=""
if command -v docker.exe &>/dev/null; then
  DOCKER="docker.exe"
elif command -v docker &>/dev/null && docker info &>/dev/null 2>&1; then
  DOCKER="docker"
elif command -v docker &>/dev/null; then
  echo "WARNING: 'docker' ditemukan tapi Docker daemon tidak merespon."
  echo "  Coba enable WSL integration di Docker Desktop, atau pastikan Docker Desktop running."
  exit 1
else
  echo "ERROR: Docker tidak ditemukan."
  echo "  Pastikan Docker Desktop sudah terinstall dan running."
  exit 1
fi
echo "  Docker: $DOCKER"

# 1. Cek .env ada
if [ ! -f .env ]; then
  echo "ERROR: file .env tidak ditemukan!"
  echo "Copy .env.example ke .env dan isi nilainya dulu."
  exit 1
fi

# 2. Generate Airflow Fernet Key kalau belum diisi
if grep -q "AIRFLOW_FERNET_KEY=CHANGE_ME" .env; then
  echo ""
  echo "Generating Airflow Fernet Key..."
  FERNET=$($PYTHON -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
  sed -i "s|AIRFLOW_FERNET_KEY=CHANGE_ME|AIRFLOW_FERNET_KEY=$FERNET|g" .env
  echo "Fernet Key berhasil di-generate dan disimpan ke .env"
fi

# 3. Generate config dari template (alertmanager.yml)
echo ""
echo "Generating config files from templates..."
$PYTHON scripts/generate_configs.py

# 4. Pull semua image
echo ""
echo "Pulling Docker images..."
$DOCKER compose pull </dev/null

# 5. Jalankan stack
echo ""
echo "Menjalankan semua service..."
$DOCKER compose up -d </dev/null

# 6. Tunggu PostgreSQL siap
echo ""
echo "Menunggu PostgreSQL siap..."
until $DOCKER compose exec -T postgres pg_isready -U aqi_user -d aqi_db > /dev/null 2>&1; do
  echo "  PostgreSQL belum siap, tunggu 3 detik..."
  sleep 3
done
echo "PostgreSQL siap!"

# 7. Tunggu Kafka siap
echo ""
echo "Menunggu Kafka siap..."
sleep 15
echo "Kafka siap!"

# 8. Install psycopg2 di spark-master (untuk batch_etl append mode)
echo ""
echo "Installing psycopg2-binary di spark-master..."
$DOCKER compose exec -u root spark-master pip install psycopg2-binary -q
echo "psycopg2-binary siap!"

# 9. Summary akses
echo ""
echo "========================================"
echo " Setup selesai! Akses via browser"
echo "========================================"
echo " Airflow       -> http://localhost:8080"
echo " Grafana       -> http://localhost:3000"
echo " MinIO Console -> http://localhost:9001"
echo " Spark UI      -> http://localhost:8081"
echo " Prometheus    -> http://localhost:9090"
echo " MLflow        -> http://localhost:5000"
echo ""
echo " Jalankan API Ingestor (Streaming):"
echo " cd producer && uv pip install -r requirements.txt && $PYTHON api_ingestor.py"
echo "========================================"