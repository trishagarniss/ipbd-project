#!/bin/bash
# run_pipeline.sh — E2E pipeline: infra → extract → ETL → ML train → predict
# Jalankan: bash run_pipeline.sh

set -e

echo "======================================"
echo " AQI Watch Surakarta — Full Pipeline"
echo "======================================"

# 1. Setup & start infrastructure
echo ""
echo "[1/6] Mengecek .env..."
if [ ! -f .env ]; then
  echo "ERROR: .env tidak ditemukan! Copy dari .env.example"
  exit 1
fi

echo "[2/6] Menjalankan Docker Compose..."
docker compose up -d
echo "Tunggu 30 detik untuk service siap..."
sleep 30

echo "[3/6] Extract data historis (1 tahun)..."
uv run python spark/batch_extract.py

echo "[4/6] ETL batch (Spark)..."
docker compose exec spark-master env HOME=/tmp \
  PYTHONPATH=/.local/lib/python3.12/site-packages \
  spark-submit \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.7.1 \
  --conf spark.sql.ansi.enabled=false \
  /opt/airflow/spark/batch_etl.py

echo "[5/6] ML Training + Predict..."
uv run python ml/train.py
uv run python ml/predict.py

echo "[6/6] Selesai!"
echo ""
echo "Akses dashboard:"
echo "  Grafana   -> http://localhost:3000 (admin / admin123)"
echo "  Airflow   -> http://localhost:8080 (admin / admin123)"
echo "  Spark UI  -> http://localhost:8081"
echo ""
echo "Streaming (opsional):"
echo "  bash spark/submit_streaming.sh  # submit Spark streaming"
echo "  uv run python producer/api_ingestor.py  # start API ingestor"
