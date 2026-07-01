# Tutorial Demo End-to-End

## Prasyarat
- Docker Desktop terinstall dan running
- Python 3.14+ dengan uv
- Semua file source code sudah lengkap

## 1. Jalankan Infrastruktur

```bash
docker compose up -d
```

Tunggu ~5 menit hingga semua service siap. Cek log:

```bash
docker compose logs -f
```

## 2. Setup Database

```bash
# PostgreSQL akan auto-run setup.sql saat container pertama naik
# Verifikasi tabel:
docker compose exec postgres psql -U aqi_user -d aqi_db -c "\dt"
```

## 3. Pipeline Batch (30 Menit)

### 3a. Extract Data Historis

```bash
# Jalankan batch_extract — fetch 30 hari data historis Open-Meteo
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/scripts/batch_extract.py
```

Verifikasi file terupload:

```bash
docker compose exec minio mc ls minio/raw/
```

### 3b. ETL ke Daily AQI

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars/postgresql-42.7.1.jar \
  /opt/spark/scripts/batch_etl.py
```

Verifikasi:

```bash
docker compose exec postgres psql -U aqi_user -d aqi_db -c \
  "SELECT station_id, date, ispu, aqi_category FROM daily_aqi LIMIT 10;"
```

### 3c. Training ML Model

```bash
docker compose exec airflow python /opt/ml/train.py
```

Cek MLflow:

```bash
open http://localhost:5000
```

## 4. Pipeline Streaming (Lanjutan)

### 4a. Jalankan Producer

```bash
# Di terminal terpisah
cd producer
uv pip install -r requirements.txt
python api_ingestor.py
```

Producer akan fetch data real-time setiap 60 detik dan kirim ke Kafka.

### 4b. Jalankan Stream Processor

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages \
    org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
    org.apache.hadoop:hadoop-aws:3.3.4,\
    org.postgresql:postgresql:42.7.1 \
  /opt/spark/scripts/stream_processor.py
```

### 4c. Verifikasi Streaming

```bash
docker compose exec postgres psql -U aqi_user -d aqi_db -c \
  "SELECT station_id, window_start, ispu_avg FROM stream_agg ORDER BY window_start DESC LIMIT 10;"
```

## 5. Dashboard Grafana

1. Buka http://localhost:3000 (admin/admin123)
2. Add data source → PostgreSQL
3. Host: `postgres:5432`, Database: `aqi_db`, User: `aqi_user`, Password: `aqi_password_123`
4. Import dashboard dari `grafana/dashboards/`
5. Buat panel:
   - **Time series:** ISPU per stasiun (dari `stream_agg`)
   - **Stat:** AQI terkini per stasiun
   - **Bar chart:** Rata-rata harian PM2.5 (dari `daily_aqi`)
   - **Table:** Prediksi terakhir (dari `predictions`)
   - **Geomap:** Peta Surakarta dengan bubble warna AQI

## 6. Monitoring & Alerting

### Prometheus
- Buka http://localhost:9090
- Query: `ispu_avg > 100`

### Alertmanager
- Notifikasi Telegram otomatis jika ISPU > 200 atau PM2.5 > 55.4

## 7. Airflow Scheduling

### Aktifkan DAG
1. Buka http://localhost:8080 (admin/admin123)
2. Aktifkan DAG `batch_ingest` → schedule setiap hari 06:30
3. Aktifkan DAG `ml_retrain` → schedule setiap Senin 08:00

### Trigger Manual
```bash
docker compose exec airflow airflow dags trigger batch_ingest
docker compose exec airflow airflow dags trigger ml_retrain
```

## 8. Hentikan Demo

```bash
# Hentikan semua service
docker compose down

# Reset total (hapus volume — data PostgreSQL & MinIO hilang)
docker compose down -v
```
