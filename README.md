# AQI Watch Jakarta

Sistem prediksi kualitas udara Kota Jakarta berbasis Big Data pipeline on-premise.
Data mengalir dari sensor simulasi → Kafka → Spark → MinIO + PostgreSQL → ML inference → Grafana dashboard.

## Prasyarat

- Docker Desktop (sudah include Docker Compose v2)
- Python 3.11+
- Git

## Cara Menjalankan

### 1. Clone repo

```bash
git clone https://github.com/username/aqi-watch-jakarta.git
cd aqi-watch-jakarta
```

### 2. Setup environment

```bash
cp .env.example .env
# Edit .env — isi nilai yang masih placeholder (lihat komentar di dalam file)
```

### 3. Jalankan setup otomatis

```bash
bash setup.sh
```

Script ini akan: pull Docker images, jalankan semua service, tunggu hingga siap, lalu tampilkan URL akses.

### 4. Jalankan sensor simulator

```bash
cd producer
pip install kafka-python
python sensor_simulator.py
```

## Akses Dashboard

| Service        | URL                       | Kredensial         |
|----------------|---------------------------|-------------------|
| Grafana        | http://localhost:3000     | admin / admin123  |
| Airflow        | http://localhost:8080     | admin / admin123  |
| MLflow         | http://localhost:5000     | -                 |
| MinIO Console  | http://localhost:9001     | minioadmin / ...  |
| Spark UI       | http://localhost:8081     | -                 |
| Prometheus     | http://localhost:9090     | -                 |

## Struktur Folder

```
aqi-watch-jakarta/
├── docker-compose.yml        # Definisi semua service
├── .env                      # Environment variables (tidak di-commit)
├── setup.sql                 # Schema PostgreSQL (auto-run saat container naik)
├── setup.sh                  # Script setup otomatis
├── producer/
│   └── sensor_simulator.py  # Simulator 5 stasiun sensor Jakarta
├── airflow/
│   └── dags/                # DAG batch ETL harian & retraining ML mingguan
├── spark/
│   ├── batch_etl.py         # Spark batch job (CSV → PostgreSQL)
│   └── stream_processor.py  # Spark Structured Streaming (Kafka → PostgreSQL)
├── ml/
│   ├── train.py             # Training Random Forest + log ke MLflow
│   └── inference.py         # Inference helper
├── grafana/
│   └── dashboards/          # Dashboard JSON (import ke Grafana)
├── prometheus/
│   ├── prometheus.yml       # Konfigurasi scrape
│   ├── alertmanager.yml     # Konfigurasi alert ke Telegram
│   └── rules/               # Alert rules YAML
├── data/
│   └── raw/                 # Dataset CSV Jakarta (tidak di-commit, download manual)
├── notebooks/               # Jupyter notebook eksplorasi dataset
└── docs/                    # Dokumentasi teknis & troubleshooting
```

## Menghentikan Semua Service

```bash
docker compose down
# Hapus volume juga (reset total):
docker compose down -v
```

## Tim

| Nama  | Peran                              |
|-------|------------------------------------|
| Kamu  | Data Engineering & ML              |
| Kayla | Serving, Monitoring & Dokumentasi  |
