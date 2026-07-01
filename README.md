# AQI Watch Surakarta

Sistem monitoring dan prediksi kualitas udara **Kota Surakarta** berbasis Big Data pipeline on-premise.

Data mengalir dari **Open-Meteo API** → **Producer** → **Kafka** → **Spark Streaming + Batch** → **MinIO + PostgreSQL** → **ML Training (Random Forest + KMeans)** → **Grafana dashboard** → **Alert Telegram**.

## Prasyarat

- Docker Desktop (Docker Compose v2)
- Python 3.14+ dengan `uv`
- Windows 11 (atau Linux/macOS dengan penyesuaian path)

## Struktur Folder

```
aqi-watch-surakarta/
├── docker-compose.yml        # 14 service (ZooKeeper, Kafka, PostgreSQL, MinIO,
│                             #   Spark master+worker, Airflow, MLflow,
│                             #   Prometheus+Alertmanager, Postgres+Kafka exporter,
│                             #   Grafana)
├── .env                      # Environment variables (KREDENSIAL ASLI — jangan di-share)
├── .gitignore
├── sql/
│   └── setup.sql             # Schema PostgreSQL (5 tabel)
├── config/
│   └── locations.json        # 5 stasiun Surakarta (SKA1–SKA5)
├── spark/
│   ├── batch_extract.py      # Python script (API historis 1 tahun → upload MinIO)
│   ├── batch_etl.py          # Spark batch (MinIO CSV → clean → daily agg → PostgreSQL + Parquet)
│   └── stream_processor.py   # Spark Structured Streaming (Kafka → windowed agg → PostgreSQL)
├── airflow/
│   └── dags/
│       ├── batch_ingest.py   # DAG ingest harian (extract → ETL) + audit
│       └── ml_retrain.py     # DAG retrain mingguan + Telegram notif
├── ml/
│   ├── train.py              # Random Forest + KMeans → MLflow
│   ├── predict.py            # Load model → batch predict → PostgreSQL
│   ├── validation.py         # Data quality check (custom SQL)
│   └── requirements.txt      # Dependencies ML
├── prometheus/
│   ├── prometheus.yml        # Scrape config
│   ├── alertmanager.yml.tmpl # Template alertmanager → Telegram
│   └── rules/aqi_alerts.yml  # 5 alert rules
├── grafana/
│   ├── datasources/          # Auto-provisioning: koneksi PostgreSQL
│   └── dashboards/           # Auto-provisioning: dashboard ISPU
├── docs/
│   ├── metadata.md           # Metadata 5 tabel database
│   ├── run-demo.md           # Tutorial demo end-to-end
│   ├── dashboard.md          # Tujuan & insight dashboard
│   └── troubleshooting.md    # Common issues & solusi
├── scripts/
│   └── generate_configs.py   # Generate konfigurasi dari .env
└── README.md
```

## Cara Menjalankan

### 1. Clone & Setup

```bash
git clone https://github.com/trishagarniss/ipbd-project.git
cd ipbd-project
```

### 2. Environment

File `.env` sudah berisi kredensial asli (PostgreSQL, MinIO, Airflow, Grafana, Telegram, OpenAQ). **Jangan commit ke GitHub.**

### 3. Jalankan Infrastruktur

```bash
docker compose up -d
```

Tunggu semua service siap (~5 menit). Cek dengan `docker compose ps`. Semua 15 container harus `Up`.

### 4. Pipeline Batch

```bash
# Extract data historis 1 tahun (dari host)
uv run python spark/batch_extract.py

# ETL ke daily_aqi (via spark-submit di spark-master)
docker compose exec spark-master env HOME=/tmp PYTHONPATH=/.local/lib/python3.12/site-packages spark-submit \
  --packages org.postgresql:postgresql:42.7.1 \
  --conf spark.sql.ansi.enabled=false \
  /opt/airflow/spark/batch_etl.py
```

### 5. ML Training & Predict

```bash
# Training model (Random Forest + KMeans)
uv run python ml/train.py

# Batch predict
uv run python ml/predict.py
```

### 6. Dashboard

Buka http://localhost:3000 (admin / admin123), add PostgreSQL data source, import dashboard JSON dari `grafana/dashboards/`.

## Akses Service

| Service           | URL                          | Kredensial                |
|-------------------|------------------------------|---------------------------|
| Grafana           | http://localhost:3000        | admin / admin123          |
| Airflow           | http://localhost:8080        | admin / admin123          |
| MLflow            | http://localhost:5000        | —                         |
| MinIO Console     | http://localhost:9001        | minioadmin / admin123     |
| Spark Master UI   | http://localhost:8081        | —                         |
| Prometheus        | http://localhost:9090        | —                         |
| Alertmanager      | http://localhost:9093        | —                         |

## Data Flow

```
Open-Meteo API (1 tahun historis)
    │
    ├── batch_extract.py ──► MinIO (raw/ CSV)
    │                              │
    │                              ▼
    │                         batch_etl.py (Spark)
    │                         clean → daily agg
    │                              │
    │                    ┌─────────┴─────────┐
    │                    ▼                   ▼
    │              daily_aqi table    MinIO (processed/ Parquet)
    │                    │
    │                    ▼
    │              ML Training (Random Forest + KMeans)
    │                    │
    │                    ▼
    │               MLflow (model registry)
    │                    │
    │                    ▼
    │              predict.py → predictions table
    │
    └───► PostgreSQL ◄── Kafka (streaming)
                │
                ▼
         Grafana Dashboard
         Alert Telegram
```

## Stasiun Pemantauan

5 stasiun yang mencakup seluruh kecamatan Kota Surakarta:

| ID   | Nama Stasiun          | Kecamatan      | Koordinat            |
|------|-----------------------|----------------|----------------------|
| SKA1 | SKA1 - Banjarsari     | Banjarsari     | 7.54°S, 110.83°E     |
| SKA2 | SKA2 - Jebres         | Jebres         | 7.56°S, 110.85°E     |
| SKA3 | SKA3 - Laweyan        | Laweyan        | 7.57°S, 110.80°E     |
| SKA4 | SKA4 - Pasar Kliwon   | Pasar Kliwon   | 7.57°S, 110.84°E     |
| SKA5 | SKA5 - Serengan       | Serengan       | 7.57°S, 110.82°E     |

## Variabel yang Dimonitor

### Polutan Udara
PM2.5, PM10, CO, NO2, SO2, O3, UV Index

### Indeks Kualitas Udara
ISPU (0–500)

### Meteorologi
Suhu, Kelembaban, Kecepatan Angin, Curah Hujan, Tutupan Awan

## Keamanan

### Informasi Sensitif di `.env`
File `.env` berisi kredensial asli untuk:
- PostgreSQL (user, password, database)
- MinIO (access key, secret key)
- Airflow (user, password)
- Grafana (user, password)
- Telegram (bot token, chat ID)
- OpenAQ (API key)

**Jangan commit `.env` ke GitHub.** File ini sudah masuk `.gitignore`.

### PII (Personally Identifiable Information)
Proyek ini **tidak mengumpulkan PII**:
- Data yang di-fetch hanya koordinat stasiun publik dan data meteorologi
- Tidak ada data pengguna, lokasi personal, atau identitas individu
- Semua data bersifat anonim dan agregat

Menghentikan Service

```bash
docker compose down          # Stop semua service
docker compose down -v       # Stop + hapus volume (data hilang)
```