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
├── docker-compose.yml        # 15 service (infrastruktur penuh)
├── .env                      # Kredensial — jangan di-share
├── .gitignore
├── run_pipeline.sh           # E2E pipeline script
├── sql/
│   └── setup.sql             # Schema PostgreSQL (5 tabel)
├── config/
│   ├── locations.json        # 5 stasiun Surakarta (SKA1–SKA5)
│   └── api_settings.yaml     # Konfigurasi API & Kafka
├── spark/
│   ├── batch_extract.py      # API historis 1 tahun → MinIO
│   ├── batch_etl.py          # Spark batch → clean → agg → PostgreSQL
│   ├── stream_processor.py   # Spark Streaming → windowed agg
│   └── submit_streaming.sh   # Script submit streaming job
├── producer/
│   └── api_ingestor.py       # API → Kafka (streaming real-time)
├── airflow/
│   └── dags/
│       ├── batch_ingest.py   # DAG harian (extract → ETL) + audit
│       └── ml_retrain.py     # DAG mingguan + Telegram notif
├── ml/
│   ├── ispu.py               # Perhitungan ISPU (Permen LHK No.14/2020)
│   ├── train.py              # Random Forest + KMeans → MLflow
│   ├── predict.py            # Load model → batch predict
│   ├── validation.py         # Data quality check
│   └── requirements.txt
├── scripts/
│   └── generate_configs.py   # Generate konfigurasi dari .env
├── prometheus/
│   ├── prometheus.yml        # Scrape config
│   ├── alertmanager.yml.tmpl # Template Telegram alert
│   └── rules/aqi_alerts.yml  # 5 alert rules
├── grafana/
│   ├── datasources/          # Auto-provisioning PostgreSQL
│   └── dashboards/           # Auto-provisioning dashboard ISPU
└── docs/
    ├── metadata.md           # Metadata 5 tabel database
    ├── run-demo.md           # Tutorial demo end-to-end
    ├── dashboard.md          # Tujuan & insight dashboard
    └── troubleshooting.md    # Common issues
```

## Cara Menjalankan

### 1. Environment

```bash
cp .env.example .env
# Isi kredensial di .env (Telegram token, dll)
```

### 2. Full Pipeline (Otomatis)

```bash
bash run_pipeline.sh
```

Atau step-by-step:

```bash
# Start infrastruktur
docker compose up -d

# Extract data historis 1 tahun
uv run python spark/batch_extract.py

# ETL ke daily_aqi (Spark)
docker compose exec spark-master env HOME=/tmp \
  PYTHONPATH=/.local/lib/python3.12/site-packages \
  spark-submit --packages org.postgresql:postgresql:42.7.1 \
  --conf spark.sql.ansi.enabled=false \
  /opt/airflow/spark/batch_etl.py

# ML Training + Predict
uv run python ml/train.py
uv run python ml/predict.py
```

### 3. Streaming (Real-time)

Terminal 1 — API ingestor ke Kafka:
```bash
uv run python producer/api_ingestor.py
```

Terminal 2 — Spark streaming processor:
```bash
bash spark/submit_streaming.sh
```

### 4. Dashboard

- Grafana: http://localhost:3000 (admin / admin123)
- Dashboard ISPU auto-provisioning dari `grafana/dashboards/`

### 5. Scheduling (Airflow)

DAG `batch_ingest` jalan setiap hari 06:30 WIB.
DAG `ml_retrain` jalan setiap Senin 08:00 WIB + notif Telegram.

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
PM2.5, PM10, CO, NO2, SO2, O3

### Indeks Kualitas Udara
ISPU (0–500) — 6 parameter, standar Indonesia Permen LHK No.14/2020

### Meteorologi
Suhu, Kelembaban, Kecepatan Angin, Curah Hujan, Tutupan Awan

## Keamanan

### Informasi Sensitif di `.env`
File `.env` berisi kredensial asli untuk:
- PostgreSQL (user, password)
- MinIO (access key, secret key)
- Airflow (user, password, Fernet key)
- Grafana (user, password)
- Telegram (bot token, chat ID)

**Jangan commit `.env` ke GitHub.** File ini sudah masuk `.gitignore`.

### PII (Personally Identifiable Information)
Proyek ini **tidak mengumpulkan PII**:
- Data yang di-fetch hanya koordinat stasiun publik dan data meteorologi
- Tidak ada data pengguna, lokasi personal, atau identitas individu
- Semua data bersifat anonim dan agregat

### Menghentikan Service

```bash
docker compose down          # Stop semua service
docker compose down -v       # Stop + hapus volume (data hilang)
```
