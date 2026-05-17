-- ── DATABASE SETUP untuk MLflow ─────────────────────────────
CREATE DATABASE mlflow;

-- ── CONNECT ke aqi_db ────────────────────────────────────────
\c aqi_db;

-- ── TABEL 1: raw_measurements ────────────────────────────────
-- Menyimpan data mentah dari sensor/API sebelum diproses
CREATE TABLE IF NOT EXISTS raw_measurements (
    id          BIGSERIAL PRIMARY KEY,
    station_id  VARCHAR(50)   NOT NULL,
    timestamp   TIMESTAMPTZ   NOT NULL,
    pm25        FLOAT,
    pm10        FLOAT,
    co          FLOAT,
    so2         FLOAT,
    no2         FLOAT,
    o3          FLOAT,
    temperature FLOAT,
    humidity    FLOAT,
    source      VARCHAR(20)   DEFAULT 'stream',
    created_at  TIMESTAMPTZ   DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_station_time
    ON raw_measurements (station_id, timestamp DESC);

-- ── TABEL 2: daily_aqi ───────────────────────────────────────
-- Hasil batch ETL: agregasi harian AQI per stasiun
CREATE TABLE IF NOT EXISTS daily_aqi (
    id            BIGSERIAL PRIMARY KEY,
    station_id    VARCHAR(50)  NOT NULL,
    date          DATE         NOT NULL,
    pm25_avg      FLOAT,
    pm10_avg      FLOAT,
    co_avg        FLOAT,
    so2_avg       FLOAT,
    no2_avg       FLOAT,
    o3_avg        FLOAT,
    aqi_value     FLOAT,
    aqi_category  VARCHAR(20),
    record_count  INTEGER,
    created_at    TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE (station_id, date)
);

CREATE INDEX IF NOT EXISTS idx_daily_station_date
    ON daily_aqi (station_id, date DESC);

-- ── TABEL 3: stream_agg ──────────────────────────────────────
-- Hasil Spark Streaming: agregasi windowed tiap 5 menit
CREATE TABLE IF NOT EXISTS stream_agg (
    id            BIGSERIAL PRIMARY KEY,
    station_id    VARCHAR(50)  NOT NULL,
    window_start  TIMESTAMPTZ  NOT NULL,
    window_end    TIMESTAMPTZ  NOT NULL,
    pm25_avg      FLOAT,
    pm10_avg      FLOAT,
    co_avg        FLOAT,
    temperature   FLOAT,
    humidity      FLOAT,
    record_count  INTEGER,
    created_at    TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stream_station_window
    ON stream_agg (station_id, window_start DESC);

-- ── TABEL 4: predictions ─────────────────────────────────────
-- Hasil inference ML: prediksi kategori AQI per window
CREATE TABLE IF NOT EXISTS predictions (
    id              BIGSERIAL PRIMARY KEY,
    station_id      VARCHAR(50)  NOT NULL,
    window_start    TIMESTAMPTZ  NOT NULL,
    pm25_avg        FLOAT,
    pm10_avg        FLOAT,
    predicted_label VARCHAR(20),
    confidence      FLOAT,
    model_version   VARCHAR(50),
    created_at      TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pred_station_time
    ON predictions (station_id, window_start DESC);

-- ── TABEL 5: pipeline_audit ──────────────────────────────────
-- Audit log setiap Airflow DAG run (untuk governance)
CREATE TABLE IF NOT EXISTS pipeline_audit (
    id          BIGSERIAL PRIMARY KEY,
    dag_id      VARCHAR(100) NOT NULL,
    run_id      VARCHAR(200) NOT NULL,
    status      VARCHAR(20)  NOT NULL,
    records_in  INTEGER,
    records_out INTEGER,
    error_msg   TEXT,
    started_at  TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at  TIMESTAMPTZ  DEFAULT NOW()
);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO aqi_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO aqi_user;
