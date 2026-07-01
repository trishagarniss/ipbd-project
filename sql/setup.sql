-- Tabel untuk menyimpan raw data (gabungan Air Quality & Forecast API)
CREATE TABLE IF NOT EXISTS raw_measurements (
    id          BIGSERIAL PRIMARY KEY,
    station_id  VARCHAR(50)   NOT NULL,
    timestamp   TIMESTAMPTZ   NOT NULL,
    pm25        FLOAT,
    pm10        FLOAT,
    temperature FLOAT,
    humidity    FLOAT,
    wind_speed  FLOAT,
    source      VARCHAR(30)   DEFAULT 'open-meteo',
    created_at  TIMESTAMPTZ   DEFAULT NOW()
);

-- Tabel utama untuk data Batch (Historis & Agregasi Harian)
CREATE TABLE IF NOT EXISTS daily_aqi (
    id              BIGSERIAL PRIMARY KEY,
    station_id      VARCHAR(50)  NOT NULL,
    date            DATE         NOT NULL,
    pm25_avg        FLOAT,
    pm10_avg        FLOAT,
    temp_avg        FLOAT,       
    humidity_avg    FLOAT,       
    wind_speed_avg  FLOAT,       
    aqi_value       FLOAT,
    aqi_category    VARCHAR(20), -- Ground truth dari perhitungan ISPU otomatis
    record_count    INTEGER,
    created_at      TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE (station_id, date)
);

-- Tabel utama untuk data Streaming (Real-time dari Kafka)
CREATE TABLE IF NOT EXISTS stream_agg (
    id                BIGSERIAL PRIMARY KEY,
    station_id        VARCHAR(50)  NOT NULL,
    window_start      TIMESTAMPTZ  NOT NULL,
    window_end        TIMESTAMPTZ  NOT NULL,
    pm25_avg          FLOAT,
    pm10_avg          FLOAT,
    temperature_avg   FLOAT,
    humidity_avg      FLOAT,
    wind_speed_avg    FLOAT,
    aqi_value         FLOAT,
    prediction_label  VARCHAR(50), -- Hasil tembakan prediksi Random Forest secara Real-Time
    created_at        TIMESTAMPTZ  DEFAULT NOW()
);