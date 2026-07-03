CREATE TABLE IF NOT EXISTS raw_measurements (
    id              BIGSERIAL PRIMARY KEY,
    station_id      VARCHAR(10)   NOT NULL,
    station_name    VARCHAR(50),
    region          VARCHAR(50),
    latitude        FLOAT,
    longitude       FLOAT,
    timestamp       TIMESTAMPTZ   NOT NULL,
    pm25            FLOAT,
    pm10            FLOAT,
    co              FLOAT,
    no2             FLOAT,
    so2             FLOAT,
    o3              FLOAT,
    uv_index        FLOAT,
    ispu            FLOAT,
    ispu_category   VARCHAR(20),
    temperature     FLOAT,
    humidity        FLOAT,
    wind_speed      FLOAT,
    precipitation   FLOAT,
    precip_prob     FLOAT,
    weather_code    FLOAT,
    cloud_cover     FLOAT,
    created_at      TIMESTAMPTZ   DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stream_agg (
    id                BIGSERIAL PRIMARY KEY,
    station_id        VARCHAR(10)   NOT NULL,
    window_start      TIMESTAMPTZ   NOT NULL,
    window_end        TIMESTAMPTZ   NOT NULL,
    pm25_avg          FLOAT,
    pm10_avg          FLOAT,
    co_avg            FLOAT,
    no2_avg           FLOAT,
    so2_avg           FLOAT,
    o3_avg            FLOAT,
    uv_index_avg      FLOAT,
    ispu_avg          FLOAT,
    temperature_avg   FLOAT,
    humidity_avg      FLOAT,
    wind_speed_avg    FLOAT,
    precipitation_sum FLOAT,
    cloud_cover_avg   FLOAT,
    record_count      INTEGER,
    created_at        TIMESTAMPTZ   DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS daily_aqi (
    id                BIGSERIAL PRIMARY KEY,
    station_id        VARCHAR(10)   NOT NULL,
    date              DATE          NOT NULL,
    pm25_avg          FLOAT,
    pm10_avg          FLOAT,
    co_avg            FLOAT,
    no2_avg           FLOAT,
    so2_avg           FLOAT,
    o3_avg            FLOAT,
    ispu            FLOAT,
    temperature_avg   FLOAT,
    humidity_avg      FLOAT,
    wind_speed_avg    FLOAT,
    precipitation_sum FLOAT,
    cloud_cover_avg   FLOAT,
    aqi_category      VARCHAR(20),
    record_count      INTEGER,
    created_at        TIMESTAMPTZ   DEFAULT NOW(),
    UNIQUE (station_id, date)
);

CREATE TABLE IF NOT EXISTS predictions (
    id              BIGSERIAL PRIMARY KEY,
    station_id      VARCHAR(10)   NOT NULL,
    window_start    TIMESTAMPTZ   NOT NULL,
    pm25_avg        FLOAT,
    pm10_avg        FLOAT,
    predicted_label VARCHAR(20),
    confidence      FLOAT,
    model_version   VARCHAR(50),
    created_at      TIMESTAMPTZ   DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stream_predictions (
    id              BIGSERIAL PRIMARY KEY,
    station_id      VARCHAR(10)   NOT NULL,
    window_start    TIMESTAMPTZ   NOT NULL,
    window_end      TIMESTAMPTZ   NOT NULL,
    pm25_avg        FLOAT,
    pm10_avg        FLOAT,
    ispu_avg        FLOAT,
    predicted_label VARCHAR(20),
    confidence      FLOAT,
    model_version   VARCHAR(50),
    created_at      TIMESTAMPTZ   DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_audit (
    id          BIGSERIAL PRIMARY KEY,
    dag_id      VARCHAR(100),
    run_id      VARCHAR(100),
    status      VARCHAR(20),
    records_in  INTEGER DEFAULT 0,
    records_out INTEGER DEFAULT 0,
    error_msg   TEXT,
    started_at  TIMESTAMPTZ,
    finished_at TIMESTAMPTZ DEFAULT NOW()
);
