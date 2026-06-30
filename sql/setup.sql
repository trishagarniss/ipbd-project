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
    wind_speed  FLOAT,
    source      VARCHAR(30)   DEFAULT 'openaq+open-meteo',
    created_at  TIMESTAMPTZ   DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS daily_aqi (
    id            BIGSERIAL PRIMARY KEY,
    station_id    VARCHAR(50)  NOT NULL,
    date          DATE         NOT NULL,
    pm25_avg      FLOAT,
    pm10_avg      FLOAT,
    aqi_value     FLOAT,
    aqi_category  VARCHAR(20),
    record_count  INTEGER,
    created_at    TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE (station_id, date)
);

CREATE TABLE IF NOT EXISTS stream_agg (
    id            BIGSERIAL PRIMARY KEY,
    station_id    VARCHAR(50)  NOT NULL,
    window_start  TIMESTAMPTZ  NOT NULL,
    window_end    TIMESTAMPTZ  NOT NULL,
    pm25_avg      FLOAT,
    pm10_avg      FLOAT,
    temperature   FLOAT,
    humidity      FLOAT,
    record_count  INTEGER,
    created_at    TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS predictions (
    id              BIGSERIAL PRIMARY KEY,
    station_id      VARCHAR(50)  NOT NULL,
    window_start    TIMESTAMPTZ  NOT NULL,
    predicted_label VARCHAR(20),
    confidence      FLOAT,
    created_at      TIMESTAMPTZ  DEFAULT NOW()
);
