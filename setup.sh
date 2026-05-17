#!/bin/bash
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE raw_measurements (id SERIAL PRIMARY KEY, location VARCHAR(255), parameter VARCHAR(50), value FLOAT, measured_at TIMESTAMP);
    CREATE TABLE daily_aqi (date DATE PRIMARY KEY, avg_pm25 FLOAT, aqi_category VARCHAR(50));
    CREATE TABLE stream_agg (window_start TIMESTAMP, window_end TIMESTAMP, avg_value FLOAT);
    CREATE TABLE predictions (predict_time TIMESTAMP, predicted_pm25 FLOAT, model_version VARCHAR(50));
EOSQL