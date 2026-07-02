import os
import sys
import logging
from datetime import datetime, timezone

import pandas as pd
import numpy as np
import mlflow
import mlflow.pyfunc
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

log = logging.getLogger(__name__)

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "aqi_db"),
    "user": os.getenv("POSTGRES_USER", "aqi_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "password123"),
}

MLFLOW_URI   = os.getenv("MLFLOW_URI", "http://localhost:5000")
MODEL_NAME   = "aqi-classifier"

LOOKBACK_DAYS = 7


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] predict - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )


def load_model():
    mlflow.set_tracking_uri(MLFLOW_URI)
    try:
        model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}/Production")
        log.info("Model '%s' Production berhasil di-load.", MODEL_NAME)
        return model
    except Exception as e:
        log.warning("Model Production belum ada, coba latest: %s", e)
        try:
            model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}/latest")
            log.info("Model '%s' latest berhasil di-load.", MODEL_NAME)
            return model
        except Exception as e2:
            log.error("Model gagal di-load: %s", e2)
            return None


def load_recent_data() -> pd.DataFrame:
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    query = """
        SELECT station_id, date,
            pm25_avg, pm10_avg, co_avg,
            no2_avg, so2_avg, o3_avg,
            ispu,
            temperature_avg, humidity_avg,
            wind_speed_avg, precipitation_sum,
            cloud_cover_avg
        FROM daily_aqi
        WHERE date >= CURRENT_DATE - INTERVAL '%d days'
        ORDER BY station_id, date
    """ % LOOKBACK_DAYS
    df = pd.read_sql(query, conn)
    conn.close()
    log.info("Loaded %d rows untuk prediksi", len(df))
    return df


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["station_id", "date"]).reset_index(drop=True)

    feature_df = df.copy()

    for col, lags in [("pm25_avg", [1, 3, 7]), ("pm10_avg", [1, 7]),
                    ("temperature_avg", [1, 7]),
                    ("humidity_avg", [1, 7])]:
        for lag in lags:
            feature_df[f"{col}_lag_{lag}"] = (
                df.groupby("station_id")[col].shift(lag)
            )

    for col, window in [("pm25_avg", 3), ("pm10_avg", 3),
                        ("ispu", 3), ("ispu", 7)]:
        feature_df[f"{col}_roll_{window}"] = (
            df.groupby("station_id")[col]
            .transform(lambda g: g.rolling(window, min_periods=1).mean())
        )

    feature_df.fillna(0, inplace=True)

    feature_df["month"] = pd.to_datetime(feature_df["date"]).dt.month
    feature_df["day_of_week"] = pd.to_datetime(feature_df["date"]).dt.dayofweek
    feature_df["is_weekend"] = feature_df["day_of_week"].isin([5, 6]).astype(int)
    feature_df["day_of_month"] = pd.to_datetime(feature_df["date"]).dt.day

    return feature_df


def get_latest_per_station(df: pd.DataFrame):
    latest = df.loc[df.groupby("station_id")["date"].idxmax()].copy()
    log.info("Latest records: %d stations", len(latest))
    return latest


def make_prediction(model, features: pd.DataFrame):
    exclude = {"station_id", "station_name", "region", "latitude", "longitude",
            "date", "tanggal", "aqi_category", "created_at"}
    feature_cols = [c for c in features.columns if c not in exclude]
    X = features[feature_cols].fillna(0)

    preds = model.predict(X)
    log.info("Predictions: %s", preds)
    return preds


def save_predictions(df: pd.DataFrame, predictions):
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()

    now = datetime.now(timezone.utc)
    rows = []
    for _, row in df.iterrows():
        rows.append((
            row["station_id"],
            row["date"].isoformat() if hasattr(row["date"], "isoformat") else str(row["date"]),
            float(row.get("pm25_avg", 0) or 0),
            float(row.get("pm10_avg", 0) or 0),
            str(predictions[_]),
            0.85,
            MODEL_NAME,
            now,
        ))

    execute_values(
        cursor,
        """
        INSERT INTO predictions
            (station_id, window_start, pm25_avg, pm10_avg,
            predicted_label, confidence, model_version, created_at)
        VALUES %s
        """,
        rows,
    )
    conn.commit()
    cursor.close()
    conn.close()
    log.info("Disimpan %d prediksi ke PostgreSQL", len(rows))


def main():
    setup_logging()
    log.info("Batch Predict mulai...")

    model = load_model()
    if model is None:
        log.error("Model tidak tersedia, keluar.")
        sys.exit(1)

    df = load_recent_data()
    if df.empty:
        log.warning("Data kosong, skip prediksi.")
        return

    df_feat = engineer_features(df)
    latest = get_latest_per_station(df_feat)
    preds = make_prediction(model, latest)
    save_predictions(latest, preds)

    log.info("Batch Predict selesai.")


if __name__ == "__main__":
    main()
