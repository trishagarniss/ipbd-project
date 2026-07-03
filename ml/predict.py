import os
import sys
import logging
from datetime import datetime, timezone

import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
import psycopg2
from psycopg2.extras import execute_values
from sklearn.preprocessing import LabelEncoder
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
if "minio:" in _endpoint:
    _endpoint = _endpoint.replace("://minio:", "://localhost:")
os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("MINIO_SECRET_KEY", "admin123")
os.environ["MLFLOW_S3_ENDPOINT_URL"] = _endpoint

log = logging.getLogger(__name__)

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "aqi_db"),
    "user": os.getenv("POSTGRES_USER", "aqi_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "password123"),
}
if "postgres" in POSTGRES_CONFIG["host"]:
    POSTGRES_CONFIG["host"] = "localhost"

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
        model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/Production")
        log.info("Model '%s' Production berhasil di-load.", MODEL_NAME)
        return model
    except Exception as e:
        log.warning("Model Production belum ada, coba latest: %s", e)
        try:
            model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/latest")
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
            cloud_cover_avg, record_count
        FROM daily_aqi
        WHERE date >= (NOW() AT TIME ZONE 'Asia/Jakarta')::date - INTERVAL '%d days'
        ORDER BY station_id, date
    """ % LOOKBACK_DAYS
    df = pd.read_sql(query, conn)
    conn.close()
    log.info("Loaded %d rows untuk prediksi", len(df))
    return df


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["station_id", "date"]).reset_index(drop=True)

    feature_df = df.copy()

    le = LabelEncoder()
    feature_df["station_encoded"] = le.fit_transform(df["station_id"])

    for col, lags in [("pm25_avg", [1, 3, 7]), ("pm10_avg", [1, 7]),
                    ("co_avg", [1, 7]),
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
    probs = model.predict_proba(X)
    confidences = probs.max(axis=1)
    log.info("Predictions: %s", preds)
    log.info("Confidences: min=%.3f max=%.3f", confidences.min(), confidences.max())
    return preds, confidences


def save_predictions(df: pd.DataFrame, predictions, confidences):
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()

    now = datetime.now(timezone.utc)
    rows = []
    for i, (_, row) in enumerate(df.iterrows()):
        rows.append((
            row["station_id"],
            row["date"].isoformat() if hasattr(row["date"], "isoformat") else str(row["date"]),
            float(row.get("pm25_avg", 0) or 0),
            float(row.get("pm10_avg", 0) or 0),
            str(predictions[i]),
            float(confidences[i]),
            MODEL_NAME,
            now,
        ))

    try:
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
        log.info("Disimpan %d prediksi ke PostgreSQL", len(rows))
    except Exception as e:
        conn.rollback()
        log.error("Gagal menyimpan prediksi: %s", e)
        raise
    finally:
        cursor.close()
        conn.close()


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
    preds, confidences = make_prediction(model, latest)
    save_predictions(latest, preds, confidences)

    log.info("Batch Predict selesai.")


if __name__ == "__main__":
    main()
