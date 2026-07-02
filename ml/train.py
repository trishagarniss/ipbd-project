import os
import sys
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.cluster import KMeans
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score,
    f1_score, classification_report, confusion_matrix
)
from sklearn.preprocessing import LabelEncoder, StandardScaler
import mlflow
import mlflow.sklearn
import boto3
from botocore.config import Config
import psycopg2
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
if "postgres" in POSTGRES_CONFIG["host"]:
    POSTGRES_CONFIG["host"] = "localhost"

MLFLOW_URI = os.getenv("MLFLOW_URI", "http://localhost:5000")
MLFLOW_EXPERIMENT = "aqi-classifier"
MODEL_NAME = "aqi-classifier"

TRAIN_DAYS = 350
TEST_DAYS  = 7


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] train - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )


def load_data() -> pd.DataFrame:
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    query = """
        SELECT station_id, date, pm25_avg, pm10_avg, co_avg,
                no2_avg, so2_avg, o3_avg,
                ispu, temperature_avg,
                humidity_avg, wind_speed_avg, precipitation_sum,
                cloud_cover_avg, aqi_category, record_count
        FROM daily_aqi
        WHERE date >= CURRENT_DATE - INTERVAL %s
        ORDER BY station_id, date
    """
    days = TRAIN_DAYS + TEST_DAYS
    df = pd.read_sql(query, conn, params=(f"{days} days",))
    conn.close()
    log.info("Loaded %d rows from daily_aqi", len(df))
    return df


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Engineering features...")

    stations = df["station_id"].unique()
    station_encoder = LabelEncoder()
    df["station_encoded"] = station_encoder.fit_transform(df["station_id"])

    numeric_cols = [
        "pm25_avg", "pm10_avg", "co_avg", "no2_avg", "so2_avg", "o3_avg",
        "ispu",
        "temperature_avg", "humidity_avg", "wind_speed_avg",
        "precipitation_sum", "cloud_cover_avg",
    ]

    df = df.sort_values(["station_id", "date"]).reset_index(drop=True)

    for col in numeric_cols:
        df[col] = df.groupby("station_id")[col].transform(
            lambda g: g.fillna(g.rolling(7, min_periods=1).mean()).fillna(g.median())
        )

    for col, lags in [("pm25_avg", [1, 3, 7]), ("pm10_avg", [1, 7]),
                        ("co_avg", [1, 7]), ("temperature_avg", [1, 7]),
                        ("humidity_avg", [1, 7])]:
        for lag in lags:
            df[f"{col}_lag_{lag}"] = df.groupby("station_id")[col].shift(lag)

    for col, window in [("pm25_avg", 3), ("pm10_avg", 3),
                        ("ispu", 3), ("ispu", 7)]:
        df[f"{col}_roll_{window}"] = (
            df.groupby("station_id")[col]
            .transform(lambda g: g.rolling(window, min_periods=1).mean())
        )

    df["month"] = pd.to_datetime(df["date"]).dt.month
    df["day_of_week"] = pd.to_datetime(df["date"]).dt.dayofweek
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)
    df["day_of_month"] = pd.to_datetime(df["date"]).dt.day

    log.info("Feature shape: %s", df.shape)
    return df


def prepare_train_test(df: pd.DataFrame):
    df = df.dropna(subset=["aqi_category"]).copy()
    df = df.sort_values("date").reset_index(drop=True)

    max_date = df["date"].max()
    cutoff = max_date - timedelta(days=TEST_DAYS)

    train_df = df[df["date"] <= cutoff].copy()
    test_df  = df[df["date"] > cutoff].copy()

    exclude_cols = {
        "station_id", "date", "aqi_category", "station_name", "region",
        "latitude", "longitude", "created_at",
    }
    feature_cols = [c for c in df.columns if c not in exclude_cols]

    train_df = train_df.dropna(subset=feature_cols)
    test_df  = test_df.dropna(subset=feature_cols)

    X_train = train_df[feature_cols]
    y_train = train_df["aqi_category"]
    X_test  = test_df[feature_cols]
    y_test  = test_df["aqi_category"]

    log.info("Train: %d, Test: %d", len(X_train), len(X_test))
    log.info("Feature columns: %s", feature_cols)

    return X_train, X_test, y_train, y_test, feature_cols


def train_rf(X_train, X_test, y_train, y_test, feature_cols):
    log.info("Training Random Forest classifier...")

    le = LabelEncoder()
    y_train_enc = le.fit_transform(y_train)
    y_test_enc  = le.transform(y_test)

    param_grid = {
        "n_estimators": [100, 200],
        "max_depth": [10, 20, None],
        "min_samples_split": [2, 5],
    }

    rf = RandomForestClassifier(random_state=42, class_weight="balanced", n_jobs=-1)
    grid = GridSearchCV(
        rf, param_grid, cv=3, scoring="f1_weighted", n_jobs=-1, verbose=0
    )
    grid.fit(X_train, y_train_enc)

    best_rf = grid.best_estimator_
    y_pred_enc = best_rf.predict(X_test)

    accuracy  = accuracy_score(y_test_enc, y_pred_enc)
    precision = precision_score(y_test_enc, y_pred_enc, average="weighted", zero_division=0)
    recall    = recall_score(y_test_enc, y_pred_enc, average="weighted", zero_division=0)
    f1        = f1_score(y_test_enc, y_pred_enc, average="weighted", zero_division=0)
    cm        = confusion_matrix(y_test_enc, y_pred_enc)

    log.info("RF best params: %s", grid.best_params_)
    log.info("RF accuracy: %.4f, f1: %.4f", accuracy, f1)
    labels = np.unique(np.concatenate([y_test_enc, y_pred_enc]))
    log.info("Classification report:\n%s",
                classification_report(y_test_enc, y_pred_enc, labels=labels,
                                      target_names=le.inverse_transform(labels), zero_division=0))

    return best_rf, le, {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1_score": f1,
        "confusion_matrix": cm.tolist(),
        "best_params": str(grid.best_params_),
    }


def train_kmeans(df: pd.DataFrame):
    log.info("Training KMeans clustering...")
    cluster_features = [
        "pm25_avg", "pm10_avg", "co_avg", "no2_avg", "so2_avg", "o3_avg",
        "ispu", "temperature_avg", "humidity_avg",
    ]
    station_avg = df.groupby("station_id")[cluster_features].mean().dropna()

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(station_avg)

    kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
    clusters = kmeans.fit_predict(X_scaled)

    station_avg["cluster"] = clusters
    log.info("KMeans clusters:\n%s", station_avg[["cluster"]].to_string())

    return kmeans, scaler


def _ensure_mlflow_bucket():
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    if "minio" in endpoint:
        endpoint = endpoint.replace("://minio:", "://localhost:")
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "admin123"),
        use_ssl=endpoint.startswith("https"),
        verify=False,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 3, "mode": "standard"},
        ),
        region_name="us-east-1",
    )
    try:
        client.head_bucket(Bucket="mlflow")
    except Exception:
        client.create_bucket(Bucket="mlflow")
        log.info("Bucket MinIO 'mlflow' dibuat untuk artifact MLflow")


def main():
    setup_logging()
    log.info("ML Training mulai...")
    _ensure_mlflow_bucket()

    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    with mlflow.start_run(run_name=f"train_{datetime.now():%Y%m%d_%H%M%S}") as run:
        try:
            df = load_data()
            if df.empty:
                log.warning("Data kosong, skip training.")
                return

            df_feat = engineer_features(df)
            X_train, X_test, y_train, y_test, feature_cols = prepare_train_test(df_feat)

            if X_train.shape[0] < 10:
                log.warning("Data train terlalu sedikit (%d), skip.", X_train.shape[0])
                return

            model, label_encoder, metrics = train_rf(X_train, X_test, y_train, y_test, feature_cols)
            kmeans_model, kmeans_scaler = train_kmeans(df)

            mlflow.log_params({
                "model_type": "RandomForestClassifier",
                "train_samples": X_train.shape[0],
                "test_samples": X_test.shape[0],
                "features": len(feature_cols),
                "n_clusters_kmeans": 3,
            })

            for metric_name, metric_val in metrics.items():
                if metric_name == "best_params":
                    mlflow.log_param("best_params", metric_val)
                elif metric_name == "confusion_matrix":
                    continue
                else:
                    mlflow.log_metric(metric_name, metric_val)

            mlflow.sklearn.log_model(
                sk_model=model,
                artifact_path="model",
            )

            model_uri = f"runs:/{run.info.run_id}/model"
            mlflow.register_model(model_uri=model_uri, name=MODEL_NAME)

            log.info("Run ID: %s", run.info.run_id)
            log.info("Model '%s' logged & registered ke MLflow.", MODEL_NAME)
            log.info("ML Training selesai.")

        except Exception as e:
            log.error("Training gagal: %s", e)
            raise


if __name__ == "__main__":
    main()
