"""
train.py
Training Random Forest Classifier untuk prediksi kategori AQI.
Dijalankan oleh Airflow DAG setiap Senin, atau manual: python train.py
"""

import os
import logging
import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
import psycopg2

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    accuracy_score, f1_score,
    classification_report, confusion_matrix
)
from sklearn.preprocessing import LabelEncoder

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── KONFIGURASI ───────────────────────────────────────────────
POSTGRES_HOST = os.getenv("POSTGRES_HOST",     "localhost")
POSTGRES_DB   = os.getenv("POSTGRES_DB",       "aqi_db")
POSTGRES_USER = os.getenv("POSTGRES_USER",     "aqi_user")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "aqi_password_123")

MLFLOW_URI    = os.getenv("MLFLOW_URI",        "http://localhost:5000")
MODEL_NAME    = "aqi-classifier"

# Kategori AQI → label numerik
LABEL_MAP = {"Baik": 0, "Sedang": 1, "Tidak Sehat": 2, "Berbahaya": 3}
LABEL_INV = {v: k for k, v in LABEL_MAP.items()}


def get_training_data(days: int = 30) -> pd.DataFrame:
    """Ambil data 30 hari terakhir dari PostgreSQL tabel daily_aqi."""
    log.info("Mengambil data %d hari terakhir dari PostgreSQL ...", days)

    conn = psycopg2.connect(
        host=POSTGRES_HOST, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASS
    )

    query = f"""
        SELECT
            station_id,
            EXTRACT(DOW  FROM date)::int AS day_of_week,
            EXTRACT(MONTH FROM date)::int AS month,
            pm25_avg, pm10_avg, co_avg,
            so2_avg,  no2_avg,  o3_avg,
            aqi_value, aqi_category
        FROM daily_aqi
        WHERE date >= CURRENT_DATE - INTERVAL '{days} days'
          AND aqi_category IS NOT NULL
        ORDER BY date DESC;
    """

    df = pd.read_sql(query, conn)
    conn.close()

    log.info("Data terbaca: %d baris, %d kolom.", len(df), len(df.columns))
    return df


def engineer_features(df: pd.DataFrame):
    """Feature engineering dan encoding."""
    # One-hot encode station_id
    df = pd.get_dummies(df, columns=["station_id"], prefix="stn")

    # Label encode target
    df["label"] = df["aqi_category"].map(LABEL_MAP)
    df = df.dropna(subset=["label"])
    df["label"] = df["label"].astype(int)

    feature_cols = [c for c in df.columns if c not in ("aqi_category", "label", "aqi_value")]
    X = df[feature_cols].fillna(0)
    y = df["label"]

    log.info("Features: %d kolom | Samples: %d", len(feature_cols), len(X))
    log.info("Distribusi label:\n%s", y.value_counts().to_string())
    return X, y, feature_cols


def train_model(X, y):
    """Training Random Forest Classifier."""
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        min_samples_split=5,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
    )

    log.info("Training Random Forest (%d samples train, %d test) ...", len(X_train), len(X_test))
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    acc    = accuracy_score(y_test, y_pred)
    f1     = f1_score(y_test, y_pred, average="weighted")

    log.info("Accuracy : %.4f", acc)
    log.info("F1-score : %.4f", f1)
    log.info("Report:\n%s", classification_report(
        y_test, y_pred,
        target_names=[LABEL_INV[i] for i in sorted(LABEL_INV)]
    ))

    return model, acc, f1, X_test, y_test, y_pred


def register_model_to_production(run_id: str, acc: float):
    """
    Bandingkan dengan model Production saat ini.
    Promote ke Production jika akurasi lebih baik.
    """
    client = mlflow.tracking.MlflowClient()

    try:
        prod_versions = client.get_latest_versions(MODEL_NAME, stages=["Production"])
        if prod_versions:
            prod_run_id  = prod_versions[0].run_id
            prod_metrics = client.get_run(prod_run_id).data.metrics
            prod_acc     = prod_metrics.get("accuracy", 0)

            if acc > prod_acc:
                log.info("Model baru (%.4f) lebih baik dari Production (%.4f). Promote!", acc, prod_acc)
                latest = client.get_latest_versions(MODEL_NAME, stages=["Staging"])
                if latest:
                    client.transition_model_version_stage(
                        name=MODEL_NAME,
                        version=latest[0].version,
                        stage="Production"
                    )
            else:
                log.info("Model baru (%.4f) tidak lebih baik dari Production (%.4f). Skip promote.", acc, prod_acc)
        else:
            log.info("Belum ada model Production. Langsung promote model baru.")
            latest = client.get_latest_versions(MODEL_NAME, stages=["Staging"])
            if latest:
                client.transition_model_version_stage(
                    name=MODEL_NAME,
                    version=latest[0].version,
                    stage="Production"
                )
    except Exception as e:
        log.error("Gagal promote model: %s", e)


def main():
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment("aqi-jakarta-prediction")

    log.info("===== Training pipeline mulai =====")

    df            = get_training_data(days=30)

    if len(df) < 50:
        log.warning("Data terlalu sedikit (%d baris). Training dibatalkan.", len(df))
        return

    X, y, feature_cols = engineer_features(df)
    model, acc, f1, X_test, y_test, y_pred = train_model(X, y)

    with mlflow.start_run() as run:
        # Log parameter
        mlflow.log_param("n_estimators",   100)
        mlflow.log_param("max_depth",      10)
        mlflow.log_param("training_days",  30)
        mlflow.log_param("n_features",     len(feature_cols))
        mlflow.log_param("n_samples",      len(X))

        # Log metrik
        mlflow.log_metric("accuracy", acc)
        mlflow.log_metric("f1_score", f1)

        # Log confusion matrix sebagai artifact
        cm = confusion_matrix(y_test, y_pred)
        cm_str = "\n".join(["\t".join(map(str, row)) for row in cm])
        with open("/tmp/confusion_matrix.txt", "w") as f:
            f.write(cm_str)
        mlflow.log_artifact("/tmp/confusion_matrix.txt")

        # Log feature importance
        fi_df = pd.DataFrame({
            "feature":   feature_cols,
            "importance": model.feature_importances_
        }).sort_values("importance", ascending=False)
        fi_df.to_csv("/tmp/feature_importance.csv", index=False)
        mlflow.log_artifact("/tmp/feature_importance.csv")
        log.info("Top 5 features:\n%s", fi_df.head(5).to_string())

        # Register model ke MLflow
        mlflow.sklearn.log_model(
            model,
            artifact_path="model",
            registered_model_name=MODEL_NAME,
        )

        run_id = run.info.run_id
        log.info("Model terdaftar di MLflow. Run ID: %s", run_id)

    # Promote ke Production jika lebih baik
    register_model_to_production(run_id, acc)
    log.info("===== Training pipeline selesai =====")


if __name__ == "__main__":
    main()
