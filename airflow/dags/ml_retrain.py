"""
ml_retrain.py
Airflow DAG — Retraining model ML mingguan
Schedule: setiap Senin pukul 02:00 (setelah batch ETL selesai)
"""

from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import logging

log = logging.getLogger(__name__)

default_args = {
    "owner":            "aqi-team",
    "depends_on_past":  False,
    "start_date":       days_ago(1),
    "email_on_failure": False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=10),
}

dag = DAG(
    dag_id="aqi_ml_retrain",
    default_args=default_args,
    description="Retraining model ML mingguan",
    schedule_interval="0 2 * * MON",
    catchup=False,
    tags=["aqi", "ml", "training"],
)


def check_data_sufficiency(**context):
    """Pastikan ada cukup data untuk training (minimal 100 baris)."""
    import psycopg2
    conn = psycopg2.connect(
        host="postgres", dbname="aqi_db",
        user="aqi_user", password="aqi_password_123"
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM daily_aqi
        WHERE date >= CURRENT_DATE - INTERVAL '30 days'
          AND aqi_category IS NOT NULL
    """)
    count = cur.fetchone()[0]
    conn.close()

    log.info("Data tersedia untuk training: %d baris", count)
    if count < 100:
        raise ValueError(f"Data tidak cukup ({count} baris). Butuh minimal 100 baris.")


with dag:

    # Task 1: Cek kecukupan data
    t_check = PythonOperator(
        task_id="check_data_sufficiency",
        python_callable=check_data_sufficiency,
        provide_context=True,
    )

    # Task 2: Jalankan training script
    t_train = BashOperator(
        task_id="run_training",
        bash_command="""
            cd /opt/airflow && python /opt/airflow/ml/train.py
        """,
    )

    # Task 3: Verifikasi model terdaftar di MLflow
    t_verify = BashOperator(
        task_id="verify_model_registered",
        bash_command="""
            python3 -c "
import mlflow
mlflow.set_tracking_uri('http://mlflow:5000')
client = mlflow.tracking.MlflowClient()
versions = client.get_latest_versions('aqi-classifier', stages=['Production', 'Staging'])
if not versions:
    raise Exception('Model tidak ditemukan di MLflow registry!')
print(f'Model terdaftar: {versions[0].name} v{versions[0].version} [{versions[0].current_stage}]')
"
        """,
    )

    t_check >> t_train >> t_verify
