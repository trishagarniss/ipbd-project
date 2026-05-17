"""
batch_ingest.py
Airflow DAG — Batch ingest harian
Schedule: setiap hari pukul 00:00
Alur: validasi data → submit Spark ETL → audit log
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import psycopg2
import logging

log = logging.getLogger(__name__)

POSTGRES_CONN = {
    "host": "postgres", "dbname": "aqi_db",
    "user": "aqi_user", "password": "aqi_password_123"
}

default_args = {
    "owner":            "aqi-team",
    "depends_on_past":  False,
    "start_date":       days_ago(1),
    "email_on_failure": False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

dag = DAG(
    dag_id="aqi_batch_ingest",
    default_args=default_args,
    description="Pipeline batch harian: validasi → ETL → audit",
    schedule_interval="0 0 * * *",
    catchup=False,
    tags=["aqi", "batch", "etl"],
)


def validate_data(**context):
    """Cek apakah data CSV tersedia dan tidak kosong sebelum ETL."""
    import os
    raw_dir = "/opt/airflow/data/raw"
    csv_files = [f for f in os.listdir(raw_dir) if f.endswith(".csv")] if os.path.exists(raw_dir) else []

    if not csv_files:
        raise FileNotFoundError(f"Tidak ada file CSV di {raw_dir}. Pipeline dibatalkan.")

    log.info("Ditemukan %d file CSV: %s", len(csv_files), csv_files)
    context["ti"].xcom_push(key="csv_files", value=csv_files)


def write_audit_log(status: str, records_in: int = 0, records_out: int = 0, error: str = None, **context):
    """Tulis audit log ke PostgreSQL tabel pipeline_audit."""
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO pipeline_audit
            (dag_id, run_id, status, records_in, records_out, error_msg, started_at, finished_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
    """, (
        context["dag"].dag_id,
        context["run_id"],
        status,
        records_in,
        records_out,
        error,
        context["data_interval_start"],
    ))
    conn.commit()
    cur.close()
    conn.close()
    log.info("Audit log ditulis: status=%s", status)


with dag:

    # Task 1: Validasi ketersediaan data
    t_validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        provide_context=True,
    )

    # Task 2: Jalankan Spark Batch ETL
    t_spark_etl = BashOperator(
        task_id="run_spark_etl",
        bash_command="""
            docker exec spark-master spark-submit \
                --master spark://spark-master:7077 \
                --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
                --conf spark.hadoop.fs.s3a.access.key=minioadmin \
                --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
                --conf spark.hadoop.fs.s3a.path.style.access=true \
                /opt/spark-apps/batch_etl.py {{ ds }}
        """,
    )

    # Task 3: Tulis audit log sukses
    t_audit_success = PythonOperator(
        task_id="audit_success",
        python_callable=write_audit_log,
        op_kwargs={"status": "SUCCESS"},
        provide_context=True,
    )

    # Task 4: Tulis audit log gagal (trigger rule: one_failed)
    t_audit_fail = PythonOperator(
        task_id="audit_fail",
        python_callable=write_audit_log,
        op_kwargs={"status": "FAILED"},
        provide_context=True,
        trigger_rule="one_failed",
    )

    # Dependency
    t_validate >> t_spark_etl >> t_audit_success
    t_validate >> t_audit_fail
    t_spark_etl >> t_audit_fail
