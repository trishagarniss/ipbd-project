from datetime import datetime, timedelta
import logging
import sys
import os
import subprocess
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "aqi-watch",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SCHEDULE = "30 6 * * *"
SPARK_DIR = "/opt/airflow/spark"


def _audit_start(**context):
    hook = PostgresHook(postgres_conn_id="aqi_postgres")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO pipeline_audit
            (dag_id, run_id, status, started_at)
        VALUES (%s, %s, 'RUNNING', NOW())
        RETURNING id
        """,
        ("batch_ingest", context["run_id"]),
    )
    audit_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    conn.close()
    context["ti"].xcom_push(key="audit_id", value=audit_id)
    log.info("Audit dimulai, id=%d", audit_id)


def _audit_finish(status, **context):
    audit_id = context["ti"].xcom_pull(key="audit_id")
    if audit_id is None:
        return
    hook = PostgresHook(postgres_conn_id="aqi_postgres")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE pipeline_audit SET status=%s, finished_at=NOW() WHERE id=%s",
        (status, audit_id),
    )
    conn.commit()
    cursor.close()
    conn.close()
    log.info("Audit %d -> %s", audit_id, status)


def _audit_success(**context):
    _audit_finish("SUCCESS", **context)


def _audit_failure(context):
    audit_id = context["ti"].xcom_pull(key="audit_id")
    if audit_id is None:
        return
    hook = PostgresHook(postgres_conn_id="aqi_postgres")
    conn = hook.get_conn()
    cursor = conn.cursor()
    error_msg = str(context.get("exception", ""))[:500]
    cursor.execute(
        "UPDATE pipeline_audit SET status='FAILED', error_msg=%s, finished_at=NOW() WHERE id=%s",
        (error_msg, audit_id),
    )
    conn.commit()
    cursor.close()
    conn.close()
    log.info("Audit %d gagal: %s", audit_id, error_msg)


def _run_batch_extract(**context):
    script = os.path.join(SPARK_DIR, "batch_extract.py")
    log.info("Running batch_extract: %s", script)
    log.debug("Python executable: %s", sys.executable)
    t0 = time.time()
    result = subprocess.run(
        [sys.executable, script],
        capture_output=True, text=True, cwd=os.path.dirname(SPARK_DIR),
    )
    elapsed = time.time() - t0
    log.debug("Exit code: %d, duration: %.2f detik", result.returncode, elapsed)
    log.info("stdout:\n%s", result.stdout)
    if result.returncode != 0:
        log.error("stderr:\n%s", result.stderr)
        raise RuntimeError(f"batch_extract gagal: {result.stderr}")
    log.info("batch_extract selesai dalam %.2f detik.", elapsed)


SUBMIT_ETL = (
    "if command -v spark-submit &>/dev/null; then "
    "  spark-submit --master spark://spark-master:7077 "
    "    --jars /opt/spark/jars/postgresql-42.7.1.jar "
    f"    {SPARK_DIR}/batch_etl.py "
    "else "
    "  echo 'WARN: spark-submit tidak tersedia — jalankan manual via spark-master'; "
    "fi"
)


with DAG(
    dag_id="batch_ingest",
    default_args=DEFAULT_ARGS,
    description="Ingest data historis harian dan ETL ke PostgreSQL",
    schedule=SCHEDULE,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["aqi-watch", "batch"],
    on_failure_callback=_audit_failure,
) as dag:

    audit_start = PythonOperator(
        task_id="audit_start",
        python_callable=_audit_start,
        provide_context=True,
    )

    extract = PythonOperator(
        task_id="batch_extract",
        python_callable=_run_batch_extract,
        provide_context=True,
    )

    etl = BashOperator(
        task_id="batch_etl",
        bash_command=SUBMIT_ETL,
    )

    audit_success = PythonOperator(
        task_id="audit_success",
        python_callable=_audit_success,
        provide_context=True,
    )

    audit_start >> extract >> etl >> audit_success
