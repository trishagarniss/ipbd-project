from datetime import datetime, timedelta
import json
import logging
import os
import sys
import subprocess
import time
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "aqi-watch",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SCHEDULE = "0 2 * * *"
ML_DIR = "/opt/ml"

TELEGRAM_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


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
        ("ml_retrain_stream", context["run_id"]),
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

    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        msg = f"❌ Stream ML Retrain GAGAL\nDAG: ml_retrain_stream\nError: {error_msg}"
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"},
                timeout=10,
            )
        except Exception as e:
            log.error("Telegram notif gagal: %s", e)

    log.info("Audit %d gagal: %s", audit_id, error_msg)


def _run_train_stream(**context):
    script = os.path.join(ML_DIR, "train.py")
    log.info("Running: %s --mode stream", script)
    t0 = time.time()
    result = subprocess.run(
        [sys.executable, script, "--mode", "stream"],
        capture_output=True, text=True, cwd=os.path.dirname(ML_DIR),
    )
    elapsed = time.time() - t0
    log.info("stdout:\n%s", result.stdout)
    if result.returncode != 0:
        log.error("stderr:\n%s", result.stderr)
        raise RuntimeError(f"train.py --mode stream gagal: {result.stderr}")

    metrics = None
    for line in result.stdout.strip().split("\n"):
        if line.startswith("__METRICS__:"):
            try:
                metrics = json.loads(line[len("__METRICS__:"):])
            except json.JSONDecodeError as e:
                log.warning("Gagal parse metrics JSON: %s", e)
    if metrics:
        context["ti"].xcom_push(key="train_metrics", value=metrics)
        log.info("Metrics stream: accuracy=%.4f, f1=%.4f",
                 metrics.get("accuracy", 0), metrics.get("f1_score", 0))

    log.info("train.py (stream) selesai dalam %.2f detik.", elapsed)


with DAG(
    dag_id="ml_retrain_stream",
    default_args=DEFAULT_ARGS,
    description="Retrain stream model ML harian dan kirim notifikasi",
    schedule=SCHEDULE,
    start_date=datetime(2025, 2, 1),
    catchup=False,
    tags=["aqi-watch", "ml", "stream"],
    on_failure_callback=_audit_failure,
) as dag:

    audit_start = PythonOperator(
        task_id="audit_start",
        python_callable=_audit_start,
        provide_context=True,
    )

    train_stream = PythonOperator(
        task_id="ml_train_stream",
        python_callable=_run_train_stream,
        provide_context=True,
    )

    audit_success = PythonOperator(
        task_id="audit_success",
        python_callable=_audit_success,
        provide_context=True,
    )

    audit_start >> train_stream >> audit_success
