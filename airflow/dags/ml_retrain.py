from datetime import datetime, timedelta
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

SCHEDULE = "0 8 * * 1"
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
        ("ml_retrain", context["run_id"]),
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
        msg = f"❌ ML Retrain GAGAL\nDAG: ml_retrain\nError: {error_msg}"
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"},
                timeout=10,
            )
        except Exception as e:
            log.error("Telegram notif gagal: %s", e)

    log.info("Audit %d gagal: %s", audit_id, error_msg)


def _run_python_script(script_name, **context):
    script = os.path.join(ML_DIR, script_name)
    log.info("Running: %s", script)
    log.debug("Python executable: %s, script path: %s", sys.executable, script)
    t0 = time.time()
    result = subprocess.run(
        [sys.executable, script],
        capture_output=True, text=True, cwd=os.path.dirname(ML_DIR),
    )
    elapsed = time.time() - t0
    log.debug("Exit code: %d, duration: %.2f detik", result.returncode, elapsed)
    log.info("stdout:\n%s", result.stdout)
    if result.returncode != 0:
        log.error("stderr:\n%s", result.stderr)
        raise RuntimeError(f"{script_name} gagal: {result.stderr}")

    if "0 baris" in result.stdout.lower() or "skipping" in result.stdout.lower():
        log.warning("%s: output mencurigakan (0 baris / skip)", script_name)

    log.info("%s selesai dalam %.2f detik.", script_name, elapsed)


def _notify_telegram(**context):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram token/chat_id tidak dikonfigurasi — skip notifikasi")
        return

    msg = (
        "✅ *ML Retrain Selesai*\n"
        f"DAG: ml_retrain\n"
        f"Run: {context['run_id']}\n"
        f"Waktu: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )
    log.debug("Mengirim Telegram ke chat_id=%s, panjang pesan=%d", TELEGRAM_CHAT_ID[:4], len(msg))
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"},
            timeout=10,
        )
        log.info("Telegram notif: status=%d", resp.status_code)
        if resp.status_code != 200:
            log.warning("Telegram API return non-200: %s", resp.text[:200])
    except Exception as e:
        log.error("Telegram notif gagal: %s", e)


with DAG(
    dag_id="ml_retrain",
    default_args=DEFAULT_ARGS,
    description="Retrain model ML mingguan dan kirim notifikasi",
    schedule=SCHEDULE,
    start_date=datetime(2025, 2, 1),
    catchup=False,
    tags=["aqi-watch", "ml"],
    on_failure_callback=_audit_failure,
) as dag:

    audit_start = PythonOperator(
        task_id="audit_start",
        python_callable=_audit_start,
        provide_context=True,
    )

    train = PythonOperator(
        task_id="ml_train",
        python_callable=_run_python_script,
        op_kwargs={"script_name": "train.py"},
        provide_context=True,
    )

    predict = PythonOperator(
        task_id="ml_predict",
        python_callable=_run_python_script,
        op_kwargs={"script_name": "predict.py"},
        provide_context=True,
    )

    bot_telegram = PythonOperator(
        task_id="notify_telegram",
        python_callable=_notify_telegram,
        provide_context=True,
    )

    audit_success = PythonOperator(
        task_id="audit_success",
        python_callable=_audit_success,
        provide_context=True,
    )

    audit_start >> train >> predict >> bot_telegram >> audit_success
