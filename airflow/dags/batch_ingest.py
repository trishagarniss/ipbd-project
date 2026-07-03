from datetime import datetime, timedelta, timezone
import json
import logging
import sys
import os
import subprocess
import time
import requests
import boto3
from botocore.config import Config as BotoConfig

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

SCHEDULE = "0 0 * * *"
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


def _notify_batch_summary(**context):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        log.warning("Telegram token/chat_id tidak dikonfigurasi — skip")
        return

    extr = context["ti"].xcom_pull(task_ids="batch_extract", key="extract_metrics") or {}
    etl  = context["ti"].xcom_pull(task_ids="batch_etl", key="etl_metrics") or {}

    wib = datetime.now(timezone.utc) + timedelta(hours=7)
    lines = ["\u2705 Batch Ingest Selesai"]
    lines.append(f"\U0001f5d3\ufe0f {wib.strftime('%A, %d %b %Y %H:%M')} WIB")
    lines.append("")

    if extr:
        lines.append("\U0001f4e5 Extract")
        lines.append(f"\u2022 Stasiun: {extr.get('stations', '?')}  |  Baris: {extr.get('total_rows', 0):,}")
        lines.append(f"\u2022 Hari: {extr.get('days', '?')}")
        if extr.get("elapsed"):
            lines.append(f"\u23f1\ufe0f {extr['elapsed']} detik")
        lines.append("")

    if etl:
        lines.append("\u2699\ufe0f ETL")
        lines.append(f"\u2022 Input CSV: {etl.get('csv_rows', 0):,}")
        lines.append(f"\u2022 Setelah clean: {etl.get('after_clean', 0):,}")
        lines.append(f"\u2022 Daily AQI: {etl.get('pg_rows', 0):,} -> PostgreSQL")
        if etl.get("minio_rows"):
            lines.append(f"\u2022 Parquet: {etl['minio_rows']:,} -> MinIO")
        if etl.get("elapsed"):
            lines.append(f"\u23f1\ufe0f {etl['elapsed']} detik")
    else:
        lines.append("(ETL selesai tanpa data)")

    msg = "\n".join(lines)
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg},
            timeout=10,
        )
        log.info("Telegram notif: status=%d", resp.status_code)
        if resp.status_code != 200:
            log.warning("Telegram API non-200: %s", resp.text[:200])
    except Exception as e:
        log.error("Telegram notif gagal: %s", e)


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

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if token and chat_id:
        task_id = context.get("task_instance", context.get("ti"))
        task_str = str(task_id) if task_id else "?"
        msg = f"\u274c Batch Ingest GAGAL\nTask: {context['dag'].dag_id}\nError: {error_msg}"
        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": msg},
                timeout=10,
            )
        except Exception as e:
            log.error("Telegram notif gagal: %s", e)


def _parse_metrics(stdout: str):
    for line in stdout.strip().split("\n"):
        if line.startswith("__METRICS__:"):
            try:
                return json.loads(line[len("__METRICS__:"):])
            except json.JSONDecodeError as e:
                log.warning("Gagal parse metrics: %s", e)
    return None


def _run_batch_extract(**context):
    script = os.path.join(SPARK_DIR, "batch_extract.py")
    log.info("Running batch_extract: %s", script)
    t0 = time.time()
    result = subprocess.run(
        [sys.executable, script],
        capture_output=True, text=True, cwd=os.path.dirname(SPARK_DIR),
    )
    elapsed = time.time() - t0
    log.info("stdout:\n%s", result.stdout)
    if result.returncode != 0:
        log.error("stderr:\n%s", result.stderr)
        raise RuntimeError(f"batch_extract gagal: {result.stderr}")
    metrics = _parse_metrics(result.stdout)
    if metrics:
        metrics["elapsed"] = round(elapsed, 2)
        context["ti"].xcom_push(key="extract_metrics", value=metrics)
        log.info("Extract metrics: %s", metrics)
    log.info("batch_extract selesai dalam %.2f detik.", elapsed)


def _download_csvs():
    raw_dir = "/tmp/raw_csvs"
    os.makedirs(raw_dir, exist_ok=True)

    for f in os.listdir(raw_dir):
        os.remove(os.path.join(raw_dir, f))

    minio_ep = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    use_ssl = minio_ep.startswith("https")
    s3 = boto3.client(
        "s3",
        endpoint_url=minio_ep,
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "admin123"),
        use_ssl=use_ssl,
        config=BotoConfig(signature_version="s3v4", retries={"max_attempts": 3}),
        region_name="us-east-1",
    )

    resp = s3.list_objects_v2(Bucket="raw")
    if "Contents" not in resp:
        log.warning("Bucket raw kosong")
        return raw_dir

    keys = [obj["Key"] for obj in resp["Contents"] if obj["Key"].endswith(".csv")]
    if not keys:
        log.warning("Tidak ada file .csv di bucket raw")
        return raw_dir

    for key in keys:
        local = os.path.join(raw_dir, os.path.basename(key))
        s3.download_file("raw", key, local)
        log.info("Downloaded %s -> %s", key, local)

    log.info("Download %d CSV ke %s", len(keys), raw_dir)
    return raw_dir


def _run_batch_etl(**context):
    raw_dir = _download_csvs()

    script = os.path.join(SPARK_DIR, "batch_etl.py")
    cmd = [sys.executable, script, "--raw-dir", raw_dir]
    log.info("Running batch_etl (local mode): %s", " ".join(cmd))
    t0 = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
    elapsed = time.time() - t0
    log.info("stdout:\n%s", result.stdout)
    if result.returncode != 0:
        log.error("stderr:\n%s", result.stderr)
        raise RuntimeError(f"batch_etl gagal: {result.stderr}")
    metrics = _parse_metrics(result.stdout)
    if metrics:
        metrics["elapsed"] = round(elapsed, 2)
        context["ti"].xcom_push(key="etl_metrics", value=metrics)
        log.info("ETL metrics: %s", metrics)
    log.info("batch_etl selesai dalam %.2f detik.", elapsed)


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

    etl = PythonOperator(
        task_id="batch_etl",
        python_callable=_run_batch_etl,
        provide_context=True,
    )

    notify_summary = PythonOperator(
        task_id="notify_summary",
        python_callable=_notify_batch_summary,
        provide_context=True,
    )

    audit_success = PythonOperator(
        task_id="audit_success",
        python_callable=_audit_success,
        provide_context=True,
    )

    audit_start >> extract >> etl >> notify_summary >> audit_success
