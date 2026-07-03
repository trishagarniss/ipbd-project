import os
import sys
import json
import csv
import io
import logging
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import boto3
from botocore.config import Config as BotoConfig
from dotenv import load_dotenv

sys.path.insert(0, "/opt/ml")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ispu import compute_ispu

log = logging.getLogger(__name__)

AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
WEATHER_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

MINIO_BUCKET_RAW = "raw"

LOCATIONS_PATH = "config/locations.json"
HISTORICAL_DAYS = 365


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] batch_extract - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )


def load_locations():
    with open(LOCATIONS_PATH) as f:
        data = json.load(f)
    log.info("Loaded %d stations for %s", len(data["stations"]), data["city"])
    return data["stations"]


def create_minio_client():
    ep = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    ak = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    sk = os.getenv("MINIO_SECRET_KEY", "admin123")
    return boto3.client(
        "s3",
        endpoint_url=ep,
        aws_access_key_id=ak,
        aws_secret_access_key=sk,
        use_ssl=False,
        config=BotoConfig(
            s3={"addressing_style": "path"},
            signature_version="s3v4",
            connect_timeout=10,
            retries={"max_attempts": 3},
        ),
        region_name="us-east-1",
    )


def ensure_bucket(s3, bucket):
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)
        log.info("Bucket '%s' dibuat", bucket)


def _date_range(days):
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def fetch_hourly_air_quality(lat, lon, days):
    start_date, end_date = _date_range(days)
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": (
            "pm10,pm2_5,carbon_monoxide,"
            "nitrogen_dioxide,sulphur_dioxide,ozone,"
            "uv_index"
        ),
        "start_date": start_date,
        "end_date": end_date,
        "timezone": "auto",
    }
    try:
        resp = requests.get(AIR_QUALITY_URL, params=params, timeout=120)
        if resp.status_code != 200:
            log.warning("Air quality API returned %d for (%.4f,%.4f)", resp.status_code, lat, lon)
            return None
        return resp.json()
    except Exception as e:
        log.error("Air quality API error for (%.4f,%.4f): %s", lat, lon, e)
        return None


def fetch_hourly_weather(lat, lon, days):
    start_date, end_date = _date_range(days)
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": (
            "temperature_2m,relative_humidity_2m,"
            "wind_speed_10m,precipitation,cloud_cover"
        ),
        "start_date": start_date,
        "end_date": end_date,
        "timezone": "auto",
    }
    try:
        resp = requests.get(WEATHER_ARCHIVE_URL, params=params, timeout=120)
        if resp.status_code != 200:
            log.warning("Weather archive API returned %d for (%.4f,%.4f)", resp.status_code, lat, lon)
            return None
        return resp.json()
    except Exception as e:
        log.error("Weather archive API error for (%.4f,%.4f): %s", lat, lon, e)
        return None


def build_csv_rows(station, aq_data, wx_data):
    if aq_data is None or wx_data is None:
        return []

    hourly_aq = aq_data.get("hourly", {})
    hourly_wx = wx_data.get("hourly", {})

    times = hourly_aq.get("time", [])
    if not times:
        return []

    aq_keys = {
        "pm25": "pm2_5",
        "pm10": "pm10",
        "co": "carbon_monoxide",
        "no2": "nitrogen_dioxide",
        "so2": "sulphur_dioxide",
        "o3": "ozone",
    }

    wx_keys = {
        "temperature": "temperature_2m",
        "humidity": "relative_humidity_2m",
        "wind_speed": "wind_speed_10m",
        "precipitation": "precipitation",
        "cloud_cover": "cloud_cover",
    }

    rows = []
    for i, t in enumerate(times):
        row = {
            "station_id": station["id"],
            "station_name": station["name"],
            "region": station["region"],
            "latitude": station["latitude"],
            "longitude": station["longitude"],
            "tanggal": t,
        }

        for key, api_key in aq_keys.items():
            vals = hourly_aq.get(api_key)
            row[key] = vals[i] if vals and i < len(vals) else ""

        for key, api_key in wx_keys.items():
            vals = hourly_wx.get(api_key)
            row[key] = vals[i] if vals and i < len(vals) else ""

        ispu_val, ispu_cat = compute_ispu(
            pm25=row.get("pm25"), pm10=row.get("pm10"),
            no2=row.get("no2"),  so2=row.get("so2"),
            co=row.get("co"),    o3=row.get("o3"),
        )
        row["ispu"] = ispu_val if ispu_val is not None else ""
        row["ispu_category"] = ispu_cat

        rows.append(row)

    return rows


def upload_csv_to_minio(s3, station, rows):
    if not rows:
        log.warning("Tidak ada data untuk %s", station["id"])
        return

    fieldnames = [
        "station_id", "station_name", "region", "latitude", "longitude",
        "tanggal", "pm25", "pm10", "co", "no2", "so2", "o3",
        "ispu", "ispu_category",
        "temperature", "humidity", "wind_speed", "precipitation", "cloud_cover",
    ]

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

    key = f"batch_{station['id']}.csv"

    s3.put_object(
        Bucket=MINIO_BUCKET_RAW,
        Key=key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    log.info("Uploaded %s (%d rows, %.1f KB)", key, len(rows), len(buf.getvalue()) / 1024)


def process_station(station, s3, days):
    log.info("Fetching %s (%s)...", station["id"], station["name"])
    aq = fetch_hourly_air_quality(station["latitude"], station["longitude"], days)
    wx = fetch_hourly_weather(station["latitude"], station["longitude"], days)
    rows = build_csv_rows(station, aq, wx)
    upload_csv_to_minio(s3, station, rows)
    return len(rows)


def main():
    setup_logging()

    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=365,
                        help="Jumlah hari data yang diambil (default: 365)")
    args = parser.parse_args()

    env_path = "/opt/airflow/.env"
    if os.path.exists(env_path):
        load_dotenv(env_path)
        log.info("Loaded environment from %s", env_path)
    else:
        log.warning(".env not found at %s, using defaults", env_path)

    log.info("Batch Extract mulai — mengambil %d hari terakhir", args.days)

    locations = load_locations()
    s3 = create_minio_client()
    ensure_bucket(s3, MINIO_BUCKET_RAW)

    total_rows = 0
    with ThreadPoolExecutor(max_workers=len(locations)) as executor:
        futures = {
            executor.submit(process_station, st, s3, args.days): st["id"]
            for st in locations
        }
        for future in as_completed(futures):
            sid = futures[future]
            try:
                n = future.result()
                total_rows += n
                log.info("Selesai %s: %d rows", sid, n)
            except Exception as e:
                log.error("Gagal memproses %s: %s", sid, e)

    log.info("Batch Extract selesai — total %d rows", total_rows)

    _metrics = json.dumps({
        "total_rows": total_rows,
        "stations": len(locations),
        "days": args.days,
    })
    print(f"__METRICS__:{_metrics}")


if __name__ == "__main__":
    main()
