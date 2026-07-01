import os
import logging
from datetime import datetime

import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

log = logging.getLogger(__name__)

POSTGRES_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB", "aqi_db"),
    "user":     os.getenv("POSTGRES_USER", "aqi_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "password123"),
}


POLLUTANT_CHECKS = {
    "pm25": (0, 500),
    "pm10": (0, 600),
    "co": (0, 100_000),
    "no2": (0, 2_000),
    "so2": (0, 2_000),
    "o3": (0, 1_000),
    "ispu": (0, 500),
    "temperature": (-50, 60),
    "humidity": (0, 100),
    "wind_speed": (0, 200),
    "precipitation": (0, 500),
    "cloud_cover": (0, 100),
}


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] ge_validation - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )


def validate_table(conn, table: str, date_col: str = "date"):
    log.info("Validating table: %s", table)
    cursor = conn.cursor()

    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    row_count = cursor.fetchone()[0]
    log.info("  Row count: %d", row_count)

    cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {date_col} IS NULL")
    null_dates = cursor.fetchone()[0]
    if null_dates > 0:
        log.warning("  WARNING: %d null dates ditemukan!", null_dates)

    cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns "
                   f"WHERE table_name = '{table}' AND table_schema = 'public'")
    cols = cursor.fetchall()
    log.info("  Columns (%d): %s", len(cols), [c[0] for c in cols])

    has_station = any(c[0] == "station_id" for c in cols)
    if has_station:
        cursor.execute(f"SELECT COUNT(DISTINCT station_id) FROM {table}")
        n_stations = cursor.fetchone()[0]
        log.info("  Distinct stations: %d", n_stations)

    cursor.close()
    return row_count


def validate_ranges(conn, table: str):
    log.info("Validating value ranges: %s", table)
    cursor = conn.cursor()
    passed = 0
    failed = 0

    for col, (lo, hi) in POLLUTANT_CHECKS.items():
        cursor.execute(f"SELECT COUNT(*) FROM {table} "
                       f"WHERE {col} IS NOT NULL AND ({col} < {lo} OR {col} > {hi})")
        out_of_range = cursor.fetchone()[0]
        if out_of_range > 0:
            log.warning("  WARNING: %s — %d values di luar range [%s, %s]",
                        col, out_of_range, lo, hi)
            failed += 1
        else:
            log.info("  OK: %s", col)
            passed += 1

    cursor.close()
    return passed, failed


def validate_recent_data(conn, table: str):
    cursor = conn.cursor()
    cursor.execute(f"SELECT MAX(date) FROM {table}")
    max_date = cursor.fetchone()[0]
    if max_date:
        days_old = (datetime.now().date() - max_date).days
        if days_old > 2:
            log.warning("  WARNING: data terakhir %s (%d hari yang lalu)", max_date, days_old)
        else:
            log.info("  OK: data terkini %s", max_date)
    else:
        log.warning("  WARNING: tabel kosong")
    cursor.close()


def main():
    setup_logging()
    log.info("===== Great Expectations — Data Quality Check =====")

    conn = psycopg2.connect(**POSTGRES_CONFIG)

    tables = ["raw_measurements", "stream_agg", "daily_aqi", "predictions"]

    for table in tables:
        log.info("--- %s ---", table)
        try:
            row_count = validate_table(conn, table)
            if row_count > 0:
                validate_ranges(conn, table)
                if "date" in [table, "daily_aqi", "stream_agg"]:
                    pass
            else:
                log.info("  (tabel kosong, skip range check)")
        except Exception as e:
            log.error("  ERROR validasi %s: %s", table, e)

    log.info("===== Quality Check selesai =====")
    conn.close()


if __name__ == "__main__":
    main()
