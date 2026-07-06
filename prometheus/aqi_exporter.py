import os
import time
import logging
import psycopg2
from prometheus_client import start_http_server, Gauge

log = logging.getLogger(__name__)

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = int(os.getenv("POSTGRES_PORT", 5432))
DB_NAME = os.getenv("POSTGRES_DB", "aqi_db")
DB_USER = os.getenv("POSTGRES_USER", "aqi_user")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "password123")

INTERVAL = int(os.getenv("COLLECTOR_INTERVAL", "15"))

QUERY = """
    SELECT station_id, ispu_avg, pm25_avg, pm10_avg,
           temperature_avg, humidity_avg, record_count
    FROM stream_agg
    WHERE (station_id, window_start) IN (
        SELECT station_id, MAX(window_start)
        FROM stream_agg
        GROUP BY station_id
    )
    ORDER BY station_id;
"""

ispu_g = Gauge("ispu_avg", "ISPU average by station", ["station_id"])
pm25_g = Gauge("pm25_avg", "PM2.5 average by station", ["station_id"])
pm10_g = Gauge("pm10_avg", "PM10 average by station", ["station_id"])
temp_g = Gauge("temperature_avg", "Temperature average by station", ["station_id"])
humid_g = Gauge("humidity_avg", "Humidity average by station", ["station_id"])
records_g = Gauge("record_count", "Record count by station", ["station_id"])


def collect():
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASS
        )
        cur = conn.cursor()
        cur.execute(QUERY)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        for row in rows:
            sid, ispu, pm25, pm10, temp, hum, cnt = row
            ispu_g.labels(station_id=sid).set(ispu or 0)
            pm25_g.labels(station_id=sid).set(pm25 or 0)
            pm10_g.labels(station_id=sid).set(pm10 or 0)
            temp_g.labels(station_id=sid).set(temp or 0)
            humid_g.labels(station_id=sid).set(hum or 0)
            records_g.labels(station_id=sid).set(cnt or 0)

        log.info("Collected metrics for %d stations", len(rows))
    except Exception as e:
        log.error("Collect failed: %s", e)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] aqi_exporter - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )
    start_http_server(8000)
    log.info("AQI Exporter started on port %d", 8000)
    while True:
        collect()
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
