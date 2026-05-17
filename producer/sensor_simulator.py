"""
sensor_simulator.py
Mensimulasikan 5 stasiun sensor kualitas udara Jakarta.
Push data JSON ke Kafka topic air-quality-raw setiap 2 detik.
"""

import json
import time
import random
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC           = "air-quality-raw"
INTERVAL        = 2

STATIONS = [
    {"id": "STN-BUNDARAN-HI", "name": "Bundaran HI",  "lat": -6.1954, "lon": 106.8230},
    {"id": "STN-KEMAYORAN",   "name": "Kemayoran",    "lat": -6.1565, "lon": 106.8507},
    {"id": "STN-JAGAKARSA",   "name": "Jagakarsa",    "lat": -6.3492, "lon": 106.8319},
    {"id": "STN-KEBON-JERUK", "name": "Kebon Jeruk",  "lat": -6.1920, "lon": 106.7629},
    {"id": "STN-LUBANG-BUAYA","name": "Lubang Buaya", "lat": -6.2888, "lon": 106.9098},
]

# Distribusi realistis berdasarkan data ISPU Jakarta historis
BASELINE = {
    "pm25":        {"mean": 42.0,  "std": 18.0,  "min": 0.0,   "max": 200.0},
    "pm10":        {"mean": 68.0,  "std": 25.0,  "min": 0.0,   "max": 300.0},
    "co":          {"mean": 12.5,  "std": 5.0,   "min": 0.0,   "max": 80.0},
    "so2":         {"mean": 8.0,   "std": 4.0,   "min": 0.0,   "max": 50.0},
    "no2":         {"mean": 30.0,  "std": 12.0,  "min": 0.0,   "max": 100.0},
    "o3":          {"mean": 45.0,  "std": 20.0,  "min": 0.0,   "max": 120.0},
    "temperature": {"mean": 30.5,  "std": 2.5,   "min": 24.0,  "max": 38.0},
    "humidity":    {"mean": 72.0,  "std": 10.0,  "min": 40.0,  "max": 95.0},
}

def clamp(value, min_val, max_val):
    return max(min_val, min(max_val, value))

def generate_reading(station: dict) -> dict:
    hour = datetime.now().hour
    # Rush hour modifier: pagi 07-09 dan sore 17-19 lebih polusi
    rush = 1.3 if hour in range(7, 10) or hour in range(17, 20) else 1.0

    reading = {"station_id": station["id"], "station_name": station["name"],
               "latitude": station["lat"], "longitude": station["lon"],
               "timestamp": datetime.now(timezone.utc).isoformat()}

    for param, cfg in BASELINE.items():
        mean = cfg["mean"] * rush if param in ("pm25", "pm10", "co", "no2") else cfg["mean"]
        val  = random.gauss(mean, cfg["std"])
        reading[param] = round(clamp(val, cfg["min"], cfg["max"]), 2)

    # Sesekali inject anomali (5% chance) untuk test alerting
    if random.random() < 0.05:
        reading["pm25"] = round(random.uniform(80, 150), 2)
        reading["anomaly"] = True
    else:
        reading["anomaly"] = False

    return reading

def main():
    log.info("Menghubungkan ke Kafka di %s ...", KAFKA_BOOTSTRAP)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )
    log.info("Terhubung! Mulai kirim data dari %d stasiun setiap %ds ...", len(STATIONS), INTERVAL)

    sent = 0
    try:
        while True:
            for station in STATIONS:
                payload = generate_reading(station)
                future  = producer.send(TOPIC, value=payload)
                future.add_errback(lambda e: log.error("Gagal kirim: %s", e))
                sent += 1

            producer.flush()
            if sent % 50 == 0:
                log.info("Total terkirim: %d record", sent)
            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        log.info("Dihentikan. Total terkirim: %d record.", sent)
    finally:
        producer.close()

if __name__ == "__main__":
    main()
