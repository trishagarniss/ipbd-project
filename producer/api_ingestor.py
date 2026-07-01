import json
import time
import logging
import sys
from pathlib import Path

import requests
import yaml
from datetime import datetime, timezone
from kafka import KafkaProducer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ml"))
from ispu import compute_ispu

log = logging.getLogger(__name__)

AQ_PARAMS = (
    "pm10,pm2_5,carbon_monoxide,"
    "nitrogen_dioxide,sulphur_dioxide,ozone,"
    "uv_index"
)

WX_PARAMS = (
    "temperature_2m,relative_humidity_2m,"
    "wind_speed_10m,precipitation,precipitation_probability,"
    "weather_code,cloud_cover"
)


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] api_ingestor - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )


def load_locations(path="config/locations.json"):
    with open(path) as f:
        data = json.load(f)
    log.info("Loaded %d stations for %s", len(data["stations"]), data["city"])
    return data["stations"]


def create_kafka_producer(settings):
    try:
        producer = KafkaProducer(
            bootstrap_servers=settings["kafka"]["bootstrap_servers"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        log.info(
            "Kafka producer connected to %s",
            settings["kafka"]["bootstrap_servers"]
        )
        return producer
    except Exception as e:
        log.error("Failed to create Kafka producer: %s", e)
        return None


def fetch_air_quality(lat, lon, base_url):
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": AQ_PARAMS,
        "timezone": "auto"
    }
    try:
        resp = requests.get(base_url, params=params, timeout=10)
        if resp.status_code != 200:
            log.warning(
                "Air quality API returned %d for (%.4f, %.4f)",
                resp.status_code, lat, lon
            )
            return None
        data = resp.json()
        c = data.get("current", {})
        return {
            "pm25": c.get("pm2_5"),
            "pm10": c.get("pm10"),
            "co": c.get("carbon_monoxide"),
            "no2": c.get("nitrogen_dioxide"),
            "so2": c.get("sulphur_dioxide"),
            "o3": c.get("ozone"),
            "uv_index": c.get("uv_index"),
        }
    except requests.exceptions.Timeout:
        log.warning("Air quality API timeout for (%.4f, %.4f)", lat, lon)
        return None
    except Exception as e:
        log.error("Air quality API error for (%.4f, %.4f): %s", lat, lon, e)
        return None


def fetch_weather(lat, lon, base_url):
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": WX_PARAMS,
        "timezone": "auto"
    }
    try:
        resp = requests.get(base_url, params=params, timeout=10)
        if resp.status_code != 200:
            log.warning(
                "Weather API returned %d for (%.4f, %.4f)",
                resp.status_code, lat, lon
            )
            return None
        data = resp.json()
        c = data.get("current", {})
        return {
            "temperature": c.get("temperature_2m"),
            "humidity": c.get("relative_humidity_2m"),
            "wind_speed": c.get("wind_speed_10m"),
            "precipitation": c.get("precipitation"),
            "precipitation_probability": c.get("precipitation_probability"),
            "weather_code": c.get("weather_code"),
            "cloud_cover": c.get("cloud_cover"),
        }
    except requests.exceptions.Timeout:
        log.warning("Weather API timeout for (%.4f, %.4f)", lat, lon)
        return None
    except Exception as e:
        log.error("Weather API error for (%.4f, %.4f): %s", lat, lon, e)
        return None


def validate_payload(payload):
    passes_filters = True

    ranges = [
        ("pm25", 0, 500),
        ("pm10", 0, 600),
        ("co", 0, 100000),
        ("no2", 0, 2000),
        ("so2", 0, 2000),
        ("o3", 0, 1000),
        ("temperature", -50, 60),
        ("humidity", 0, 100),
        ("uv_index", 0, 20),
        ("ispu", 0, 500),
        ("cloud_cover", 0, 100),
        ("precipitation_probability", 0, 100),
    ]

    for key, lo, hi in ranges:
        val = payload.get(key)
        if val is not None and (val < lo or val > hi):
            log.warning(
                "%s out of range [%d-%d]: %.2f for %s",
                key, lo, hi, val, payload["station_id"]
            )
            passes_filters = False

    return passes_filters


def build_payload(station, aq_data, wx_data):
    if aq_data is None and wx_data is None:
        return None

    payload = {
        "station_id": station["id"],
        "station_name": station["name"],
        "region": station["region"],
        "latitude": station["latitude"],
        "longitude": station["longitude"],
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }

    if aq_data:
        for key in (
            "pm25", "pm10", "co", "no2", "so2", "o3",
            "uv_index"
        ):
            payload[key] = aq_data.get(key)

    if wx_data:
        for key in (
            "temperature", "humidity", "wind_speed",
            "precipitation", "precipitation_probability",
            "weather_code", "cloud_cover"
        ):
            payload[key] = wx_data.get(key)

    ispu_val, ispu_cat = compute_ispu(
        pm25=payload.get("pm25"), pm10=payload.get("pm10"),
        no2=payload.get("no2"),   so2=payload.get("so2"),
        co=payload.get("co"),     o3=payload.get("o3"),
    )
    payload["ispu"] = ispu_val
    payload["ispu_category"] = ispu_cat

    return payload


def format_payload_summary(payload):
    fields = [
        ("PM2.5", payload.get("pm25", "-")),
        ("ISPU", payload.get("ispu", "-")),
        ("T", payload.get("temperature", "-")),
        ("H", payload.get("humidity", "-")),
        ("Rain", payload.get("precipitation", "-")),
        ("Cloud", payload.get("cloud_cover", "-")),
    ]
    return " | ".join(f"{k}: {v}" for k, v in fields)


def main():
    setup_logging()
    log.info("=" * 50)
    log.info("AQI Watch Surakarta — API Ingestor started")
    log.info("=" * 50)

    locations = load_locations()

    with open("config/api_settings.yaml") as f:
        settings = yaml.safe_load(f)

    producer = create_kafka_producer(settings)
    if producer is None:
        log.warning("Running without Kafka — data will only be logged")

    poll_interval = settings["open_meteo"]["poll_interval_seconds"]
    topic = settings["kafka"]["topic_raw"]
    log.info("Poll interval: %ds | Kafka topic: %s", poll_interval, topic)

    cycle = 0
    while True:
        cycle += 1
        log.info("--- Cycle %d ---", cycle)

        aq_url = settings["air_quality"]["base_url"] + settings["air_quality"]["endpoints"]["current"]
        wx_url = settings["open_meteo"]["base_url"] + settings["open_meteo"]["endpoints"]["forecast"]

        for station in locations:
            try:
                aq_data = fetch_air_quality(
                    station["latitude"], station["longitude"], aq_url
                )
                wx_data = fetch_weather(
                    station["latitude"], station["longitude"], wx_url
                )

                payload = build_payload(station, aq_data, wx_data)
                if payload is None:
                    log.warning(
                        "No data for %s (%s)", station["id"], station["name"]
                    )
                    continue

                if not validate_payload(payload):
                    log.warning(
                        "Validation failed for %s — skipped", station["id"]
                    )
                    continue

                if producer:
                    producer.send(topic, value=payload)
                    log.info(
                        "[SENT] %s — %s",
                        station["id"],
                        format_payload_summary(payload)
                    )
                else:
                    log.info(
                        "[SIMULATED] %s — %s",
                        station["id"],
                        format_payload_summary(payload)
                    )

            except Exception as e:
                log.error(
                    "Unexpected error for %s: %s", station["id"], e
                )

        if producer:
            producer.flush()
            log.info("All data flushed — next cycle in %ds", poll_interval)

        time.sleep(poll_interval)


if __name__ == "__main__":
    main()
