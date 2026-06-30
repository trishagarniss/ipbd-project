"""
api_ingestor.py
Polling API OpenAQ (kualitas udara) dan Open-Meteo (cuaca) untuk 5 stasiun Jakarta,
gabungkan datanya, lalu push ke Kafka topic air-quality-raw.

Dijalankan terus-menerus (loop), polling setiap N detik (default 60 detik)
sesuai poll_interval_seconds di config/api_settings.yaml.

BUKAN simulasi — ini data live asli dari API publik.
"""

import os
import json
import time
import logging
import yaml
import requests
from datetime import datetime, timezone
from pathlib import Path
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

BASE_DIR     = Path(__file__).resolve().parent.parent
CONFIG_DIR   = BASE_DIR / "config"
LOCATIONS_PATH = CONFIG_DIR / "locations.json"
SETTINGS_PATH  = CONFIG_DIR / "api_settings.yaml"

OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")
if not OPENAQ_API_KEY:
    log.warning("OPENAQ_API_KEY belum diset di .env! Beberapa endpoint OpenAQ butuh API key.")


def load_config():
    with open(LOCATIONS_PATH) as f:
        locations = json.load(f)
    with open(SETTINGS_PATH) as f:
        settings = yaml.safe_load(f)
    return locations, settings


def resolve_openaq_location_ids(locations: dict, settings: dict):
    """
    Cari openaq_location_id untuk setiap stasiun berdasarkan koordinat,
    kalau belum ada di locations.json. Hasilnya disimpan kembali ke file.
    """
    base_url = settings["openaq"]["base_url"]
    endpoint = settings["openaq"]["endpoints"]["locations"]
    radius   = locations.get("search_radius_meters", 5000)
    headers  = {"X-API-Key": OPENAQ_API_KEY} if OPENAQ_API_KEY else {}

    changed = False
    for station in locations["stations"]:
        if station.get("openaq_location_id"):
            continue

        coords = f"{station['latitude']},{station['longitude']}"
        try:
            resp = requests.get(
                f"{base_url}{endpoint}",
                params={"coordinates": coords, "radius": radius, "limit": 1},
                headers=headers,
                timeout=15,
            )
            resp.raise_for_status()
            results = resp.json().get("results", [])

            if results:
                loc_id = results[0]["id"]
                station["openaq_location_id"] = loc_id
                changed = True
                log.info("Stasiun %s (%s) -> OpenAQ location_id: %s",
                         station["id"], station["name"], loc_id)
            else:
                log.warning("Tidak ada stasiun OpenAQ ditemukan dekat %s (%s) dalam radius %dm.",
                            station["id"], station["name"], radius)

        except Exception as e:
            log.error("Gagal resolve location_id untuk %s: %s", station["id"], e)

    if changed:
        with open(LOCATIONS_PATH, "w") as f:
            json.dump(locations, f, indent=2)
        log.info("locations.json diperbarui dengan openaq_location_id.")

    return locations


def fetch_openaq_latest(location_id: int, settings: dict) -> dict:
    """Ambil data kualitas udara terbaru untuk satu lokasi OpenAQ."""
    base_url = settings["openaq"]["base_url"]
    endpoint = settings["openaq"]["endpoints"]["location_latest"].format(locations_id=location_id)
    headers  = {"X-API-Key": OPENAQ_API_KEY} if OPENAQ_API_KEY else {}

    try:
        resp = requests.get(f"{base_url}{endpoint}", headers=headers, timeout=15)
        resp.raise_for_status()
        results = resp.json().get("results", [])

        data = {}
        for r in results:
            param = r.get("parameter", {}).get("name", "").lower()
            if param in settings["openaq"]["parameters_of_interest"]:
                data[param] = r.get("value")
        return data

    except Exception as e:
        log.error("Gagal fetch OpenAQ untuk location_id=%s: %s", location_id, e)
        return {}


def fetch_open_meteo(lat: float, lon: float, settings: dict) -> dict:
    """Ambil data cuaca terkini dari Open-Meteo untuk satu koordinat."""
    base_url = settings["open_meteo"]["base_url"]
    endpoint = settings["open_meteo"]["endpoints"]["forecast"]
    params_list = settings["open_meteo"]["parameters_of_interest"]

    try:
        resp = requests.get(
            f"{base_url}{endpoint}",
            params={
                "latitude": lat,
                "longitude": lon,
                "current": ",".join(params_list),
                "timezone": "Asia/Jakarta",
            },
            timeout=15,
        )
        resp.raise_for_status()
        current = resp.json().get("current", {})
        return {
            "temperature": current.get("temperature_2m"),
            "humidity":    current.get("relative_humidity_2m"),
            "wind_speed":  current.get("wind_speed_10m"),
        }

    except Exception as e:
        log.error("Gagal fetch Open-Meteo untuk (%s, %s): %s", lat, lon, e)
        return {}


def build_payload(station: dict, aq_data: dict, weather_data: dict) -> dict:
    """Gabungkan data OpenAQ + Open-Meteo jadi satu payload siap kirim ke Kafka."""
    return {
        "station_id":   station["id"],
        "station_name": station["name"],
        "region":       station["region"],
        "latitude":     station["latitude"],
        "longitude":    station["longitude"],
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "pm25":         aq_data.get("pm25"),
        "pm10":         aq_data.get("pm10"),
        "co":           aq_data.get("co"),
        "no2":          aq_data.get("no2"),
        "so2":          aq_data.get("so2"),
        "o3":           aq_data.get("o3"),
        "temperature":  weather_data.get("temperature"),
        "humidity":     weather_data.get("humidity"),
        "wind_speed":   weather_data.get("wind_speed"),
        "source":       "openaq+open-meteo",
    }


def main():
    locations, settings = load_config()
    locations = resolve_openaq_location_ids(locations, settings)

    producer = KafkaProducer(
        bootstrap_servers=settings["kafka"]["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )
    topic = settings["kafka"]["topic_raw"]
    interval = settings["openaq"]["poll_interval_seconds"]

    log.info("Mulai polling %d stasiun setiap %ds, push ke topic '%s' ...",
             len(locations["stations"]), interval, topic)

    sent = 0
    try:
        while True:
            for station in locations["stations"]:
                loc_id = station.get("openaq_location_id")
                if not loc_id:
                    log.warning("Stasiun %s belum punya openaq_location_id, skip.", station["id"])
                    continue

                aq_data      = fetch_openaq_latest(loc_id, settings)
                weather_data = fetch_open_meteo(station["latitude"], station["longitude"], settings)
                payload      = build_payload(station, aq_data, weather_data)

                producer.send(topic, value=payload)
                sent += 1
                log.info("Terkirim [%s]: PM2.5=%s, suhu=%s",
                         station["id"], payload["pm25"], payload["temperature"])

            producer.flush()
            log.info("Siklus selesai. Total terkirim: %d. Tunggu %ds ...", sent, interval)
            time.sleep(interval)

    except KeyboardInterrupt:
        log.info("Dihentikan oleh user. Total terkirim: %d.", sent)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
