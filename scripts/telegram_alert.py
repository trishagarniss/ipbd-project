import os
import logging
from datetime import datetime, timezone

import pandas as pd
import psycopg2
import requests
from dotenv import load_dotenv
from sklearn.preprocessing import LabelEncoder

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

log = logging.getLogger(__name__)

CATEGORY_ICONS = {
    "Baik": "\U0001F7E2",
    "Sedang": "\U0001F7E1",
    "Tidak Sehat": "\U0001F7E0",
    "Sangat Tidak Sehat": "\U0001F534",
    "Berbahaya": "\U0001F7E3",
}

STATION_MAP = {
    "SKA1": "Banjarsari",
    "SKA2": "Jebres",
    "SKA3": "Laweyan",
    "SKA4": "Pasar Kliwon",
    "SKA5": "Serengan",
}

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "aqi_db"),
    "user": os.getenv("POSTGRES_USER", "aqi_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "password123"),
}
if "postgres" in POSTGRES_CONFIG["host"]:
    POSTGRES_CONFIG["host"] = "localhost"

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def get_category_mapping():
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    df = pd.read_sql("SELECT DISTINCT aqi_category FROM daily_aqi ORDER BY aqi_category", conn)
    conn.close()

    if df.empty:
        return {}

    le = LabelEncoder()
    le.fit(df["aqi_category"])
    return {i: name for i, name in enumerate(le.classes_)}


def get_today_aqi():
    query = """
        SELECT station_id, date, pm25_avg, pm10_avg, ispu, aqi_category
        FROM daily_aqi
        WHERE date = CURRENT_DATE
        ORDER BY station_id
    """
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def get_latest_predictions():
    query = """
        SELECT DISTINCT ON (station_id)
            station_id, window_start, predicted_label, confidence, model_version, created_at
        FROM predictions
        ORDER BY station_id, created_at DESC
    """
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def build_message(aqi_df, pred_df, cat_map):
    from datetime import timedelta
    tz = timezone(timedelta(hours=7))
    now = datetime.now(tz)
    date_str = now.strftime("%d %B %Y")
    time_str = now.strftime("%H:%M") + " WIB"

    lines = [
        f"<b>Pantauan ISPU Surakarta</b>",
        f"<i>{date_str} | {time_str}</i>",
        "",
    ]

    if aqi_df.empty:
        lines.append("<i>Tidak ada data ISPU untuk hari ini.</i>")
        return "\n".join(lines)

    for i, (_, row) in enumerate(aqi_df.iterrows()):
        region = STATION_MAP.get(row["station_id"], row["station_id"])
        cat = row["aqi_category"]
        ispu_val = f"{row['ispu']:.0f}"
        icon = CATEGORY_ICONS.get(cat, "")

        pred_name = None
        pred_conf = None
        pred_icon = ""
        if not pred_df.empty and row["station_id"] in pred_df["station_id"].values:
            p = pred_df[pred_df["station_id"] == row["station_id"]].iloc[0]
            try:
                label_num = int(float(p["predicted_label"]))
                pred_name = cat_map.get(label_num, p["predicted_label"])
            except (ValueError, TypeError):
                pred_name = p["predicted_label"]
            pred_conf = f"{p['confidence']:.0%}"
            pred_icon = CATEGORY_ICONS.get(pred_name, "")

        lines.append(f"<b>{region}</b>")
        lines.append(f"  Sekarang: {icon} {cat} (ISPU {ispu_val})")
        if pred_name:
            lines.append(f"  Besok: {pred_icon} {pred_name} (keyakinan {pred_conf})")
        if i < len(aqi_df) - 1:
            lines.append("")

    return "\n".join(lines)


def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram token/chat_id tidak dikonfigurasi — skip")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        resp = requests.post(url, json=payload, timeout=15)
        if resp.status_code == 200:
            log.info("Telegram notif: status=%d", resp.status_code)
            return True
        else:
            log.warning("Telegram API return non-200: %s", resp.text[:200])
            return False
    except Exception as e:
        log.error("Telegram notif gagal: %s", e)
        return False


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] telegram - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    log.info("Telegram Alert mulai...")

    cat_map = get_category_mapping()
    log.info("Category mapping: %s", cat_map)

    aqi_df = get_today_aqi()
    pred_df = get_latest_predictions()

    log.info("AQI data: %d stasiun, Predictions: %d stasiun", len(aqi_df), len(pred_df))

    message = build_message(aqi_df, pred_df, cat_map)
    log.debug("Message:\n%s", message)

    ok = send_telegram(message)
    if ok:
        log.info("Telegram Alert selesai — sukses")
    else:
        log.info("Telegram Alert selesai — gagal")


if __name__ == "__main__":
    main()
