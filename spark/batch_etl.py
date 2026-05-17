"""
batch_etl.py
Spark Batch Job — dijalankan oleh Airflow setiap hari pukul 00:00
Alur: Baca CSV dari MinIO /raw → Cleaning → Hitung AQI → Tulis ke PostgreSQL & MinIO /processed
"""

import os
import sys
import logging
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, DateType, TimestampType
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── KONFIGURASI ───────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

POSTGRES_URL  = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/aqi_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "aqi_user")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "aqi_password_123")

RAW_PATH       = "s3a://raw/"
PROCESSED_PATH = "s3a://processed/"

# ── FORMULA AQI (ISPU Kep-107/Kabapedal/11/1997) ─────────────
# Breakpoints: (batas_bawah_ispu, batas_atas_ispu, batas_bawah_konsentrasi, batas_atas_konsentrasi)
PM25_BREAKPOINTS = [
    (0,   50,   0.0,   15.5),
    (51,  100,  15.5,  35.4),
    (101, 199,  35.5,  65.4),
    (200, 299,  65.5,  150.4),
    (300, 500,  150.5, 250.4),
]

def calc_ispu(concentration: float, breakpoints: list) -> float:
    """Hitung nilai ISPU dari konsentrasi menggunakan formula linear interpolasi."""
    for (i_low, i_high, c_low, c_high) in breakpoints:
        if c_low <= concentration <= c_high:
            ispu = ((i_high - i_low) / (c_high - c_low)) * (concentration - c_low) + i_low
            return round(ispu, 2)
    return 500.0  # beyond scale

def aqi_category(aqi_value: float) -> str:
    if aqi_value <= 50:   return "Baik"
    if aqi_value <= 100:  return "Sedang"
    if aqi_value <= 199:  return "Tidak Sehat"
    if aqi_value <= 299:  return "Sangat Tidak Sehat"
    return "Berbahaya"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("AQI-Watch-BatchETL")
        .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "org.postgresql:postgresql:42.7.1")
        .getOrCreate()
    )


def read_raw_csv(spark: SparkSession, date_str: str):
    """Baca CSV dari MinIO bucket /raw untuk tanggal tertentu."""
    schema = StructType([
        StructField("station_id",  StringType(),    True),
        StructField("tanggal",     StringType(),    True),
        StructField("pm10",        FloatType(),     True),
        StructField("pm25",        FloatType(),     True),
        StructField("so2",         FloatType(),     True),
        StructField("co",          FloatType(),     True),
        StructField("o3",          FloatType(),     True),
        StructField("no2",         FloatType(),     True),
        StructField("max",         FloatType(),     True),
        StructField("kategori",    StringType(),    True),
    ])

    path = f"{RAW_PATH}ispu_jakarta_*.csv"
    log.info("Membaca CSV dari: %s", path)
    df = spark.read.csv(path, header=True, schema=schema)
    log.info("Jumlah baris terbaca: %d", df.count())
    return df


def clean_data(df):
    """
    Cleaning:
    1. Hapus baris yang semua nilai polutan null
    2. Filter nilai di luar range fisik yang masuk akal
    3. Konversi kolom tanggal
    4. Isi null dengan rata-rata per stasiun
    """
    log.info("Mulai cleaning data...")
    before = df.count()

    # Hapus baris yang semua polutan null
    pollutant_cols = ["pm25", "pm10", "co", "so2", "no2", "o3"]
    df = df.filter(
        F.coalesce(*[F.col(c) for c in pollutant_cols]).isNotNull()
    )

    # Filter nilai tidak masuk akal
    df = df.filter(
        (F.col("pm25").isNull() | ((F.col("pm25") >= 0) & (F.col("pm25") <= 500))) &
        (F.col("pm10").isNull() | ((F.col("pm10") >= 0) & (F.col("pm10") <= 600))) &
        (F.col("co").isNull()   | ((F.col("co")   >= 0) & (F.col("co")   <= 100))) &
        (F.col("so2").isNull()  | ((F.col("so2")  >= 0) & (F.col("so2")  <= 200))) &
        (F.col("no2").isNull()  | ((F.col("no2")  >= 0) & (F.col("no2")  <= 200))) &
        (F.col("o3").isNull()   | ((F.col("o3")   >= 0) & (F.col("o3")   <= 300)))
    )

    # Konversi tanggal
    df = df.withColumn(
        "date",
        F.to_date(F.col("tanggal"), "dd-MM-yyyy")
    ).filter(F.col("date").isNotNull())

    # Isi null dengan rata-rata per stasiun
    for col in pollutant_cols:
        avg_per_station = df.groupBy("station_id").agg(
            F.avg(col).alias(f"{col}_avg")
        )
        df = df.join(avg_per_station, on="station_id", how="left")
        df = df.withColumn(
            col,
            F.coalesce(F.col(col), F.col(f"{col}_avg"))
        ).drop(f"{col}_avg")

    after = df.count()
    log.info("Cleaning selesai: %d → %d baris (hapus %d)", before, after, before - after)
    return df


def calculate_aqi(df):
    """Hitung nilai AQI dan kategori menggunakan UDF."""
    calc_ispu_udf    = F.udf(lambda c: calc_ispu(c, PM25_BREAKPOINTS) if c else None, FloatType())
    aqi_category_udf = F.udf(aqi_category, StringType())

    df = df.withColumn("aqi_value",    calc_ispu_udf(F.col("pm25")))
    df = df.withColumn("aqi_category", aqi_category_udf(F.col("aqi_value")))

    log.info("Kalkulasi AQI selesai.")
    return df


def aggregate_daily(df):
    """Agregasi per stasiun per hari."""
    agg = df.groupBy("station_id", "date").agg(
        F.avg("pm25").alias("pm25_avg"),
        F.avg("pm10").alias("pm10_avg"),
        F.avg("co").alias("co_avg"),
        F.avg("so2").alias("so2_avg"),
        F.avg("no2").alias("no2_avg"),
        F.avg("o3").alias("o3_avg"),
        F.avg("aqi_value").alias("aqi_value"),
        F.first("aqi_category").alias("aqi_category"),
        F.count("*").alias("record_count"),
    ).withColumn(
        "pm25_avg",  F.round("pm25_avg", 2)
    ).withColumn(
        "pm10_avg",  F.round("pm10_avg", 2)
    ).withColumn(
        "aqi_value", F.round("aqi_value", 2)
    )

    log.info("Agregasi harian: %d baris hasil.", agg.count())
    return agg


def write_to_postgres(df, table: str):
    """Tulis DataFrame ke PostgreSQL menggunakan JDBC."""
    log.info("Menulis ke PostgreSQL tabel: %s ...", table)
    (
        df.write
        .format("jdbc")
        .option("url",      POSTGRES_URL)
        .option("dbtable",  table)
        .option("user",     POSTGRES_USER)
        .option("password", POSTGRES_PASS)
        .option("driver",   "org.postgresql.Driver")
        .mode("append")
        .save()
    )
    log.info("Berhasil tulis ke %s.", table)


def write_to_minio(df, path: str):
    """Tulis DataFrame ke MinIO dalam format Parquet."""
    log.info("Menulis ke MinIO: %s ...", path)
    (
        df.write
        .mode("append")
        .partitionBy("station_id", "date")
        .parquet(path)
    )
    log.info("Berhasil tulis ke MinIO.")


def main():
    date_str = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    log.info("===== Batch ETL mulai — tanggal: %s =====", date_str)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        df_raw       = read_raw_csv(spark, date_str)
        df_clean     = clean_data(df_raw)
        df_aqi       = calculate_aqi(df_clean)
        df_daily     = aggregate_daily(df_aqi)

        write_to_postgres(df_daily, "daily_aqi")
        write_to_minio(df_daily,    f"{PROCESSED_PATH}daily_aqi/")

        log.info("===== Batch ETL selesai =====")

    except Exception as e:
        log.error("ETL gagal: %s", str(e))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
