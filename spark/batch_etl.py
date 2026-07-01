import os
import sys
import tempfile
import logging
from datetime import datetime

import boto3
from botocore.config import Config
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, IntegerType
)

log = logging.getLogger(__name__)

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")

POSTGRES_URL  = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/aqi_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "aqi_user")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "password123")

RAW_BUCKET       = "raw"
PROCESSED_BUCKET = "processed"


def ispu_category(ispu_val):
    if ispu_val is None:
        return None
    if ispu_val <= 50:
        return "Baik"
    if ispu_val <= 100:
        return "Sedang"
    if ispu_val <= 200:
        return "Tidak Sehat"
    if ispu_val <= 300:
        return "Sangat Tidak Sehat"
    return "Berbahaya"


def _get_minio_bucket(bucket_name: str):
    use_ssl = MINIO_ENDPOINT.startswith("https")
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        use_ssl=use_ssl,
        verify=False,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 3, "mode": "standard"},
        ),
        region_name="us-east-1",
    )
    try:
        client.head_bucket(Bucket=bucket_name)
    except Exception:
        client.create_bucket(Bucket=bucket_name)
        log.info("Bucket '%s' dibuat", bucket_name)
    return client


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("AQI-Watch-BatchETL")
        .config("spark.jars.packages",
                "org.postgresql:postgresql:42.7.1")
        .getOrCreate()
    )


def _download_raw_csvs(spark: SparkSession, tmp_dir: str) -> list[str]:
    s3 = _get_minio_bucket(RAW_BUCKET)
    resp = s3.list_objects_v2(Bucket=RAW_BUCKET)
    if "Contents" not in resp:
        log.warning("Bucket %s kosong — tidak ada file untuk diproses", RAW_BUCKET)
        return []
    keys = [obj["Key"] for obj in resp["Contents"] if obj["Key"].endswith(".csv")]
    if not keys:
        log.warning("Tidak ada file .csv di bucket %s", RAW_BUCKET)
        return []
    local_files = []
    for key in keys:
        local_path = os.path.join(tmp_dir, os.path.basename(key))
        log.debug("Download %s -> %s", key, local_path)
        s3.download_file(RAW_BUCKET, key, local_path)
        local_files.append(local_path)
    log.info("Download %d file CSV ke %s", len(local_files), tmp_dir)
    return local_files


def read_raw_csv(spark: SparkSession, input_files: list[str]):
    schema = StructType([
        StructField("station_id",  StringType(),  True),
        StructField("station_name", StringType(), True),
        StructField("region",      StringType(),  True),
        StructField("latitude",    FloatType(),   True),
        StructField("longitude",   FloatType(),   True),
        StructField("tanggal",     StringType(),  True),
        StructField("pm25",        FloatType(),   True),
        StructField("pm10",        FloatType(),   True),
        StructField("co",          FloatType(),   True),
        StructField("no2",         FloatType(),   True),
        StructField("so2",         FloatType(),   True),
        StructField("o3",          FloatType(),   True),
        StructField("ispu",          FloatType(),   True),
        StructField("ispu_category", StringType(),  True),
        StructField("temperature", FloatType(),   True),
        StructField("humidity",    FloatType(),   True),
        StructField("wind_speed",  FloatType(),   True),
        StructField("precipitation", FloatType(), True),
        StructField("cloud_cover",  FloatType(),  True),
    ])

    if not input_files:
        log.warning("Tidak ada file input — ETL akan menghasilkan output kosong")
        return spark.createDataFrame([], schema)

    log.info("Membaca %d file CSV dari lokal", len(input_files))
    dfs = [spark.read.format("csv").option("header", "true").schema(schema).load(f) for f in input_files]
    if not dfs:
        df = spark.createDataFrame([], schema)
    else:
        df = dfs[0]
        for d in dfs[1:]:
            df = df.unionAll(d)
    count = df.count()
    log.info("Jumlah baris terbaca: %d", count)
    log.debug("Schema:\n%s", df.schema)
    if count == 0:
        log.warning("Tidak ada data — ETL akan menghasilkan output kosong")
    return df


def clean_data(df):
    log.info("Mulai cleaning data...")
    before = df.count()

    pollutant_cols = ["pm25", "pm10", "co", "no2", "so2", "o3"]
    total_before = df.count()
    df = df.filter(
        F.coalesce(*[F.col(c) for c in pollutant_cols]).isNotNull()
    )
    after_not_null = df.count()
    log.debug("Setelah filter null pollutants: %d -> %d (hapus %d)",
              total_before, after_not_null, total_before - after_not_null)
    if after_not_null == 0:
        log.warning("Semua baris memiliki null pada polutan — tidak ada data valid")

    ranges = [
        ("pm25", 0, 500), ("pm10", 0, 600), ("co", 0, 100000),
        ("no2", 0, 2000), ("so2", 0, 2000), ("o3", 0, 1000),
        ("temperature", -50, 60), ("humidity", 0, 100),
        ("wind_speed", 0, 200), ("cloud_cover", 0, 100),
    ]
    for col, lo, hi in ranges:
        before_range = df.count()
        df = df.filter(
            (F.col(col) >= lo) & (F.col(col) <= hi)
        )
        after_range = df.count()
        removed = before_range - after_range
        if removed > 0:
            log.debug("Range filter %s [%s, %s]: hapus %d baris", col, lo, hi, removed)
            if removed > before_range * 0.5:
                log.warning("Kolom %s: >50%% data di luar range [%s, %s]", col, lo, hi)

    df = df.withColumn(
        "date",
        F.col("tanggal").substr(0, 10).cast("date")
    ).filter(F.col("date").isNotNull())

    all_numeric = [c for c in df.columns if c not in ("station_id", "station_name", "region", "tanggal", "date")]
    before_dropna = df.count()
    df = df.dropna(subset=all_numeric)
    df.cache()
    log.info("DropNA: %d -> %d (hapus %d karena null)", before_dropna, df.count(), before_dropna - df.count())

    after = df.count()
    log.info("Cleaning: %d -> %d (hapus %d)", before, after, before - after)
    return df


def calculate_aqi_category(df):
    ispu_category_udf = F.udf(ispu_category, StringType())
    df = df.withColumn("aqi_category", ispu_category_udf(F.col("ispu")))
    return df


def aggregate_daily(df):
    log.debug("Agregasi harian dimulai — input: %d baris", df.count())
    agg = df.groupBy("station_id", "date").agg(
        F.round(F.avg("pm25"),         2).alias("pm25_avg"),
        F.round(F.avg("pm10"),         2).alias("pm10_avg"),
        F.round(F.avg("co"),           2).alias("co_avg"),
        F.round(F.avg("no2"),          2).alias("no2_avg"),
        F.round(F.avg("so2"),          2).alias("so2_avg"),
        F.round(F.avg("o3"),           2).alias("o3_avg"),
        # uv_index tidak ada di data CSV, skip
        F.round(F.avg("ispu"),  2).alias("ispu"),
        F.round(F.avg("temperature"),  2).alias("temperature_avg"),
        F.round(F.avg("humidity"),     2).alias("humidity_avg"),
        F.round(F.avg("wind_speed"),   2).alias("wind_speed_avg"),
        F.round(F.sum("precipitation"),2).alias("precipitation_sum"),
        F.round(F.avg("cloud_cover"),  2).alias("cloud_cover_avg"),
        F.first("aqi_category").alias("aqi_category"),
        F.count("*").alias("record_count"),
    )
    count = agg.count()
    log.info("Agregasi harian: %d baris", count)
    if count == 0:
        log.warning("Agregasi harian menghasilkan 0 baris — kemungkinan data kosong")
    return agg


def write_to_postgres(df, table: str):
    count = df.count()
    log.info("Menulis %d baris ke PostgreSQL tabel: %s", count, table)
    log.debug("Sample columns: %s", df.columns[:5])
    if count == 0:
        log.warning("Tidak ada data ditulis ke %s — tabel kosong", table)
        return
    (
        df.write
        .format("jdbc")
        .option("url",      POSTGRES_URL)
        .option("dbtable",  table)
        .option("user",     POSTGRES_USER)
        .option("password", POSTGRES_PASS)
        .option("driver",   "org.postgresql.Driver")
        .mode("overwrite")
        .option("truncate", "true")
        .save()
    )
    log.info("Berhasil tulis %d baris ke %s", count, table)


def write_to_minio(spark: SparkSession, df, tmp_dir: str):
    count = df.count()
    log.info("Menulis %d baris ke MinIO: %s", count, tmp_dir)
    if count == 0:
        log.warning("Tidak ada data ditulis ke MinIO")
        return
    local_out = os.path.join(tmp_dir, "daily_aqi_output")
    (
        df.write
        .mode("overwrite")
        .partitionBy("station_id", "date")
        .parquet(local_out)
    )
    s3 = _get_minio_bucket(PROCESSED_BUCKET)
    for root, _dirs, files in os.walk(local_out):
        for fname in files:
            if fname.endswith(".crc"):
                continue
            local_path = os.path.join(root, fname)
            rel_path = os.path.relpath(local_path, local_out)
            s3_key = f"daily_aqi/{rel_path.replace(os.sep, '/')}"
            try:
                s3.upload_file(local_path, PROCESSED_BUCKET, s3_key)
                log.debug("Uploaded %s -> s3://%s/%s", local_path, PROCESSED_BUCKET, s3_key)
            except Exception as e:
                log.warning("Upload warning %s: %s", s3_key, e)
    log.info("Berhasil upload daily_aqi ke MinIO")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] batch_etl - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    date_str = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    log.info("Batch ETL mulai...")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    with tempfile.TemporaryDirectory(prefix="batch_etl_") as tmp_dir:
        input_files = _download_raw_csvs(spark, tmp_dir)
        df_raw      = read_raw_csv(spark, input_files)
        if df_raw.count() == 0:
            log.warning("Data kosong — ETL dihentikan")
            return

        df_clean = clean_data(df_raw)
        df_aqi   = calculate_aqi_category(df_clean)
        df_daily = aggregate_daily(df_aqi)

        write_to_postgres(df_daily, "daily_aqi")
        write_to_minio(spark, df_daily, tmp_dir)

        log.info("Batch ETL selesai.")


if __name__ == "__main__":
    main()
