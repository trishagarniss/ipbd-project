"""
stream_processor.py
Spark Structured Streaming — baca Kafka → windowed aggregation → inference ML → PostgreSQL
Window: 10 menit, slide: 5 menit
"""

import os
import json
import logging
import mlflow.pyfunc
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    BooleanType, TimestampType, DoubleType
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── KONFIGURASI ───────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "kafka:29092")
KAFKA_TOPIC      = os.getenv("KAFKA_TOPIC",      "air-quality-raw")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

POSTGRES_URL     = os.getenv("POSTGRES_URL",      "jdbc:postgresql://postgres:5432/aqi_db")
POSTGRES_USER    = os.getenv("POSTGRES_USER",     "aqi_user")
POSTGRES_PASS    = os.getenv("POSTGRES_PASSWORD", "aqi_password_123")

MLFLOW_URI       = os.getenv("MLFLOW_URI",        "http://mlflow:5000")
MODEL_NAME       = "aqi-classifier"
CHECKPOINT_PATH  = "/tmp/spark-checkpoints/stream_processor"

# ── SCHEMA PAYLOAD JSON dari Kafka ────────────────────────────
PAYLOAD_SCHEMA = StructType([
    StructField("station_id",   StringType(),  True),
    StructField("station_name", StringType(),  True),
    StructField("latitude",     FloatType(),   True),
    StructField("longitude",    FloatType(),   True),
    StructField("timestamp",    StringType(),  True),
    StructField("pm25",         FloatType(),   True),
    StructField("pm10",         FloatType(),   True),
    StructField("co",           FloatType(),   True),
    StructField("so2",          FloatType(),   True),
    StructField("no2",          FloatType(),   True),
    StructField("o3",           FloatType(),   True),
    StructField("temperature",  FloatType(),   True),
    StructField("humidity",     FloatType(),   True),
    StructField("anomaly",      BooleanType(), True),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("AQI-Watch-StreamProcessor")
        .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.shuffle.partitions",           "4")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "org.postgresql:postgresql:42.7.1")
        .getOrCreate()
    )


def load_model():
    """Load model production terbaru dari MLflow registry."""
    try:
        mlflow.set_tracking_uri(MLFLOW_URI)
        model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}/Production")
        log.info("Model '%s' berhasil di-load dari MLflow.", MODEL_NAME)
        return model
    except Exception as e:
        log.warning("Model belum tersedia di MLflow: %s. Inference di-skip.", e)
        return None


def write_stream_agg_to_postgres(batch_df, batch_id: int):
    """Tulis hasil windowed aggregation ke PostgreSQL tabel stream_agg."""
    if batch_df.count() == 0:
        return

    log.info("Batch %d: menulis %d baris ke stream_agg ...", batch_id, batch_df.count())
    (
        batch_df.write
        .format("jdbc")
        .option("url",      POSTGRES_URL)
        .option("dbtable",  "stream_agg")
        .option("user",     POSTGRES_USER)
        .option("password", POSTGRES_PASS)
        .option("driver",   "org.postgresql.Driver")
        .mode("append")
        .save()
    )


def write_predictions_to_postgres(batch_df, batch_id: int, model):
    """Jalankan inference ML dan tulis prediksi ke PostgreSQL tabel predictions."""
    if model is None or batch_df.count() == 0:
        return

    import pandas as pd

    pdf = batch_df.toPandas()

    # Feature engineering
    features = pdf[["pm25_avg", "pm10_avg", "co_avg", "temperature", "humidity"]].copy()
    features["hour_of_day"]  = datetime.now().hour
    features["day_of_week"]  = datetime.now().weekday()
    features = features.fillna(0)

    try:
        predictions    = model.predict(features)
        pdf["predicted_label"] = predictions
        pdf["confidence"]      = 0.85   # placeholder — ganti dengan predict_proba jika tersedia
        pdf["model_version"]   = "Production"

        result = pdf[[
            "station_id", "window_start", "pm25_avg",
            "pm10_avg", "predicted_label", "confidence", "model_version"
        ]]

        spark = SparkSession.getActiveSession()
        result_df = spark.createDataFrame(result)

        (
            result_df.write
            .format("jdbc")
            .option("url",      POSTGRES_URL)
            .option("dbtable",  "predictions")
            .option("user",     POSTGRES_USER)
            .option("password", POSTGRES_PASS)
            .option("driver",   "org.postgresql.Driver")
            .mode("append")
            .save()
        )
        log.info("Batch %d: %d prediksi berhasil disimpan.", batch_id, len(result))

    except Exception as e:
        log.error("Inference gagal pada batch %d: %s", batch_id, str(e))


def main():
    log.info("===== Stream Processor mulai =====")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    model = load_model()

    # ── BACA DARI KAFKA ───────────────────────────────────────
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe",               KAFKA_TOPIC)
        .option("startingOffsets",         "latest")
        .option("failOnDataLoss",          "false")
        .load()
    )

    # ── PARSE JSON PAYLOAD ────────────────────────────────────
    parsed_df = (
        kafka_df
        .select(F.from_json(
            F.col("value").cast("string"),
            PAYLOAD_SCHEMA
        ).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withWatermark("event_time", "10 minutes")
    )

    # ── WINDOWED AGGREGATION ──────────────────────────────────
    # Window 10 menit, slide 5 menit
    windowed_agg = (
        parsed_df
        .groupBy(
            F.col("station_id"),
            F.window(F.col("event_time"), "10 minutes", "5 minutes")
        )
        .agg(
            F.round(F.avg("pm25"),        2).alias("pm25_avg"),
            F.round(F.avg("pm10"),        2).alias("pm10_avg"),
            F.round(F.avg("co"),          2).alias("co_avg"),
            F.round(F.avg("so2"),         2).alias("so2_avg"),
            F.round(F.avg("no2"),         2).alias("no2_avg"),
            F.round(F.avg("o3"),          2).alias("o3_avg"),
            F.round(F.avg("temperature"), 2).alias("temperature"),
            F.round(F.avg("humidity"),    2).alias("humidity"),
            F.count("*").alias("record_count"),
        )
        .select(
            F.col("station_id"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "pm25_avg", "pm10_avg", "co_avg", "so2_avg",
            "no2_avg", "o3_avg", "temperature", "humidity",
            "record_count",
        )
    )

    # ── TULIS KE POSTGRESQL (foreachBatch) ───────────────────
    def process_batch(batch_df, batch_id):
        batch_df.cache()
        write_stream_agg_to_postgres(batch_df, batch_id)
        write_predictions_to_postgres(batch_df, batch_id, model)
        batch_df.unpersist()

    query = (
        windowed_agg
        .writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("update")
        .trigger(processingTime="30 seconds")
        .start()
    )

    log.info("Stream berjalan... tekan Ctrl+C untuk berhenti.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
