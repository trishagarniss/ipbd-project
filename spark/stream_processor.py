import os
import json
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    BooleanType, TimestampType
)

log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "kafka:29092")
KAFKA_TOPIC      = os.getenv("KAFKA_TOPIC",      "air-quality-raw")

POSTGRES_URL     = os.getenv("POSTGRES_URL",      "jdbc:postgresql://postgres:5432/aqi_db")
POSTGRES_USER    = os.getenv("POSTGRES_USER",     "aqi_user")
POSTGRES_PASS    = os.getenv("POSTGRES_PASSWORD", "password123")

MLFLOW_URI       = os.getenv("MLFLOW_URI",        "http://mlflow:5000")
MODEL_NAME       = "aqi-classifier-stream"

CHECKPOINT_PATH  = os.getenv("CHECKPOINT_PATH",   "/tmp/spark-checkpoints/stream_processor")

PAYLOAD_SCHEMA = StructType([
    StructField("station_id",               StringType(),  True),
    StructField("station_name",             StringType(),  True),
    StructField("region",                   StringType(),  True),
    StructField("latitude",                 FloatType(),   True),
    StructField("longitude",                FloatType(),   True),
    StructField("timestamp",                StringType(),  True),
    StructField("pm25",                     FloatType(),   True),
    StructField("pm10",                     FloatType(),   True),
    StructField("co",                       FloatType(),   True),
    StructField("no2",                      FloatType(),   True),
    StructField("so2",                      FloatType(),   True),
    StructField("o3",                       FloatType(),   True),
    StructField("uv_index",                 FloatType(),   True),
    StructField("ispu",                     FloatType(),   True),
    StructField("ispu_category",            StringType(),  True),
    StructField("temperature",              FloatType(),   True),
    StructField("humidity",                 FloatType(),   True),
    StructField("wind_speed",               FloatType(),   True),
    StructField("precipitation",            FloatType(),   True),
    StructField("precipitation_probability",FloatType(),   True),
    StructField("weather_code",             FloatType(),   True),
    StructField("cloud_cover",              FloatType(),   True),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("AQI-Watch-StreamProcessor")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.postgresql:postgresql:42.7.1")
        .getOrCreate()
    )


def load_stream_model():
    import mlflow.sklearn
    try:
        mlflow.set_tracking_uri(MLFLOW_URI)
        model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/Production")
        log.info("Stream model '%s' Production berhasil di-load.", MODEL_NAME)
        return model
    except Exception as e:
        log.warning("Stream model '%s' belum tersedia: %s", MODEL_NAME, e)
        try:
            model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/latest")
            log.info("Stream model '%s' latest berhasil di-load.", MODEL_NAME)
            return model
        except Exception as e2:
            log.warning("Stream model gagal di-load (akan skip predictions): %s", e2)
            return None


def write_stream_agg_to_postgres(batch_df, batch_id: int):
    try:
        count = batch_df.count()
        if count == 0:
            return
        log.info("Batch %d: menulis %d baris ke stream_agg", batch_id, count)
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
    except Exception as e:
        log.error("Batch %d: gagal menulis ke stream_agg: %s", batch_id, e)


def write_stream_predictions_to_postgres(batch_df, batch_id: int, model):
    if model is None:
        return
    try:
        count = batch_df.count()
        if count == 0:
            return

        import pandas as pd
        pdf = batch_df.toPandas()

        features = pdf[[
            "pm25_avg", "pm10_avg", "co_avg", "no2_avg", "so2_avg", "o3_avg",
            "uv_index_avg", "ispu_avg", "temperature_avg", "humidity_avg",
            "wind_speed_avg", "precipitation_sum", "cloud_cover_avg",
        ]].copy()

        ts = pd.to_datetime(pdf["window_start"])
        features["hour_of_day"] = ts.dt.hour
        features["day_of_week"] = ts.dt.dayofweek
        features["is_weekend"] = features["day_of_week"].isin([5, 6]).astype(int)
        features["day_of_month"] = ts.dt.day
        features = features.fillna(0)

        predictions = model.predict(features)
        probs = model.predict_proba(features)
        confidences = probs.max(axis=1)

        result = pdf[[
            "station_id", "window_start", "window_end",
            "pm25_avg", "pm10_avg", "ispu_avg",
        ]].copy()
        result["predicted_label"] = predictions
        result["confidence"] = confidences
        result["model_version"] = MODEL_NAME

        spark = SparkSession.getActiveSession()
        result_df = spark.createDataFrame(result)

        (
            result_df.write
            .format("jdbc")
            .option("url",      POSTGRES_URL)
            .option("dbtable",  "stream_predictions")
            .option("user",     POSTGRES_USER)
            .option("password", POSTGRES_PASS)
            .option("driver",   "org.postgresql.Driver")
            .mode("append")
            .save()
        )
        log.info("Batch %d: %d prediksi stream disimpan", batch_id, len(result))
    except Exception as e:
        log.error("Batch %d: stream inference gagal: %s", batch_id, e)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] stream_processor - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )
    log.info("===== Stream Processor mulai =====")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    model = load_stream_model()
    if model is None:
        log.warning("Stream prediction akan di-skip (model tidak tersedia)")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe",               KAFKA_TOPIC)
        .option("startingOffsets",         "latest")
        .option("failOnDataLoss",          "false")
        .load()
    )

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

    windowed_agg = (
        parsed_df
        .groupBy(
            F.col("station_id"),
            F.window(F.col("event_time"), "10 minutes", "5 minutes")
        )
        .agg(
            F.round(F.avg("pm25"),                      2).alias("pm25_avg"),
            F.round(F.avg("pm10"),                      2).alias("pm10_avg"),
            F.round(F.avg("co"),                        2).alias("co_avg"),
            F.round(F.avg("no2"),                       2).alias("no2_avg"),
            F.round(F.avg("so2"),                       2).alias("so2_avg"),
            F.round(F.avg("o3"),                        2).alias("o3_avg"),
            F.round(F.avg("uv_index"),                  2).alias("uv_index_avg"),
            F.round(F.avg("ispu"),                      2).alias("ispu_avg"),
            F.round(F.avg("temperature"),               2).alias("temperature_avg"),
            F.round(F.avg("humidity"),                  2).alias("humidity_avg"),
            F.round(F.avg("wind_speed"),                2).alias("wind_speed_avg"),
            F.round(F.sum("precipitation"),             2).alias("precipitation_sum"),
            F.round(F.avg("cloud_cover"),               2).alias("cloud_cover_avg"),
            F.count("*").alias("record_count"),
        )
        .select(
            F.col("station_id"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "pm25_avg", "pm10_avg", "co_avg", "no2_avg",
            "so2_avg", "o3_avg", "uv_index_avg",
            "ispu_avg",
            "temperature_avg", "humidity_avg", "wind_speed_avg",
            "precipitation_sum", "cloud_cover_avg", "record_count",
        )
    )

    def process_batch(batch_df, batch_id):
        batch_df.cache()
        try:
            write_stream_agg_to_postgres(batch_df, batch_id)
            write_stream_predictions_to_postgres(batch_df, batch_id, model)
        finally:
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
