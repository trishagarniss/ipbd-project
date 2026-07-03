#!/bin/bash
# submit_streaming.sh — Submit Spark Structured Streaming job
# Jalankan: bash spark/submit_streaming.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

docker compose exec spark-master \
  spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
  --conf spark.sql.ansi.enabled=false \
  --conf spark.sql.streaming.schemaInference=true \
  /opt/airflow/spark/stream_processor.py
