#!/usr/bin/env bash
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

echo "Running CDC progress check..."
docker compose exec spark spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /workspace/spark-app/jobs/health/cdc_progress.py 2>&1 | tail -5

echo "Running Kafka high-water check..."
docker compose exec spark spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0,org.apache.kafka:kafka-clients:3.5.1 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.jars.ivy=/tmp/ivy-cache \
  /workspace/spark-app/jobs/health/high_water.py 2>&1 | tail -5

echo "Running reconciliation checks..."
docker compose exec spark spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.7.3 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.jars.ivy=/tmp/ivy-cache \
  /workspace/spark-app/jobs/health/reconcile.py 2>&1 | tail -5

echo "Health check cycle complete."