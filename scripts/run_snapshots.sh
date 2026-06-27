#!/usr/bin/env bash
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

echo "Taking hourly snapshots of current-state tables..."
docker compose exec spark spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.jars.ivy=/tmp/ivy-cache \
  /workspace/spark-app/jobs/snapshot.py 2>&1 | tail -5

echo "Snapshots complete."