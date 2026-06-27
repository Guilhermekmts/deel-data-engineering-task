#!/usr/bin/env bash
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <metric>"
  echo "Metrics: open_orders_by_delivery_status | top3_delivery_dates | pending_items_by_product | top3_customers_pending_orders | --drift"
  exit 1
fi

metric="$1"

if [[ "$metric" == "--drift" ]]; then
  echo "=== CDC Slot Progress (Debezium lag) ==="
  docker compose exec spark spark-submit \
    --master local[*] \
    --packages io.delta:delta-spark_2.12:3.2.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.jars.ivy=/tmp/ivy-cache \
    --class org.apache.spark.examples.SparkPi \
    /dev/null 2>/dev/null || true

  docker compose exec spark spark-sql \
    --packages io.delta:delta-spark_2.12:3.2.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    -e "
    SELECT slot_name, source_db_lsn, connector_lsn, lag_bytes, captured_at
    FROM delta.\`/workspace/data/delta/ops/cdc_slot_progress\`
    ORDER BY captured_at DESC
    LIMIT 5;" 2>&1 | grep -v "^SLF4J\|^WARNING\|^log4j\|^WARN" || echo "No CDC progress data"

  echo ""
  echo "=== Kafka High-Water vs Processed Offsets (lag) ==="
  docker compose exec spark spark-sql \
    --packages io.delta:delta-spark_2.12:3.2.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    -e "
    SELECT
      p.stream_name,
      p.kafka_partition,
      p.kafka_offset AS processed_offset,
      h.topic_offset AS available_offset,
      (h.topic_offset - p.kafka_offset) AS lag
    FROM (
      SELECT stream_name, kafka_partition, kafka_topic,
             MAX(kafka_offset) AS kafka_offset
      FROM delta.\`/workspace/data/delta/ops/processed_offsets\`
      GROUP BY stream_name, kafka_partition, kafka_topic
    ) p
    JOIN delta.\`/workspace/data/delta/ops/kafka_topic_high_water\` h
      ON p.kafka_topic = h.kafka_topic
     AND p.kafka_partition = h.kafka_partition
    ORDER BY lag DESC;" 2>&1 | grep -v "^SLF4J\|^WARNING\|^log4j\|^WARN" || echo "No drift data"

  echo ""
  echo "=== Latest Reconciliation Results ==="
  docker compose exec spark spark-sql \
    --packages io.delta:delta-spark_2.12:3.2.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    -e "
    SELECT check_name, status, source_value, target_value, details, detected_at
    FROM delta.\`/workspace/data/delta/ops/reconciliation_audit\`
    ORDER BY detected_at DESC
    LIMIT 10;" 2>&1 | grep -v "^SLF4J\|^WARNING\|^log4j\|^WARN" || echo "No reconciliation data"
  exit 0
fi

case "$metric" in
  open_orders_by_delivery_status)
    sql="SELECT delivery_date, status, open_orders, updated_at FROM analytics.mart_open_orders_by_delivery_status ORDER BY delivery_date, status;"
    ;;
  top3_delivery_dates)
    sql="SELECT rank_position, delivery_date, open_orders, updated_at FROM analytics.mart_top3_delivery_dates_open_orders ORDER BY rank_position;"
    ;;
  pending_items_by_product)
    sql="SELECT product_id, pending_items, updated_at FROM analytics.mart_open_pending_items_by_product ORDER BY pending_items DESC, product_id;"
    ;;
  top3_customers_pending_orders)
    sql="SELECT rank_position, customer_id, pending_orders, updated_at FROM analytics.mart_top3_customers_pending_orders ORDER BY rank_position;"
    ;;
  *)
    echo "Invalid metric: $metric"
    exit 1
    ;;
esac

docker compose exec analytics-db psql -U analytics_user -d analytics_db -c "$sql"
