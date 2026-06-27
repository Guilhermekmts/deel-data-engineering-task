-- Health & Drift Views for Delta ops tables
-- Query via: spark-sql -f spark-app/sql/health_views.sql

-- 1. Debezium lag (source-side drift)
SELECT slot_name, source_db_lsn, connector_lsn, lag_bytes, captured_at
FROM delta.`/workspace/data/delta/ops/cdc_slot_progress`
ORDER BY captured_at DESC
LIMIT 10;

-- 2. Kafka high-water (transport-side available)
SELECT kafka_topic, kafka_partition, topic_offset, scanned_at
FROM delta.`/workspace/data/delta/ops/kafka_topic_high_water`
ORDER BY scanned_at DESC
LIMIT 20;

-- 3. Processed offsets (stream-side consumed, deduped from append-only table)
SELECT stream_name, kafka_partition,
       MAX(kafka_offset) AS kafka_offset,
       MAX(source_lsn) AS source_lsn
FROM delta.`/workspace/data/delta/ops/processed_offsets`
GROUP BY stream_name, kafka_partition
ORDER BY stream_name, kafka_partition;

-- 4. Drift (processed vs available) — processed_offsets is append-only, dedupe with MAX
SELECT
  p.stream_name,
  p.kafka_partition,
  p.kafka_offset AS processed_offset,
  h.topic_offset AS available_offset,
  (h.topic_offset - p.kafka_offset) AS lag
FROM (
  SELECT stream_name, kafka_partition, kafka_topic,
         MAX(kafka_offset) AS kafka_offset
  FROM delta.`/workspace/data/delta/ops/processed_offsets`
  GROUP BY stream_name, kafka_partition, kafka_topic
) p
JOIN delta.`/workspace/data/delta/ops/kafka_topic_high_water` h
  ON p.kafka_topic = h.kafka_topic
 AND p.kafka_partition = h.kafka_partition
ORDER BY lag DESC;

-- 5. Latest reconciliation results
SELECT check_name, status, source_value, target_value, details, detected_at
FROM delta.`/workspace/data/delta/ops/reconciliation_audit`
ORDER BY detected_at DESC
LIMIT 20;

-- 6. Latest snapshot registry
SELECT stream_name, snapshot_path, snapshot_kafka_offset, snapshot_lsn, captured_at
FROM delta.`/workspace/data/delta/ops/snapshot_registry`
ORDER BY captured_at DESC
LIMIT 20;

-- 7. Recovery audit log
SELECT recovery_id, stream_name, mode, before_state, after_state, started_at, completed_at, actor
FROM delta.`/workspace/data/delta/ops/recovery_audit`
ORDER BY started_at DESC
LIMIT 20;