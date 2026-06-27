# Late-Arrival & Topic Breakage — Execution Runbook

This document contains ready-to-run commands for every new handler, including
expected output snippets and incident-response workflows.

---

## 1. Starting the pipeline (including compact job)

```bash
# Detached (background) mode
./scripts/run_pipeline.sh --detach

# Foreground mode (logs stream to terminal)
./scripts/run_pipeline.sh
```

On first run you will see both the main pipeline and compact job starting:

```
Pipeline started in background.
Starting compact current-state job...
Compact job started in background.
Use: docker compose logs -f spark
```

**Verify both are alive:**

```bash
docker compose logs spark | grep -E "(received batch|compact:)"
```

Expected output (after 10-15s):

```
2026-06-27 01:20:00 INFO delta_manager: [operations.customers:0] received batch with 12 row(s)
2026-06-27 01:20:00 INFO delta_manager: [compact:operations.customers:0] received 12 row(s)
2026-06-27 01:20:00 INFO delta_manager: [compact:operations.customers:0] merged 12 row(s) into /workspace/data/delta/current/customers
```

---

## 2. Drift detection

### 2a. Full drift report (one command)

```bash
./scripts/query_metrics.sh --drift
```

**Example output:**

```
=== CDC Slot Progress (Debezium lag) ===
 slot_name   | source_db_lsn | connector_lsn | lag_bytes | captured_at
-------------+---------------+--------------+-----------+--------------------
 cdc_pgoutput | 28734982144   | 28734982144  | 0         | 2026-06-27 01:20:00

=== Kafka High-Water vs Processed Offsets (lag) ===
 stream_name            | partition | processed_offset | available_offset | lag
------------------------+-----------+-----------------+-----------------+-----
 operations.customers   | 0         | 42              | 42              | 0
 operations.orders      | 0         | 87              | 87              | 0

=== Latest Reconciliation Results ===
 check_name      | status | source_value | target_value | details
-----------------+--------+-------------+-------------+-----------------------------------------
 count_customers | OK     | 50          | 50          | Counts match: source=50, target=50
 count_orders    | OK     | 120         | 120         | Counts match: source=120, target=120
```

### 2b. Low-level SQL queries against Delta ops tables

```bash
docker compose exec spark spark-sql \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  -f /workspace/spark-app/sql/health_views.sql
```

### 2c. Manual CDC progress check

```bash
docker compose exec spark spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.7.3 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /workspace/spark-app/jobs/health/cdc_progress.py
```

### 2d. Manual Kafka high-water check

```bash
docker compose exec spark spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /workspace/spark-app/jobs/health/high_water.py
```

### 2e. Manual reconciliation run

```bash
docker compose exec spark spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.7.3 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /workspace/spark-app/jobs/health/reconcile.py
```

**Expected output:**

```
2026-06-27 01:25:00 INFO reconcile: Reconciliation count_customers: OK
2026-06-27 01:25:01 INFO reconcile: Reconciliation count_products: OK
2026-06-27 01:25:02 INFO reconcile: Reconciliation count_orders: OK
2026-06-27 01:25:03 INFO reconcile: Reconciliation count_order_items: OK
2026-06-27 01:25:03 INFO reconcile: Reconciliation complete
```

---

## 3. Recovery (topic/partition breakage)

Three recovery modes. All are **dry-run by default** — use `--apply` only when ready.
All modes run a **replication slot health pre-check** before any recovery action.

### Pre-check behavior

```
============================================
 Pre-check: Replication Slot Status
============================================
 Slot: cdc_pgoutput
 Active: t
 restart_lsn: 0/16B6A48
 confirmed_flush_lsn: 0/16BA000
 ✓ Slot pre-check passed
```

The recovery **aborts** immediately if:
- Slot does not exist → create it with `docker compose run --rm debezium-init`
- Slot is inactive (`active=f`) → restart the Debezium connector
- `restart_lsn` is NULL → WAL may have been recycled; bootstrap from Postgres needed

### 3a. Mode A — Catch-up replay (minor lag)

Use when the pipeline fell behind but checkpoint + Kafka are both intact.

```bash
# Dry-run
./scripts/recover_stream.sh operations.customers --mode A
```

**Expected output:**

```
============================================
 Recovery Plan for stream: operations.customers
 Mode: A
 Dry-run: YES (use --apply to execute)
============================================

1. Verify checkpoint exists: .spark-checkpoints/customers
   ✓ Checkpoint found

2. Verify processed_offsets in Delta ops

3. Restart main pipeline
============================================
 Dry-run complete. No changes made.
 Re-run with --apply to execute recovery.
============================================
```

```bash
# Execute
./scripts/recover_stream.sh operations.customers --mode A --apply
```

### 3b. Mode B — Snapshot + replay (topic lost / retention expiry)

Use when Kafka no longer has the full history (topic deleted, retention window expired).

```bash
# Dry-run
./scripts/recover_stream.sh operations.orders --mode B
```

**Expected output:**

```
============================================
 Pre-check: Replication Slot Status
============================================
 Slot: cdc_pgoutput
 Active: t
 restart_lsn: 0/16B6A48
 confirmed_flush_lsn: 0/16BA000
 ✓ Slot pre-check passed

============================================
 Recovery Plan for stream: operations.orders
 Mode: B
 Dry-run: YES (use --apply to execute)
============================================

1. Find latest snapshot for operations.orders

2. CLONE snapshot back to current-state table
   SHALLOW CLONE data/delta/snapshots/orders_20260627_010000

3. Delete compact checkpoint
   rm -rf .spark-checkpoints/compact_orders

4. Restart compact job + main pipeline

5. Slot context: restart_lsn=0/16B6A48, confirmed_flush_lsn=0/16BA000
============================================
 Dry-run complete. No changes made.
============================================
```

```bash
# Execute
./scripts/recover_stream.sh operations.orders --mode B --apply
```

### 3c. Mode C — Partition reset (checkpoint divergent)

Use when `failOnDataLoss` would have thrown — checkpoints are out of sync with Kafka.

```bash
# Dry-run
./scripts/recover_stream.sh operations.order_items --mode C
```

**Expected output:**

```
============================================
 Recovery Plan for stream: operations.order_items
 Mode: C
 Dry-run: YES (use --apply to execute)
============================================

1. Wipe Spark checkpoint: rm -rf .spark-checkpoints/order_items
2. Wipe compact checkpoint: rm -rf .spark-checkpoints/compact_order_items
3. Restore from snapshot or re-bootstrap
4. Restart pipeline with startingOffsets=earliest
============================================
 Dry-run complete. No changes made.
 Re-run with --apply to execute recovery.
============================================
```

```bash
# Execute
./scripts/recover_stream.sh operations.order_items --mode C --apply
```

---

## 4. Snapshots

### 4a. Automatic (hourly)

The `snapshot-runner` service in docker-compose runs `scripts/run_snapshots.sh` every 3600 seconds.
No action needed.

### 4b. Manual snapshot (on-demand)

```bash
./scripts/run_snapshots.sh
```

**Expected output:**

```
Taking hourly snapshots of current-state tables...
2026-06-27 02:00:00 INFO snapshot: Taking snapshot of /workspace/data/delta/current/customers -> /workspace/data/delta/snapshots/customers_20260627_020000
2026-06-27 02:00:01 INFO snapshot: Snapshot /workspace/data/delta/snapshots/customers_20260627_020000: max kafka_offset=87, max source_lsn=28734982144
...
Snapshots complete.
```

### 4c. List available snapshots

```bash
docker compose exec spark spark-sql \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  -e "SELECT stream_name, snapshot_path, snapshot_kafka_offset, captured_at
      FROM delta.\`/workspace/data/delta/ops/snapshot_registry\`
      ORDER BY captured_at DESC
      LIMIT 20;"
```

---

## 5. Health watcher (automatic)

The `health-watcher` service runs every 60 seconds and executes `scripts/run_health.sh`.
Logs are visible via:

```bash
docker compose logs health-watcher
```

**Expected log lines:**

```
Running health checks...
2026-06-27 01:20:00 INFO cdc_progress: CDC progress: source_lsn=28734982144, connector_lsn=28734982144, lag=0 bytes
2026-06-27 01:20:01 INFO high_water: Kafka high-water written to /workspace/data/delta/ops/kafka_topic_high_water
2026-06-27 01:20:02 INFO reconcile: Reconciliation count_customers: OK
2026-06-27 01:20:03 INFO reconcile: Reconciliation count_products: OK
...
Health check cycle complete.
Sleeping 60 seconds...
```

---

## 6. Browsing the Delta ops layer

### 6a. List all ops tables

```bash
ls -la data/delta/ops/
```

**Expected:**

```
drwxr-xr-x cdc_slot_progress
drwxr-xr-x kafka_topic_high_water
drwxr-xr-x processed_offsets
drwxr-xr-x reconciliation_audit
drwxr-xr-x recovery_audit
drwxr-xr-x snapshot_registry
```

### 6b. Read an ops table from the notebook

```python
spark.read.format("delta").load("/workspace/data/delta/ops/processed_offsets").show()
spark.read.format("delta").load("/workspace/data/delta/ops/cdc_slot_progress").orderBy("captured_at", ascending=False).show(5)
spark.read.format("delta").load("/workspace/data/delta/ops/reconciliation_audit").orderBy("detected_at", ascending=False).show(10)
```

### 6c. Read from spark-sql

```bash
docker compose exec spark spark-sql \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  -e "SELECT * FROM delta.\`/workspace/data/delta/ops/recovery_audit\` ORDER BY started_at DESC;"
```

---

## 7. Incident-response workflows

### Scenario A: Pipeline fell behind (lag > 0)

```
query_metrics.sh --drift
  → shows lag > 0 for one or more streams
```

```
recover_stream.sh <stream> --mode A
  → dry-run confirms checkpoint exists
recover_stream.sh <stream> --mode A --apply
  → pipeline restarts, compact catches up
query_metrics.sh --drift
  → lag = 0
```

### Scenario B: Topic was accidentally deleted and re-created

```
query_metrics.sh --drift
  → kafka_topic_high_water shows new offsets starting at 0
  → processed_offsets still shows old high offset
  → main pipeline may crash with UnknownTopicOrPartitionException
```

```
recover_stream.sh <stream> --mode B
  → dry-run shows latest snapshot and replay offset
recover_stream.sh <stream> --mode B --apply
  → snapshot restored, checkpoint wiped for compact, pipeline restarts
query_metrics.sh --drift
  → lag = 0
```

### Scenario C: Spark checkpoint corrupted / out of sync

```
recover_stream.sh <stream> --mode C
  → dry-run shows checkpoint wipe + snapshot restore plan
recover_stream.sh <stream> --mode C --apply
  → both checkpoints wiped, snapshot restored, pipeline replays from offset
query_metrics.sh --drift
  → lag = 0
```

### Scenario D: Debezium connector lagging

```
query_metrics.sh --drift
  → CDC lag_bytes > 0 (e.g., 500 MB behind)
```

```
# 1. Check connector status
docker compose exec kafka-connect curl -s http://localhost:8083/connectors/finance-db-connector/status

# 2. If connector is healthy, wait; lag is transient
# 3. If connector is FAILED:
docker compose exec kafka-connect curl -X DELETE http://localhost:8083/connectors/finance-db-connector
docker compose run --rm debezium-init

# 4. Run catch-up recovery to re-read from earliest
recover_stream.sh <stream> --mode A --apply
query_metrics.sh --drift
  → lag_bytes → 0
```

### Scenario E: Source DB replayed WAL / LSN rolled back

```
query_metrics.sh --drift
  → reconciliation shows DRIFT (count mismatch)
```

```
# Run fresh reconciliation
docker compose exec spark spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.7.3 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /workspace/spark-app/jobs/health/reconcile.py

# If DRIFT persists, perform a full re-bootstrap via Mode B recovery
recover_stream.sh <stream> --mode B --apply
```

---

## 8. Quick reference (cheat sheet)

| Action | Command |
|---|---|
| Start pipeline | `./scripts/run_pipeline.sh --detach` |
| Drift report | `./scripts/query_metrics.sh --drift` |
| Health cycle | `./scripts/run_health.sh` |
| Manual snapshot | `./scripts/run_snapshots.sh` |
| Recovery dry-run | `./scripts/recover_stream.sh <stream> --mode A` |
| Recovery execute | `./scripts/recover_stream.sh <stream> --mode A --apply` |
| View ops tables | `docker compose exec spark spark-sql -f /workspace/spark-app/sql/health_views.sql` |
| Check logs | `docker compose logs -f spark` |
| Check health-watcher | `docker compose logs health-watcher` |
| Full reset | `./scripts/reset.sh` |