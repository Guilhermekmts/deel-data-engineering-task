## DEEL Spark Take-Home - Online Analytics Pipeline

This repository now contains a full Dockerized baseline for the DEEL Spark take-home assignment, including:

- Source transactional database (`transactions-db`)
- Debezium CDC into Kafka
- Spark Structured Streaming app running in Docker with mounted project volume
- Destination analytics database (`analytics-db`)
- Dimensional and mart tables for required business metrics

## Architecture

```text
operations.* (Postgres)
    │
    │ Debezium CDC
    ▼
Kafka topics (finance_db.operations.*)
    │
    │ Spark Structured Streaming
    ▼
Delta silver tables (partitionBy event_date, CDF enabled)
    │
    │ CDF stream (change data feed)
    ▼
Delta current/* tables (one row per entity, Z-order on entity_id)
    │
    │ Spark SQL / DataFrame computation
    ▼
Postgres analytics tables (dim/fact/mart)
    │
    │ JDBC
    ▼
PySpark notebook

Delta ops layer (data/delta/ops/):
  processed_offsets          per-batch, per-partition consumed offset
  cdc_slot_progress          Debezium vs Postgres WAL LSN
  kafka_topic_high_water     Kafka end-offset per (topic, partition)
  recovery_audit             recovery-mode execution events
  recovery_runbook_state     last-known-good checkpoint per stream
  reconciliation_audit       aggregate-count checks (source vs target)
  batch_metrics              inputRows/lateRows/dedupedRows per batch
  snapshot_registry          hourly compact-state snapshot metadata
```

1. `operations.*` tables in source Postgres (`transactions-db`)
2. Debezium connector publishes CDC events to Kafka topics
3. Spark Structured Streaming consumes topics and applies CDC logic
4. Spark upserts dimensional/fact tables into `analytics-db`
5. Spark refreshes mart tables used for required operational metrics
6. Metrics are queried with helper scripts

Main topics consumed:

- `finance_db.operations.customers`
- `finance_db.operations.products`
- `finance_db.operations.orders`
- `finance_db.operations.order_items`

## Repository Additions

- `docker-compose.yaml`: added `analytics-db` and `spark` services
- `docker/spark/Dockerfile`: Spark runtime image
- `db-scripts/initialize_analytics_ddl.sql`: analytics schema DDL
- `spark-app/jobs/main.py`: streaming pipeline entrypoint
- `spark-app/common/config.py`: runtime settings
- `spark-app/common/db_writer.py`: upserts, history writes, mart refreshes
- `spark-app/sql/metrics.sql`: required metric queries
- `scripts/run_pipeline.sh`: runs Spark job
- `scripts/query_metrics.sh`: queries metrics from `analytics-db`

## Dimensional and Fact Model

Schema: `analytics`

Dimensions:

- `dim_customers`
- `dim_products`

Facts:

- `fact_orders_current` (latest order state)
- `fact_order_items_current` (latest order item state)
- `fact_orders_history` (append-only order change history)
- `fact_order_items_history` (append-only order item change history)

Marts:

- `mart_open_orders_by_delivery_status`
- `mart_top3_delivery_dates_open_orders`
- `mart_open_pending_items_by_product`
- `mart_top3_customers_pending_orders`

## Open/Pending Logic

- `is_open`: `status != 'COMPLETED'`
- `is_pending`: `status in ('PENDING', 'PROCESSING', 'REPROCESSING')`

This logic is implemented in `spark-app/common/db_writer.py` and can be adjusted if needed.

## Prerequisites

- Docker + Docker Compose available locally
- Internet access for Docker image pulls and Spark package download on first run

## Run

From repository root:

1. Build and start stack

```bash
docker compose up -d --build
```

2. Register Debezium connector (if not already done by startup)

```bash
docker compose logs debezium-init
```

3. Run Spark pipeline

```bash
./scripts/run_pipeline.sh
```

This is a streaming job, so it is expected to keep running (it does not exit on success).
To start it in background mode:

```bash
./scripts/run_pipeline.sh --detach
```

Useful commands:

```bash
docker compose logs -f spark
docker compose restart spark
```

The runner pre-creates required Kafka topics to avoid startup race conditions between Debezium and Spark.

Spark is deployed inside Docker and runs against mounted code in `/workspace`.

For Git Bash on Windows, disable path conversion for docker exec commands:

```bash
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"
```

The provided scripts already set these variables automatically.

Windows line endings note:

- The repository enforces LF for shell scripts via `.gitattributes` (`*.sh text eol=lf`).
- If you already cloned before this rule, normalize once:

```bash
git add --renormalize .
git status
```

If `spark-submit` is not found in the container, rebuild the Spark image:

```bash
docker compose build spark
docker compose up -d
```

Spark Kafka connector dependencies are baked into the Spark image during build from Maven downloads, so runtime does not depend on Ivy package resolution.

## Query Required Metrics

Run any of the following:

```bash
./scripts/query_metrics.sh open_orders_by_delivery_status
./scripts/query_metrics.sh top3_delivery_dates
./scripts/query_metrics.sh pending_items_by_product
./scripts/query_metrics.sh top3_customers_pending_orders
```

You can also run all SQLs from `spark-app/sql/metrics.sql`.

## Spark Deployment Notes

- Spark service mounts the current folder with:
  - `${PWD}:/workspace`
  - `${PWD}/.spark-checkpoints:/workspace/.spark-checkpoints`
- This allows editing code locally without rebuilding the Spark image.
- Streaming checkpoints persist locally in `.spark-checkpoints/`.

- `startingOffsets=earliest` ensures nothing is missed on first run.
- Each micro-batch is deduplicated by entity key using
  `source_lsn DESC, source_ts_ms DESC, kafka_offset DESC`.
- Silver tables are append-only CDC logs, partitioned by `event_date`, with Delta CDF enabled.
- Compact current-state tables (one row per entity) are maintained by consuming silver CDF
  via `MERGE` with ordering guard.
- Marts are computed from bounded current-state tables — no full silver scan per batch.
- All ops metadata (processed offsets, Kafka high-water, CDC progress, reconciliation, recovery audit)
  lives in `data/delta/ops/*` as Delta tables, not in Postgres.
- Postgres serves only the final analytics serving layer (`analytics.{dim,fact,mart}_*`).

## Recovery

When Kafka topics break or offsets/partitions are lost, use `scripts/recover_stream.sh`:

```bash
# Preview recovery plan (dry-run)
./scripts/recover_stream.sh operations.customers --mode A

# Execute snapshot-based recovery
./scripts/recover_stream.sh operations.orders --mode B --apply

# Partition reset (wipes checkpoint, restores snapshot)
./scripts/recover_stream.sh operations.order_items --mode C --apply
```

Three recovery modes (all run a replication slot health pre-check before proceeding):

| Mode | Description | When to use |
|---|---|---|
| **A** Catch-up replay | Resume from last checkpoint; idempotent MERGE | Default — minor lag |
| **B** Snapshot + replay | Restore hourly snapshot; replay Kafka from snapshot offset; verifies LSN gap vs replication slot | Topic lost / retention expiry |
| **C** Partition reset | Wipe checkpoint + restore snapshot + replay from earliest | `failOnDataLoss` would have thrown |

## Health monitoring

```bash
# Full drift report (CDC lag, Kafka lag, reconciliation)
./scripts/query_metrics.sh --drift

# Run health cycle once
./scripts/run_health.sh

# View all ops tables
docker compose exec spark spark-sql \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  -f /workspace/spark-app/sql/health_views.sql
```

- Confirm Debezium topics are receiving events.
- Confirm Spark process is running and consuming continuously.
- Validate records in:
  - `analytics.fact_orders_current`
  - `analytics.fact_order_items_current`
  - all four `analytics.mart_*` tables
- Update source tables and verify near-real-time reflection in marts.

Spark code is bind-mounted from the repository, so edits take effect without an
image rebuild.  Checkpoints live in `.spark-checkpoints/`, Delta tables in
`data/delta/` (silver, current, snapshots, ops), and Postgres data in Docker volumes.

- Spark job logs and Spark UI screenshots
- Sample rows from fact/mart tables
- Before/after screenshots of metrics after source updates

## Useful Commands

```bash
docker compose ps
docker compose logs -f spark
docker compose exec analytics-db psql -U analytics_user -d analytics_db -c "\dt analytics.*"
docker compose down
```

This clears containers, volumes, checkpoints, and all Delta data (silver, current, ops, snapshots) for a clean run.
