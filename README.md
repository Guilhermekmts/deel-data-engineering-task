## DEEL Spark Take-Home - Online Analytics Pipeline

This project implements a CDC-driven real-time analytics pipeline.  All processing
dimensions, facts, and operational marts are computed inside **Spark** and only
the final serving layer is persisted in **Postgres**.

A **Jupyter PySpark notebook** is included inside the Spark container for
interactive exploration of both CDC streams and the final analytics tables.

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
Delta silver tables (latest state per entity)
   │
   │ Spark SQL / DataFrame computation
   ▼
Postgres analytics tables (dim/fact/mart)
   │
   │ JDBC
   ▼
PySpark notebook
```

### Why Delta?

Delta Lake provides ACID merge semantics so each micro-batch can safely upsert
the latest CDC state.  The ordering key `source_lsn > source_ts_ms > kafka_offset`
guarantees that **late or replayed CDC events never overwrite newer state**.

### Postgres data model

Schema `analytics` contains only final tables:

| Table | Purpose |
|---|---|
| `dim_customers` | Current customer attributes |
| `dim_products` | Current product attributes |
| `fact_orders_current` | Latest order state with `is_open` / `is_pending` |
| `fact_order_items_current` | Latest order item state |
| `mart_open_orders_by_delivery_status` | Open orders count per delivery/status |
| `mart_top3_delivery_dates_open_orders` | Top 3 delivery dates by open orders |
| `mart_open_pending_items_by_product` | Pending items count per product |
| `mart_top3_customers_pending_orders` | Top 3 customers by pending orders |
| `pipeline_watermark` | Per-stream/per-partition progress |
| `reconciliation_audit` | Consistency checks |

## Quick start

```bash
# 1. Build and start the stack
docker compose up -d --build

# 2. Register the Debezium connector
docker compose run --rm debezium-init

# 3. Run the streaming pipeline
./scripts/run_pipeline.sh

# 4. (Optional) query metrics
./scripts/query_metrics.sh open_orders_by_delivery_status
./scripts/query_metrics.sh top3_delivery_dates
./scripts/query_metrics.sh pending_items_by_product
./scripts/query_metrics.sh top3_customers_pending_orders

# 5. (Optional) start the PySpark notebook
./scripts/start_notebook.sh
# Open http://localhost:8888/?token=deel
```

The Spark `run_pipeline.sh` command uses `--packages` to download the required
JVM connectors (Kafka, Delta, Postgres JDBC) at runtime. The first run may take
a few minutes to fetch them; they are cached in `/tmp/ivy-cache` inside the
container.

`run_pipeline.sh` pre-creates Kafka topics so Spark and Debezium never race.

## Streaming semantics

- `startingOffsets=earliest` ensures nothing is missed on first run.
- Each micro-batch is deduplicated by entity key using
  `source_lsn DESC, source_ts_ms DESC, kafka_offset DESC`.
- Delta `MERGE` rejects stale records and applies deletes (`op='d'`).
- Marts are recomputed inside Spark and overwritten to Postgres on every
  micro-batch.

## Development

Spark code is bind-mounted from the repository, so edits take effect without an
image rebuild.  Checkpoints live in `.spark-checkpoints/` and Delta tables in
`data/delta/`.

## Notebooks

A sample notebook at `notebooks/exploration.ipynb` demonstrates:

- Creating a `SparkSession` with Delta support
- Reading a Kafka CDC topic
- Reading Delta silver tables
- Querying Postgres final tables via JDBC

## Reset

```bash
./scripts/reset.sh
```

This clears containers, volumes, checkpoints, and Delta data for a clean run.
