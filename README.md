## DEEL Spark Take-Home - Online Analytics Pipeline

This repository now contains a full Dockerized baseline for the DEEL Spark take-home assignment, including:

- Source transactional database (`transactions-db`)
- Debezium CDC into Kafka
- Spark Structured Streaming app running in Docker with mounted project volume
- Destination analytics database (`analytics-db`)
- Dimensional and mart tables for required business metrics

## Architecture

Data flow:

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

## Validation Checklist

- Confirm Debezium topics are receiving events.
- Confirm Spark process is running and consuming continuously.
- Validate records in:
  - `analytics.fact_orders_current`
  - `analytics.fact_order_items_current`
  - all four `analytics.mart_*` tables
- Update source tables and verify near-real-time reflection in marts.

## Suggested Evidence to Capture

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
