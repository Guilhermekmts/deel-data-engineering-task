# DEEL Spark Take-Home Implementation Plan

## Objective

Build a Spark Structured Streaming analytics pipeline that consumes Debezium CDC topics, combines streaming changes with historical data, and exposes near-real-time metrics in a dimensional model.

## Scope from Task Requirements

- Process streaming events from Debezium Kafka topics.
- Join streaming data with historical state for consistent outputs.
- Deliver a dimensional model that supports operational analytics.
- Provide near-real-time access to these metrics:
  - Number of open orders by `delivery_date` and `status`
  - Top 3 delivery dates with the most open orders
  - Number of open pending items by `product_id`
  - Top 3 customers with the most pending orders
- Deliver architecture diagram, Spark sizing strategy, run evidence, Dockerized app, and detailed README.

## Architecture Overview

1. Source DB (`transactions-db` Postgres)
2. Debezium CDC connector (Kafka Connect)
3. Kafka topics (`finance_db.operations.*`)
4. Spark Structured Streaming app (CDC parsing, stateful transformations, upserts)
5. Destination analytical store (Postgres, new DB or schema)
6. Query layer (CLI SQL scripts or API)

## Proposed Repository Structure

```text
spark-app/
  jobs/
  common/
  sql/
docker/
  spark/
docs/
  architecture/
scripts/
IMPLEMENTATION_PLAN.md
README.md
```

## Phased Implementation

### Phase 0 - Project Foundation

- Add Spark app scaffold and Python dependencies.
- Add `.env.example` with Kafka/DB/checkpoint settings.
- Add helper commands (`Makefile` or scripts): up/down/build/run/query.

### Phase 1 - Infrastructure (Docker)

- Keep current source stack (Postgres, Kafka, Debezium).
- Add destination analytics DB service (or dedicated schema on existing DB).
- Add Spark app service/container using bind mount to this repository.
- Add health checks for Kafka Connect and destination DB readiness.

### Spark Deployment Model (Updated)

Deploy Spark inside Docker with the current project folder mounted as a volume for fast iteration.

- Add `spark` service in `docker-compose.yaml`:
  - `working_dir: /workspace`
  - `volumes: ["${PWD}:/workspace"]`
  - `depends_on`: Kafka and DB services
  - environment for internal Docker networking (Kafka `kafka:29092`, Postgres service hostnames)
- Keep Spark code in the mounted folder (`/workspace/spark-app`) so code changes do not require image rebuild.
- Run the streaming app with `spark-submit` from mounted paths.
- Store checkpoints in mounted storage (for example `/workspace/.spark-checkpoints`) to preserve state across container restarts.
- Default execution mode: local Spark inside the container (`--master local[*]`) for assignment simplicity.

### Phase 2 - Dimensional Data Model

Define and create:

- Dimensions:
  - `dim_customer`
  - `dim_product`
  - `dim_date`
  - `dim_order_status`
- Facts:
  - `fact_order_snapshot` (latest order state)
  - `fact_order_item_snapshot` (latest item state)
  - `fact_order_history` (append-only CDC history, recommended)
- Serving marts:
  - `mart_open_orders_by_delivery_status`
  - `mart_top3_delivery_dates_open_orders`
  - `mart_open_pending_items_by_product`
  - `mart_top3_customers_pending_orders`

### Phase 3 - Spark Structured Streaming Pipeline

- Consume Debezium topics:
  - `finance_db.operations.customers`
  - `finance_db.operations.products`
  - `finance_db.operations.orders`
  - `finance_db.operations.order_items`
- Parse Debezium envelope (`before`, `after`, `op`, `ts_ms`).
- Normalize CRUD operations and deduplicate by key + latest event time.
- Bootstrap historical baseline from source DB (JDBC) before continuous updates.
- Join entities to produce dimensional/fact outputs.
- Implement upserts to destination using `foreachBatch` + idempotent merge logic.
- Configure checkpoints per stream in mounted volume and trigger interval (e.g., 10s).

### Phase 4 - Business Metric Layer

Expose required metrics via either:

- CLI SQL scripts (fastest path), or
- Lightweight API (FastAPI)

Required outputs:

- Open orders by `delivery_date` and `status`
- Top 3 delivery dates by open orders
- Open pending items by `product_id`
- Top 3 customers by pending orders

### Phase 5 - Validation and Evidence

- Validate correctness against source DB samples.
- Validate CDC update behavior (insert/update/delete propagation).
- Capture evidence:
  - Spark UI screenshots
  - Output table/query screenshots
  - Connector/stream status snapshots
- Document assumptions for status classification (`open`, `pending`).

### Phase 6 - Documentation and Packaging

- Finalize README with:
  - Setup/run instructions
  - Architecture explanation
  - Data model and table grains
  - Query examples for all required metrics
  - Operational notes (checkpoints, restart behavior)
- Add architecture diagram with component flow and scaling strategy.
- Include Spark sizing rationale and key monitoring metrics.

## Spark Sizing and Monitoring Strategy

Initial baseline:

- Local/small deployment for assignment demo
- Short trigger interval for low latency

Scale factors to document:

- Kafka partitions and ingestion rate
- State-store growth due to joins/dedup
- Micro-batch duration and backlog
- Executor memory/cores and shuffle pressure

Key Spark metrics to track:

- `inputRowsPerSecond`
- `processedRowsPerSecond`
- `batchDuration`
- state operator metrics (rows/state memory)
- end-to-end processing lag

## Acceptance Criteria Checklist

- [ ] Streaming CDC from Debezium topics is consumed continuously.
- [ ] Historical + current state are both queryable.
- [ ] Dimensional model is implemented and documented.
- [ ] All 4 required business metrics are available with near-real-time updates.
- [ ] Application is Dockerized and runnable from repository instructions.
- [ ] README and architecture diagram are complete.
- [ ] Evidence of running pipeline is included.

## Suggested Build Order (Practical)

1. Docker services + destination DB
2. DDL for dimensions/facts/marts
3. CDC parser + one stream end-to-end (orders)
4. Remaining streams and joins
5. Upsert/mart logic
6. Metric query interface
7. Validation, screenshots, and final docs
