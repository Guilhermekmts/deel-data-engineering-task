# Errors and Fixes Log

This document captures the key errors encountered during setup and implementation, plus the fix applied (or recommended) for each one.

## 1) Docker build failed on Postgres custom image (`postgresql-15-cron`)

**Error**
- `apt-get update && apt-get install -y postgresql-15-cron` failed inside `docker/postgres-db/Dockerfile`
- Messages included unresolved/blocked hosts (`deb.debian.org`, `apt.postgresql.org`) and `Unable to locate package postgresql-15-cron`

**Root Cause**
- Sandbox/network restrictions prevented apt repository access during image build.

**Fix / Action**
- Installation was deferred to a network-enabled environment.
- User triggered Docker stack manually outside restricted sandbox.

---

## 2) PDF requirements could not be read initially

**Error**
- Tooling initially reported PDF parsing unavailable.

**Root Cause**
- Missing Python package support for PDF parsing in the environment.

**Fix / Action**
- Installed `pypdf` and extracted `DEEL-SPARK-TAKE-HOME-TEST.pdf` text with Python.

---

## 3) `spark-submit` not found in Spark container

**Error**
- `exec: "spark-submit": executable file not found in $PATH`

**Root Cause**
- Spark binary existed in `/opt/spark/bin` but was not guaranteed in PATH in runtime context.

**Fix Applied**
- Updated `docker/spark/Dockerfile`:
  - Added `/opt/spark/bin` to `PATH`
  - Added symlink `/usr/local/bin/spark-submit`

---

## 4) Ivy cache file/path failures with `--packages`

### 4.1) Missing cache path

**Error**
- `FileNotFoundException: /home/spark/.ivy2/cache/resolved-...xml`

**Root Cause**
- Runtime Ivy cache directory did not exist or was not writable.

**Fix Applied**
- Temporary: created a writable cache path and passed `spark.jars.ivy` to Spark.

### 4.2) Git Bash path conversion broke Ivy path

**Error**
- `basedir must be absolute: C:/Program Files/Git/workspace/.ivy2/local`

**Root Cause**
- Git Bash converted Linux-style path arguments.

**Fix Applied**
- Added in scripts:
  - `MSYS_NO_PATHCONV=1`
  - `MSYS2_ARG_CONV_EXCL="*"`

### 4.3) Removed runtime Ivy dependency resolution

**Problem**
- Runtime package resolution added fragility.

**Final Fix Applied**
- Removed `--packages` usage from runtime execution.
- Baked required Spark Kafka connector dependencies into image build process.

---

## 5) Kafka topic not hosted / partition errors

**Error**
- `UnknownTopicOrPartitionException: This server does not host this topic-partition`

**Root Cause**
- Spark started before topics/connectors were fully ready (startup race).

**Fix Applied**
- Updated `scripts/run_pipeline.sh` to pre-create expected topics:
  - `finance_db.operations.customers`
  - `finance_db.operations.products`
  - `finance_db.operations.orders`
  - `finance_db.operations.order_items`

---

## 6) `run_pipeline.sh` did not finish

**Observation**
- Script remained running in foreground.

**Root Cause**
- Structured Streaming job is long-running by design.

**Fix Applied**
- Added background mode support:
  - `./scripts/run_pipeline.sh --detach`

---

## 7) Analytics not updating despite source changes

**Error Signal**
- Source rows changed, analytics remained static.
- Connector list returned empty: `[]`

**Root Cause**
- Debezium connector not registered/running.

**Fix Applied**
- Re-ran connector initialization.
- Validated with Kafka Connect status endpoints.

---

## 8) Debezium init script syntax error on Windows line endings

**Error**
- `/init-connectors.sh: line 14: syntax error: unexpected word (expecting "do")`

**Root Cause**
- CRLF line endings in shell script mounted into Linux container.

**Fix Applied**
- Recreated `debezium/init-connectors.sh` with LF endings.
- Added `.gitattributes` rule:
  - `*.sh text eol=lf`

---

## 9) Spark streaming crash: invalid date conversion (`year 20630 out of range`)

**Error**
- `ValueError: year 20630 is out of range`

**Root Cause**
- Debezium/Postgres `DATE` fields were interpreted incorrectly in Python conversion path.

**Fix Applied**
- Updated order schema/normalization in `spark-app/jobs/main.py`:
  - Treated date fields as epoch-day integers
  - Converted via `date_add(lit("1970-01-01"), <days>)`
- Reworked batch row serialization using `toJSON()` + `json.loads()` to avoid problematic internal date deserialization path.

---

## 10) Streaming crash: `KeyError: 'updated_at'`

**Error**
- `KeyError: 'updated_at'` in writer list comprehension

**Root Cause**
- Some CDC records lacked optional fields expected by direct dictionary indexing.

**Fix Applied**
- Replaced direct dict indexing with safe `dict.get()` access in upsert writers.
- Added `.dropna()` guards for required keys in normalization step.

---

## 11) Mart refresh race caused PK conflict

**Error**
- `duplicate key value violates unique constraint "mart_open_orders_by_delivery_status_pkey"`

**Root Cause**
- Concurrent refreshes from multiple streams performed overlapping delete/insert cycles.

**Fix Applied**
- Added transactional advisory lock in mart refresh:
  - `SELECT pg_advisory_xact_lock(424242)`
- Converted mart writes to idempotent upserts (`ON CONFLICT DO UPDATE`).
- Added stale-row cleanup deletes for non-top3 marts.

---

## 12) Non-fatal warnings observed (expected)

### 12.1) Adaptive execution warning
- `spark.sql.adaptive.enabled is not supported in streaming DataFrames/Datasets and will be disabled.`
- This is informational for streaming jobs.

### 12.2) Native Hadoop warning
- `Unable to load native-hadoop library for your platform...`
- Typical in containerized/local Spark, not a hard failure.

### 12.3) AdminClientConfig unused config warning
- Kafka client logs mention some unused consumer-style configs.
- Non-fatal in this context.

---

## Quick Recovery Commands

```bash
docker compose down
rm -rf .spark-checkpoints
docker compose up -d --build
docker compose run --rm debezium-init
./scripts/run_pipeline.sh --detach
docker compose logs -f spark
```

## 13) CDC resilience and late-arrival safety upgrades

**Problem**
- Late or replayed CDC events can overwrite newer state if there is no event ordering guard.
- Break/restart scenarios need deterministic replay and reconciliation support.

**Fix Applied**
- Added event lineage metadata to pipeline payload and staging tables (`analytics_staging.stg_*_cdc`):
  - `source_ts_ms`, `source_lsn`, `kafka_topic`, `kafka_partition`, `kafka_offset`.
- Kept dimensional and fact serving tables (`analytics.dim_*`, `analytics.fact_*`) metadata-free.
- Added idempotent staging inserts with unique Kafka coordinate constraints.
- Added `analytics.pipeline_watermark` to track per-stream/per-partition progress.
- Added reconciliation SQL/script (`spark-app/sql/reconciliation.sql`, `scripts/run_reconciliation.sh`).

## Validation Commands

```bash
docker compose exec kafka-connect curl -s http://localhost:8083/connectors
docker compose exec kafka-connect curl -s http://localhost:8083/connectors/finance-db-connector/status
./scripts/query_metrics.sh open_orders_by_delivery_status
./scripts/query_metrics.sh top3_delivery_dates
./scripts/query_metrics.sh pending_items_by_product
./scripts/query_metrics.sh top3_customers_pending_orders
```
