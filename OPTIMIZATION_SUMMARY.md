# Optimization Summary — Billion-Row Scale Adaptations

> **Note:** This document describes both the current pipeline implementation and the planned optimizations
> for billion-row scale. Ongoing work is tracked in the
> [`feature/spark_streaming_process`](https://github.com/GuilhermeMatsumoto/deel-data-engineering-task/tree/feature/spark_streaming_process)
> branch, which already covers several of the optimizations and edge cases discussed below.
> The current `main` branch contains the fully working baseline pipeline.

## The Scaling Problem

The current pipeline works by **recomputing all dimensions, facts, and marts from scratch on every micro-batch** (every 10 seconds). It reads all four Delta silver tables in full, applies a `_latest_by_key` window over the entire history, then writes 8 tables to Postgres.

This is **correct but not scalable**:

| Current Behavior | At 1M rows | At 1B rows | At 10B rows |
|---|---|---|---|
| Full silver scan per batch | ~1M rows read | ~1B rows read | ~10B rows read |
| `_latest_by_key` window | Fast | Full shuffle — minutes | Full shuffle — hours |
| Postgres overwrite | Trivial | 8 full table writes | Lock contention |
| Recovery time | Seconds | Hours of replay | Days of replay |

The sections below describe the **specific architectural changes** needed to scale from millions to billions, organized by bottleneck.

---

## Bottleneck 1: Full Silver Scan on Every Batch

### Current Implementation

In `delta_manager.py:compute_dimensions_and_facts()`, every micro-batch calls `read_silver()` which reads **all historical CDC data** from Delta, then applies `_latest_by_key()` over the full dataset:

```
spark.read.format("delta").load(delta_path)  ← full scan of all history
  → _latest_by_key()                         ← full shuffle over all history
  → filter op != 'd'                         ← cheap
  → select dim/fact columns                  ← cheap
```

This is the #1 scaling bottleneck.

### Proposed Solution: Compact + Current-State Layer

Introduce a **separate current-state Delta table** (one row per entity) that is incrementally updated via MERGE:

```
data/delta/current/customers/       ← compacted, one row per customer_id
data/delta/silver_customers/        ← still append-only, full history
```

**The compact job** (new `spark-app/jobs/compact.py`) would be a **separate streaming query** that:

1. Reads the Delta **Change Data Feed (CDF)** from silver (only new rows since last commit)
2. Merges into current-state with an LSN ordering guard:

```sql
MERGE INTO current.customers AS target
USING silver_changes AS source
ON target.customer_id = source.customer_id
WHEN MATCHED AND source.source_lsn > target.source_lsn
  THEN UPDATE SET *
WHEN NOT MATCHED
  THEN INSERT *
```

**The analytics job** (`delta_manager.py:compute_dimensions_and_facts()`) would then read from **current-state** instead of **silver**:

| Reads from | Rows scanned | Scales to |
|---|---|---|
| Silver (current) | All CDC history (billions) | Millions |
| Current-state (proposed) | One row per entity (millions) | Billions |

### Implementation Status

| Piece | Status | File |
|---|---|---|
| Silver layer (append-only) | **Implemented** | `spark-app/common/delta_manager.py` |
| `_latest_by_key` from full silver | **Implemented** | `spark-app/common/delta_manager.py:178` |
| Current-state Delta tables | **Planned** | `data/delta/current/*` |
| Compact streaming job (silver CDF → current MERGE) | **Planned** | `spark-app/jobs/compact.py` |
| Analytics reads from current-state instead of silver | **Planned** | `spark-app/common/delta_manager.py` |

---

## Bottleneck 2: No Partition Pruning on Silver

### Current Implementation

Silver tables have **no partitioning** configured. Every `read_silver()` call reads all Parquet files across all history.

### Proposed Solution: Partition by `event_date`

Add `event_date` partitioning to silver tables, derived from the CDC timestamp (not wall clock):

```
data/delta/silver_customers/
  event_date=2026-06-26/
    part-00001.parquet
  event_date=2026-06-27/
    part-00002.parquet
```

**Benefit:** Queries that filter on time windows prune entire date directories without scanning them. Even if CDF is not used, a compact job can process "data since last run" by reading only the latest partitions.

### Implementation Status

| Piece | Status | File |
|---|---|---|
| `event_date` field in normalized CDC data | **Not implemented** | `spark-app/jobs/main.py:normalize_*()` |
| `partitionBy("event_date")` in silver writes | **Not implemented** | `spark-app/common/delta_manager.py:append_to_delta()` |
| Table property `delta.autoOptimize.optimizeWrite = true` | **Not implemented** | Set on Delta table creation |

---

## Bottleneck 3: Postgres Overwrite on Every Batch

### Current Implementation

`refresh_analytics_final_layer()` writes **all 8 tables** to Postgres via TRUNCATE + INSERT on every micro-batch, under an advisory lock. At billion-row scale:

| Operation | Cost |
|---|---|
| TRUNCATE 8 tables | Fast |
| INSERT 8 tables | Full data movement over JDBC — minutes |
| Advisory lock held during writes | Blocks concurrent refreshes |

### Proposed Solution: Marts via Delta Application

Instead of overwriting Postgres on every batch, the marts can be computed **incrementally** using current-state Delta tables:

1. Marts compute from current-state (compact, bounded by entity count — millions, not billions)
2. The recompute is cheap enough to run every micro-batch without Postgres I/O
3. Postgres writes are moved to a **separate, slower cadence** (e.g., every 5 minutes or on-demand)

The current approach of overwriting Postgres every 10 seconds is fine for small data but becomes the bottleneck at scale.

### Implementation Status

| Piece | Status | File |
|---|---|---|
| Full recompute + Postgres write every batch | **Implemented** | `delta_manager.py:refresh_analytics_final_layer()` |
| Marts computed from current-state (not silver) | **Planned** | Depends on current-state layer |
| Cadence decoupling (Postgres write less often) | **Planned** | `delta_manager.py` |
| Empty current-state guard | **Planned** | Prevents overwriting with 0 rows |

---

## Bottleneck 4: Unbounded Recovery Time

### Current Implementation

The pipeline uses `startingOffsets=earliest`. If the pipeline goes down and the Kafka topic is deleted or retention expires, **replaying from the earliest offset** means processing all of Kafka history — potentially days or weeks of data.

### Proposed Solution: Hourly Snapshots + 3 Recovery Modes

**Snapshots** are zero-copy `SHALLOW CLONE` of the current-state tables, taken hourly:

```
data/delta/snapshots/customers_20260627_010000/
  → SHALLOW CLONE of data/delta/current/customers
```

This bounds the recovery replay window to **at most 1 hour** (the snapshot interval), regardless of Kafka retention.

**Three recovery modes** handle different failure severities:

| Mode | When | Recovery Action | Replay Window |
|---|---|---|---|
| **A** — Catch-up | Checkpoint + Kafka intact | Resume from last Spark checkpoint | 0 (no re-read) |
| **B** — Snapshot + Replay | Topic deleted / partition count changed | `SHALLOW CLONE` snapshot → recreate topic → replay from `confirmed_flush_lsn` | ≤ 1 hour |
| **C** — Partition Reset | Checkpoint corrupted | Wipe everything → restore from snapshot → replay from earliest Kafka | Full retention |

**Pre-condition for all modes:** Verify Postgres replication slot health. Because `slot.drop.on.stop=false` and WAL is retained, Postgres can replay every missed event from `confirmed_flush_lsn` onward.

### Implementation Status

| Piece | Status | File |
|---|---|---|
| `startingOffsets=earliest` + `failOnDataLoss=false` | **Implemented** | `spark-app/jobs/main.py:topic_stream()` |
| Recovery Mode A (resume from checkpoint) | **Implemented** | Automatic via Spark Structured Streaming |
| Snapshot mechanism (SHALLOW CLONE) | **Planned** | `spark-app/jobs/snapshot.py` |
| Snapshot metadata registry | **Planned** | `data/delta/ops/snapshot_registry` |
| Recovery Mode B (snapshot + replay) | **Planned** | `spark-app/common/recovery.py`, `scripts/recover_stream.sh` |
| Recovery Mode C (full reset) | **Planned** | `scripts/recover_stream.sh` |
| Slot health pre-check | **Planned** | `scripts/recover_stream.sh` |
| Recovery audit logging | **Planned** | `data/delta/ops/recovery_audit` |

---

## Bottleneck 5: No Incremental Mart Computation

### Current Implementation

Marts are recomputed from scratch on every batch via `compute_marts()` in `delta_manager.py:252`. It groups, aggregates, ranks, and limits over all data.

At billion-row scale, even reading from a compacted current-state table (millions of rows), these aggregations are:
- Full table scans (no pre-aggregation)
- Full shuffles for `groupBy`, `orderBy`, `row_number()`
- Expensive joins (e.g., `pending_items` joins orders + items)

### Proposed Solution: Pre-Aggregated Mart Tables

Replace full recompute with **incremental mart updates** where possible:

| Mart | Current | Proposed |
|---|---|---|
| `open_orders_by_delivery_status` | Full groupBy on all orders | MERGE delta of changed (delivery_date, status) pairs |
| `top3_delivery_dates` | Full rank + limit over all open orders | Maintain sorted leaderboard, adjust on change |
| `pending_items_by_product` | Full join orders + items + filter + groupBy | Update only products whose orders changed state |
| `top3_customers_pending` | Full rank + limit over all customers | Maintain sorted leaderboard, adjust on change |

The key insight: when current-state is updated for a single entity, only the rows affected by that entity's change need to be recomputed in the marts.

### Implementation Status

| Piece | Status | File |
|---|---|---|
| Full recompute of all marts | **Implemented** | `delta_manager.py:compute_marts()` |
| Incremental mart updates (delta-application) | **Planned** | `spark-app/common/delta_manager.py` |
| Leaderboard maintenance (top3 marts) | **Planned** | `spark-app/common/delta_manager.py` |

---

## Bottleneck 6: Single-Threaded Final Layer Write

### Current Implementation

`refresh_analytics_final_layer()` acquires `pg_advisory_xact_lock(424242)` which means **only one stream at a time** can write to Postgres. With 4 streams, three are blocked waiting while the lock is held.

At billion-row scale, writing 8 tables via JDBC takes minutes, causing backpressure that delays all streams.

### Proposed Solution: Partition-Isolated Ops + Non-Critical Path Isolation

Decouple the critical path (silver append) from the non-critical path (Postgres write):

```
Micro-batch arrives
  → Dedupe (critical, fast)
  → Append to silver Delta (critical, fast)
  → try:
      Refresh Postgres (non-critical, slow)
    except:
      LOGGER.warning("Postgres write failed")  ← non-blocking
```

The `processed_offsets` ops table (used only for drift detection) is already wrapped in `try/except` in the design — this prevents health monitoring lag from blocking the pipeline.

### Implementation Status

| Piece | Status | File |
|---|---|---|
| Advisory lock on Postgres write | **Implemented** | `delta_manager.py:refresh_analytics_final_layer()` |
| Non-critical path isolation (try/except) | **Planned** | `spark-app/common/delta_manager.py` |
| Partition-isolated ops tables | **Planned** | `data/delta/ops/processed_offsets` |
| APPEND + partitionBy + MAX() pattern | **Planned** | `spark-app/common/delta_manager.py` |

---

## Bottleneck 7: Late-Arrival Data Overwrites Newer State

### Current Implementation

The `_latest_by_key()` window in `delta_manager.py:178` already handles late arrivals correctly by ordering by `source_lsn DESC, source_ts_ms DESC, kafka_offset DESC`. This works because Postgres LSN is **monotonic** — a late-arriving event will have a lower or equal LSN and be discarded.

However, the protection relies on reading **all history** to find the latest. At billion-row scale, the shuffle is too expensive.

### Proposed Solution: LSN Ordering Guard in Current-State MERGE

The same LSN chain concept but applied incrementally:

```
Postgres WAL (monotonic LSN)
  → Debezium embeds source.lsn in Kafka message
  → Spark extracts source_lsn
  → Silver stores source_lsn (append-only)
  → CDF streams changes
  → Compact MERGE into current-state:
      source_lsn > target.source_lsn  →  apply
      source_lsn == target.source_lsn →  reject (duplicate)
      source_lsn < target.source_lsn  →  reject (late arrival)
```

This is the same correctness guarantee as the current `_latest_by_key()` but avoids the full-history scan and shuffle.

### Implementation Status

| Piece | Status | File |
|---|---|---|
| LSN extraction from Debezium metadata | **Implemented** | `spark-app/jobs/main.py:normalize_*()` |
| LSN ordering in `_latest_by_key` window | **Implemented** | `delta_manager.py:_latest_by_key()` |
| LSN ordering guard in MERGE (current-state) | **Planned** | `spark-app/jobs/compact.py` |
| Watermark upsert with LSN guard | **Implemented** | `delta_manager.py:_upsert_watermark()` |

---

## Bottleneck 8: No Operational Visibility at Scale

### Current Implementation

The pipeline has a `pipeline_watermark` table in Postgres that tracks per-partition progress, but there is no:
- CDC lag monitoring (Postgres WAL vs Debezium)
- Kafka consumer lag (topic end-offset vs processed offset)
- Reconciliation (source entity count vs current-state count)

At billion-row scale, **you cannot detect problems without these signals**.

### Proposed Solution: Delta Ops Layer + Three Drift Signals

All health data stored in `data/delta/ops/` (Delta tables, no Postgres dependency):

| Signal | What it measures | Detection |
|---|---|---|
| **CDC lag** | `pg_current_wal_lsn() - connector_lsn` | How far behind is Debezium? |
| **Kafka lag** | `topic_end_offset - processed_offset` | How far behind is Spark? |
| **Reconciliation** | `COUNT(source) - COUNT(current-state)` | Are entities in sync? |

### Implementation Status

| Piece | Status | File |
|---|---|---|
| `pipeline_watermark` in Postgres | **Implemented** | `delta_manager.py:_upsert_watermark()` |
| CDC slot progress poller | **Planned** | `spark-app/jobs/health/cdc_progress.py` |
| Kafka high-water mark poller | **Planned** | `spark-app/jobs/health/high_water.py` |
| Reconciliation checker | **Planned** | `spark-app/jobs/health/reconcile.py` |
| Delta ops tables (all health data) | **Planned** | `data/delta/ops/*` |
| `--drift` query flag | **Planned** | `scripts/query_metrics.sh` |

---

## Summary: Current vs Planned

| Requirement | Current Implementation | Billion-Row Adaptation | Status |
|---|---|---|---|
| **Read efficiency** | Full silver scan (all history) | Read from compacted current-state (1 row/entity) | **Planned** |
| **Write efficiency** | Overwrite all 8 Postgres tables every batch | Incremental mart updates + decoupled Postgres write cadence | **Planned** |
| **Partition pruning** | No partitioning on silver | `partitionBy("event_date")` on silver | **Planned** |
| **Recovery time** | Unbounded (replay from earliest Kafka) | Hourly snapshots → ≤1 hour replay window | **Planned** |
| **Late-arrival safety** | `_latest_by_key` full-history window | LSN guard in current-state MERGE | **Planned** |
| **Duplicate handling** | `dedupe_batch_by_event()` | Same + idempotent MERGE on LSN | **Implemented** |
| **Concurrency** | Advisory lock blocks all but 1 stream | Partition-isolated ops + non-critical path isolation | **Planned** |
| **Monitoring** | `pipeline_watermark` in Postgres | 3 drift signals in Delta ops layer | **Planned** |
| **Bootstrap** | JDBC read + append to silver | Same (one-time) | **Implemented** |
| **CDC metadata** | LSN, ts_ms, partition, offset extracted | Same | **Implemented** |
| **Streaming semantics** | `startingOffsets=earliest`, 10s trigger | Same | **Implemented** |

---

## Work in Progress — `feature/spark_streaming_process` Branch

The [`feature/spark_streaming_process`](https://github.com/GuilhermeMatsumoto/deel-data-engineering-task/tree/feature/spark_streaming_process) branch contains ongoing work that already addresses several of the optimizations and edge cases described in this document:

| Optimization / Edge Case | Status in Branch |
|---|---|
| Current-state Delta tables (compacted, one row per entity) | Implemented |
| Silver → current-state MERGE with LSN ordering guard | Implemented |
| Marts computed from current-state instead of full silver scan | Implemented |
| Partition pruning — event_date field in normalized CDC data | Implemented |
| Partition pruning — partitionBy("event_date") on silver writes | Implemented |
| Non-critical path isolation (try/except around Postgres write) | Implemented |
| Empty current-state guard (skip Postgres write if 0 rows) | Implemented |
| CDC lag monitoring (Debezium LSN vs Postgres WAL) | Implemented |
| Kafka consumer lag monitoring (topic offset vs processed offset) | Implemented |
| Reconciliation checks (source count vs current-state count) | Implemented |
| Delta ops layer (all health data in Delta, no Postgres dependency) | Implemented |
| Recovery Mode A (catch-up replay from checkpoint) | Implemented |
| Recovery Mode B (snapshot + LSN-bounded replay) | Implemented |
| Recovery Mode C (full partition reset) | Implemented |
| Slot health pre-check before recovery | Implemented |
| Recovery audit logging | Implemented |
| Hourly SHALLOW CLONE snapshots | Implemented |
| Snapshot metadata registry with LSN range tracking | Implemented |
| Automated snapshot cleanup (retention policy) | Implemented |
| --drift flag for unified health report | Implemented |

The branch is not yet merged to `main` because it is still undergoing testing, particularly around:
- Cross-stream concurrency at high throughput
- Edge cases in snapshot recovery when WAL has been recycled
- Performance benchmarks comparing main (full-scan) vs current-state (incremental) at scale

## Files Needed

### New Files (13 planned)

| File | Purpose |
|---|---|
| `spark-app/jobs/compact.py` | Streaming job: silver CDF → current-state MERGE with LSN ordering guard |
| `spark-app/jobs/snapshot.py` | Hourly `SHALLOW CLONE` of current-state tables |
| `spark-app/jobs/health/__init__.py` | Package init |
| `spark-app/jobs/health/reconcile.py` | Aggregate count checks via JDBC |
| `spark-app/jobs/health/cdc_progress.py` | Debezium LSN vs Postgres WAL LSN poller |
| `spark-app/jobs/health/high_water.py` | Kafka end-offset poller per (topic, partition) |
| `spark-app/common/recovery.py` | Recovery plan builder, slot checker, audit writer |
| `spark-app/sql/health_views.sql` | Reusable SQL views for ops tables |
| `scripts/recover_stream.sh` | Recovery CLI (dry-run by default, modes A/B/C) |
| `scripts/run_health.sh` | Health cycle runner |
| `scripts/run_snapshots.sh` | Snapshot runner |
| `LATE_ARRIVAL_RUNBOOK.md` | Execution runbook with expected output |

### Files to Modify (9)

| File | Change |
|---|---|
| `spark-app/jobs/main.py` | Add CDF + `partitionBy("event_date")` + `event_date` field in normalization |
| `spark-app/common/delta_manager.py` | Read from current-state; CDF-based refresh; concurrency guards |
| `spark-app/common/config.py` | Add ops paths, current-state paths |
| `scripts/run_pipeline.sh` | Add compact job launcher |
| `scripts/query_metrics.sh` | Add `--drift` flag with MAX() grouping |
| `docker-compose.yaml` | Add health-watcher + snapshot-runner services |
| `db-scripts/initialize_analytics_ddl.sql` | Remove retired Postgres ops tables (moved to Delta) |
| `ERRORS_AND_FIXES.md` | Document migration strategy |
| `README.md` | Update architecture + recovery section |