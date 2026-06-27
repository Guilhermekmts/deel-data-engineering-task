from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

import psycopg2

from common.config import Settings
from common.delta_manager import _delta_exists
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

LOGGER = logging.getLogger(__name__)

RECOVERY_AUDIT_SCHEMA = StructType(
    [
        StructField("recovery_id", LongType(), False),
        StructField("stream_name", StringType(), False),
        StructField("mode", StringType(), False),
        StructField("before_state", StringType()),
        StructField("after_state", StringType()),
        StructField("started_at", TimestampType()),
        StructField("completed_at", TimestampType()),
        StructField("actor", StringType()),
    ]
)


def _ensure_table(spark: SparkSession) -> None:
    path = f"{Settings.ops_root()}/recovery_audit"
    if not _delta_exists(spark, path):
        spark.createDataFrame([], RECOVERY_AUDIT_SCHEMA).write.format("delta").mode("overwrite").save(path)


def get_latest_snapshot(spark: SparkSession, stream_name: str) -> dict[str, Any] | None:
    path = f"{Settings.ops_root()}/snapshot_registry"
    if not _delta_exists(spark, path):
        return None
    snaps = (
        spark.read.format("delta").load(path)
        .where(f"stream_name = '{stream_name}'")
        .orderBy("captured_at", ascending=False)
        .limit(1)
        .collect()
    )
    if not snaps:
        return None
    r = snaps[0]
    return {
        "snapshot_path": r.snapshot_path,
        "snapshot_kafka_offset": r.snapshot_kafka_offset,
        "snapshot_lsn": r.snapshot_lsn,
        "min_source_lsn": r.min_source_lsn,
        "max_source_lsn": r.max_source_lsn,
        "captured_at": str(r.captured_at),
    }


def get_processed_offsets(spark: SparkSession, stream_name: str) -> list[dict[str, Any]]:
    path = f"{Settings.ops_root()}/processed_offsets"
    if not _delta_exists(spark, path):
        return []
    rows = (
        spark.read.format("delta").load(path)
        .where(f"stream_name = '{stream_name}'")
        .groupBy("kafka_partition")
        .agg({"kafka_offset": "max", "source_lsn": "max"})
        .collect()
    )
    return [
        {
            "kafka_partition": r.kafka_partition,
            "kafka_offset": r["max(kafka_offset)"],
            "source_lsn": r["max(source_lsn)"],
        }
        for r in rows
    ]


def check_replication_slot(slot_name: str = "cdc_pgoutput") -> dict[str, Any]:
    """Query Postgres for replication slot health.
    
    Returns dict with slot_name, active, restart_lsn, confirmed_flush_lsn.
    Raises RuntimeError if slot is unhealthy (missing, inactive, or restart_lsn is NULL).
    """
    try:
        conn = psycopg2.connect(
            host=Settings.source_db_host,
            port=Settings.source_db_port,
            dbname=Settings.source_db_name,
            user=Settings.source_db_user,
            password=Settings.source_db_password,
        )
        cur = conn.cursor()
        cur.execute(
            "SELECT slot_name, active, restart_lsn, confirmed_flush_lsn "
            "FROM pg_replication_slots WHERE slot_name = %s",
            (slot_name,),
        )
        row = cur.fetchone()
        conn.close()
    except Exception as e:
        raise RuntimeError(f"Failed to query replication slot '{slot_name}': {e}")

    if row is None:
        raise RuntimeError(
            f"Replication slot '{slot_name}' does not exist in Postgres. "
            "Cannot proceed with recovery. Create the slot or use a full bootstrap."
        )

    slot_name_db, active, restart_lsn, confirmed_flush_lsn = row

    if not active:
        raise RuntimeError(
            f"Replication slot '{slot_name}' is inactive (active=false). "
            "The slot exists but replication is not running. "
            "Restart the Debezium connector before retrying recovery."
        )

    if restart_lsn is None:
        raise RuntimeError(
            f"Replication slot '{slot_name}' has restart_lsn=NULL. "
            "This means the slot position is unknown. "
            "The WAL may have been recycled. Use a full bootstrap from Postgres."
        )

    return {
        "slot_name": slot_name_db,
        "active": active,
        "restart_lsn": restart_lsn,
        "confirmed_flush_lsn": confirmed_flush_lsn,
    }


def get_current_table_count(spark: SparkSession, stream_name: str) -> int | None:
    path_map = {
        "customers": Settings.current_customers_path,
        "products": Settings.current_products_path,
        "orders": Settings.current_orders_path,
        "order_items": Settings.current_order_items_path,
    }
    short = stream_name.split(".")[-1]
    if short not in path_map:
        return None
    path = path_map[short]()
    if not _delta_exists(spark, path):
        return None
    try:
        return spark.read.format("delta").load(path).where("op != 'd'").count()
    except Exception:
        return None


def plan_recovery(spark: SparkSession, stream_name: str, mode: str) -> dict[str, Any]:
    plan = {
        "stream_name": stream_name,
        "mode": mode,
        "actions": [],
        "estimated_replay_offset": None,
        "slot": None,
        "snapshot": None,
    }

    try:
        slot = check_replication_slot()
        plan["slot"] = slot
        plan["actions"].append(
            f"✓ Replication slot '{slot['slot_name']}' is active, "
            f"restart_lsn={slot['restart_lsn']}, "
            f"confirmed_flush_lsn={slot['confirmed_flush_lsn']}"
        )
    except RuntimeError as e:
        plan["actions"].append(f"✗ Replication slot check FAILED: {e}")
        plan["actions"].append("Aborting recovery. Fix the slot issue before retrying.")

    if mode == "A":
        offsets = get_processed_offsets(spark, stream_name)
        plan["actions"].append(f"Read {len(offsets)} partition(s) from processed_offsets for {stream_name}")
        plan["actions"].append("Restart streaming pipeline; compact job handles idempotent MERGE")
        plan["actions"].append("No destructive action needed; resume from last checkpoint")
        plan["estimated_replay_offset"] = max((o["kafka_offset"] for o in offsets), default=None)

    elif mode == "B":
        snapshot = get_latest_snapshot(spark, stream_name)
        plan["snapshot"] = snapshot
        if snapshot:
            plan["actions"].append(f"Restore from snapshot: {snapshot['snapshot_path']}")
            plan["actions"].append(f"Snapshot LSN range: min={snapshot['min_source_lsn']}, max={snapshot['max_source_lsn']}")
            plan["actions"].append(f"Replay Kafka from offset: {snapshot['snapshot_kafka_offset']}")
            plan["actions"].append("CLONE snapshot back to current-state table")
            plan["actions"].append("Delete checkpoint for compact job")
            plan["actions"].append("Restart both main pipeline and compact job")
            plan["estimated_replay_offset"] = snapshot["snapshot_kafka_offset"]

            if plan["slot"] and snapshot.get("max_source_lsn") is not None:
                slot_confirmed = plan["slot"].get("confirmed_flush_lsn")
                if slot_confirmed is not None and snapshot["max_source_lsn"] > slot_confirmed:
                    plan["actions"].append(
                        f"✗ LSN GAP: snapshot.max_source_lsn ({snapshot['max_source_lsn']}) > "
                        f"slot.confirmed_flush_lsn ({slot_confirmed}). "
                        "Snapshot state is ahead of Debezium's acknowledged position. "
                        "The slot may not have confirmed these events yet."
                    )
                elif slot_confirmed is not None:
                    gap = slot_confirmed - snapshot["max_source_lsn"]
                    plan["actions"].append(
                        f"✓ LSN gap OK: snapshot.max_source_lsn ({snapshot['max_source_lsn']}) <= "
                        f"slot.confirmed_flush_lsn ({slot_confirmed}), gap={gap}"
                    )
        else:
            plan["actions"].append(f"No snapshot found for {stream_name}")
            plan["actions"].append("Fall back to mode C or run a full bootstrap from source DB")

    elif mode == "C":
        plan["actions"].append(f"Wipe .spark-checkpoints/{stream_name}")
        plan["actions"].append(f"Wipe .spark-checkpoints/compact_{stream_name}")
        snapshot = get_latest_snapshot(spark, stream_name)
        plan["snapshot"] = snapshot
        if snapshot:
            plan["actions"].append(f"Restore from snapshot: {snapshot['snapshot_path']}")
            plan["actions"].append(f"Snapshot LSN range: min={snapshot['min_source_lsn']}, max={snapshot['max_source_lsn']}")
            plan["actions"].append(f"Set startingOffsets to earliest; replay from offset {snapshot['snapshot_kafka_offset']}")
        else:
            plan["actions"].append("No snapshot found; full re-bootstrap from source DB")

    else:
        plan["actions"].append(f"Unknown mode: {mode}. Use A (catch-up), B (snapshot+replay), or C (partition reset).")

    return plan


def write_recovery_audit(
    spark: SparkSession,
    stream_name: str,
    mode: str,
    before_state: dict,
    after_state: dict,
    actor: str,
    slot_info: dict[str, Any] | None = None,
    snapshot_info: dict[str, Any] | None = None,
    before_count: int | None = None,
    after_count: int | None = None,
) -> None:
    _ensure_table(spark)

    path = f"{Settings.ops_root()}/recovery_audit"
    now = datetime.utcnow()

    full_before = dict(before_state)
    full_after = dict(after_state)

    if slot_info:
        full_before["slot_name"] = slot_info.get("slot_name")
        full_before["slot_active"] = slot_info.get("active")
        full_before["slot_restart_lsn"] = slot_info.get("restart_lsn")
        full_before["slot_confirmed_lsn"] = slot_info.get("confirmed_flush_lsn")
        full_after["slot_name"] = slot_info.get("slot_name")
        full_after["slot_active"] = slot_info.get("active")
        full_after["slot_restart_lsn"] = slot_info.get("restart_lsn")
        full_after["slot_confirmed_lsn"] = slot_info.get("confirmed_flush_lsn")

    if snapshot_info:
        full_before["snapshot_path"] = snapshot_info.get("snapshot_path")
        full_before["snapshot_min_lsn"] = snapshot_info.get("min_source_lsn")
        full_before["snapshot_max_lsn"] = snapshot_info.get("max_source_lsn")
        full_before["snapshot_kafka_offset"] = snapshot_info.get("snapshot_kafka_offset")

    if before_count is not None:
        full_before["current_state_count"] = before_count
    if after_count is not None:
        full_after["current_state_count"] = after_count

    existing = spark.read.format("delta").load(path)
    max_id = existing.agg({"recovery_id": "max"}).collect()[0][0]
    next_id = (max_id or 0) + 1

    from pyspark.sql import Row
    row = Row(
        recovery_id=next_id,
        stream_name=stream_name,
        mode=mode,
        before_state=json.dumps(full_before, default=str),
        after_state=json.dumps(full_after, default=str),
        started_at=now,
        completed_at=now,
        actor=actor,
    )
    df = spark.createDataFrame([row], RECOVERY_AUDIT_SCHEMA)
    df.write.format("delta").mode("append").save(path)
    LOGGER.info("Recovery audit written for %s mode %s", stream_name, mode)