from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Any

CURRENT_DIR = Path(__file__).resolve().parent
APP_ROOT = CURRENT_DIR.parent.parent
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from common.config import Settings
from common.delta_manager import _delta_exists
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, max, min
from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

SNAPSHOT_REGISTRY_SCHEMA = StructType(
    [
        StructField("stream_name", StringType(), False),
        StructField("snapshot_path", StringType(), False),
        StructField("snapshot_kafka_offset", LongType()),
        StructField("snapshot_lsn", LongType()),
        StructField("min_source_lsn", LongType()),
        StructField("max_source_lsn", LongType()),
        StructField("captured_at", TimestampType()),
    ]
)


CURRENT_STREAMS = [
    ("customers", Settings.current_customers_path()),
    ("products", Settings.current_products_path()),
    ("orders", Settings.current_orders_path()),
    ("order_items", Settings.current_order_items_path()),
]


def _ensure_snapshot_registry(spark: SparkSession) -> None:
    path = f"{Settings.ops_root()}/snapshot_registry"
    if not _delta_exists(spark, path):
        spark.createDataFrame([], SNAPSHOT_REGISTRY_SCHEMA).write.format("delta").mode("overwrite").save(path)


def take_snapshot(spark: SparkSession, stream_name: str, current_path: str) -> str:
    from datetime import datetime

    now = datetime.utcnow()
    ts = now.strftime("%Y%m%d_%H%M%S")
    snapshot_path = f"{Settings.delta_root}/snapshots/{stream_name}_{ts}"

    LOGGER.info("Taking snapshot of %s -> %s", current_path, snapshot_path)

    spark.sql(f"CREATE OR REPLACE TABLE delta.`{snapshot_path}` SHALLOW CLONE delta.`{current_path}`")

    current_df = spark.read.format("delta").load(current_path)

    max_offset = current_df.agg(max(col("kafka_offset"))).collect()[0][0]
    max_lsn = current_df.agg(max(col("source_lsn"))).collect()[0][0]
    min_lsn = current_df.agg(min(col("source_lsn"))).collect()[0][0]

    LOGGER.info(
        "Snapshot %s: max_kafka_offset=%s, min_source_lsn=%s, max_source_lsn=%s",
        snapshot_path, max_offset, min_lsn, max_lsn,
    )

    registry_df = spark.createDataFrame(
        [(stream_name, snapshot_path, max_offset, max_lsn, min_lsn, max_lsn, now)],
        SNAPSHOT_REGISTRY_SCHEMA,
    )
    reg_path = f"{Settings.ops_root()}/snapshot_registry"
    registry_df.write.format("delta").mode("append").save(reg_path)

    return snapshot_path


def run() -> None:
    os.makedirs(Settings.delta_root, exist_ok=True)

    spark = (
        SparkSession.builder.appName("deel-snapshot-hourly")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    _ensure_snapshot_registry(spark)

    for stream_name, current_path in CURRENT_STREAMS:
        if _delta_exists(spark, current_path):
            take_snapshot(spark, stream_name, current_path)
        else:
            LOGGER.warning("Current table for %s not found at %s, skipping", stream_name, current_path)

    LOGGER.info("Hourly snapshots complete")


if __name__ == "__main__":
    run()