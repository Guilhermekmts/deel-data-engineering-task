from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Any

CURRENT_DIR = Path(__file__).resolve().parent
APP_ROOT = CURRENT_DIR.parent
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from common.config import Settings
from common.delta_manager import (
    CURRENT_CUSTOMER_SCHEMA,
    CURRENT_ORDER_ITEM_SCHEMA,
    CURRENT_ORDER_SCHEMA,
    CURRENT_PRODUCT_SCHEMA,
    ensure_current_state_tables,
)
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, struct
from pyspark.sql.types import LongType, StringType

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def _batch_is_empty(df: DataFrame) -> bool:
    return df.rdd.isEmpty()


def _merge_into_current(
    batch_df: DataFrame,
    batch_id: int,
    stream_name: str,
    key_col: str,
    current_path: str,
) -> None:
    LOGGER.info("[compact:%s:%s] received %s row(s)", stream_name, batch_id, batch_df.count())
    if _batch_is_empty(batch_df):
        return

    df = batch_df.select(
        col("customer_id").cast("long") if "customer_id" in batch_df.columns else lit(None).cast("long"),
    )

    df = batch_df.where(col("op").isin("c", "u", "d", "r"))

    if _batch_is_empty(df):
        return

    target = DeltaTable.forPath(batch_df.sparkSession, current_path)

    merge_key = f"target.{key_col} = source.{key_col}"
    merge_condition = (
        "COALESCE(source.source_lsn, -1) > COALESCE(target.source_lsn, -1)"
        " OR ("
        "  COALESCE(source.source_lsn, -1) = COALESCE(target.source_lsn, -1)"
        "  AND COALESCE(source.source_ts_ms, -1) > COALESCE(target.source_ts_ms, -1)"
        " )"
        " OR ("
        "  COALESCE(source.source_lsn, -1) = COALESCE(target.source_lsn, -1)"
        "  AND COALESCE(source.source_ts_ms, -1) = COALESCE(target.source_ts_ms, -1)"
        "  AND COALESCE(source.kafka_offset, -1) >= COALESCE(target.kafka_offset, -1)"
        " )"
    )

    match_condition = f"{merge_key} AND ({merge_condition})"

    target.alias("target").merge(
        df.alias("source"),
        match_condition,
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    LOGGER.info("[compact:%s:%s] merged %s row(s) into %s", stream_name, batch_id, df.count(), current_path)


def stream_entity(
    spark: SparkSession,
    silver_path: str,
    current_path: str,
    key_col: str,
    stream_name: str,
    checkpoint_root: str,
) -> Any:
    return (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", "0")
        .load(silver_path)
        .writeStream.foreachBatch(
            lambda df, bid: _merge_into_current(df, bid, stream_name, key_col, current_path)
        )
        .option("checkpointLocation", f"{checkpoint_root}/compact_{stream_name}")
        .trigger(processingTime="10 seconds")
        .start()
    )


def run() -> None:
    os.makedirs(Settings.checkpoint_root, exist_ok=True)

    spark = (
        SparkSession.builder.appName("deel-compact-current-state")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    ensure_current_state_tables(spark)

    entities = [
        ("operations.customers", Settings.silver_customers_path(), Settings.current_customers_path(), "customer_id"),
        ("operations.products", Settings.silver_products_path(), Settings.current_products_path(), "product_id"),
        ("operations.orders", Settings.silver_orders_path(), Settings.current_orders_path(), "order_id"),
        ("operations.order_items", Settings.silver_order_items_path(), Settings.current_order_items_path(), "order_item_id"),
    ]

    queries = [
        stream_entity(spark, silver, current, key, name, Settings.checkpoint_root)
        for name, silver, current, key in entities
    ]

    spark.streams.awaitAnyTermination()

    for q in queries:
        q.stop()


if __name__ == "__main__":
    run()