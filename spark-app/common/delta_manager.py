from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any

import psycopg2
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, max, row_number, struct, to_date
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

from common.config import Settings

LOGGER = logging.getLogger(__name__)


PROCESSED_OFFSETS_SCHEMA = StructType(
    [
        StructField("stream_name", StringType(), False),
        StructField("kafka_partition", IntegerType(), False),
        StructField("kafka_topic", StringType()),
        StructField("kafka_offset", LongType()),
        StructField("source_ts_ms", LongType()),
        StructField("source_lsn", LongType()),
        StructField("event_ts", TimestampType()),
        StructField("updated_at", TimestampType()),
    ]
)


def jdbc_options() -> dict[str, str]:
    return {
        "url": Settings.target_jdbc_url(),
        "user": Settings.target_db_user,
        "password": Settings.target_db_password,
        "driver": "org.postgresql.Driver",
        "batchsize": str(Settings.target_jdbc_batchsize),
        "isolationLevel": "READ_COMMITTED",
    }


def write_final_table(df: DataFrame, table: str) -> None:
    """Overwrite a final Postgres table preserving DDL (TRUNCATE + INSERT)."""
    (
        df.write.format("jdbc")
        .options(**jdbc_options())
        .option("dbtable", f"analytics.{table}")
        .mode("overwrite")
        .option("truncate", "true")
        .save()
    )


@contextmanager
def db_connection(host: str, port: str, dbname: str, user: str, password: str):
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    try:
        yield conn
    finally:
        conn.close()


def dedupe_batch_by_event(df: DataFrame) -> DataFrame:
    """Remove duplicate events within a micro-batch using (kafka_partition, kafka_offset) as unique event ID."""
    if df.rdd.isEmpty():
        return df
    return df.dropDuplicates(["kafka_partition", "kafka_offset"])


def append_to_silver(delta_path: str, df: DataFrame) -> None:
    """Append a micro-batch to a Delta silver table (partitioned by event_date via table metadata)."""
    df.write.format("delta").mode("append").option("mergeSchema", "true").save(delta_path)


def _delta_exists(spark: SparkSession, path: str) -> bool:
    try:
        DeltaTable.forPath(spark, path)
        return True
    except Exception:
        return False


def read_silver(spark: SparkSession, delta_path: str) -> DataFrame:
    return spark.read.format("delta").load(delta_path)


def ensure_silver_tables(spark: SparkSession) -> None:
    """Create empty Delta tables with CDF enabled so streaming MERGE can attach to existing schema."""
    silver_schemas = [
        (Settings.silver_customers_path(), SILVER_CUSTOMER_SCHEMA),
        (Settings.silver_products_path(), SILVER_PRODUCT_SCHEMA),
        (Settings.silver_orders_path(), SILVER_ORDER_SCHEMA),
        (Settings.silver_order_items_path(), SILVER_ORDER_ITEM_SCHEMA),
    ]
    for path, schema in silver_schemas:
        exists = _delta_exists(spark, path)
        if not exists:
            LOGGER.info("Creating empty Delta table at %s", path)
            spark.createDataFrame([], schema).write.format("delta").mode("overwrite").partitionBy("event_date").save(path)
        if _delta_exists(spark, path):
            spark.sql(
                f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
            )


def ensure_current_state_tables(spark: SparkSession) -> None:
    """Create empty current-state Delta tables (one row per entity)."""
    current_schemas = [
        (Settings.current_customers_path(), CURRENT_CUSTOMER_SCHEMA),
        (Settings.current_products_path(), CURRENT_PRODUCT_SCHEMA),
        (Settings.current_orders_path(), CURRENT_ORDER_SCHEMA),
        (Settings.current_order_items_path(), CURRENT_ORDER_ITEM_SCHEMA),
    ]
    for path, schema in current_schemas:
        if not _delta_exists(spark, path):
            LOGGER.info("Creating empty current-state Delta table at %s", path)
            spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(path)


# Schemas for empty Delta bootstrap - keep type imports reachable
SILVER_CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", LongType(), False),
        StructField("customer_name", StringType()),
        StructField("is_active", BooleanType()),
        StructField("customer_address", StringType()),
        StructField("updated_at", TimestampType()),
        StructField("created_at", TimestampType()),
        StructField("op", StringType()),
        StructField("event_ts", TimestampType()),
        StructField("event_date", DateType()),
        StructField("source_ts_ms", LongType()),
        StructField("source_lsn", LongType()),
        StructField("kafka_topic", StringType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
    ]
)

SILVER_PRODUCT_SCHEMA = StructType(
    [
        StructField("product_id", LongType(), False),
        StructField("product_name", StringType()),
        StructField("barcode", StringType()),
        StructField("unity_price", DecimalType(18, 2)),
        StructField("is_active", BooleanType()),
        StructField("updated_at", TimestampType()),
        StructField("created_at", TimestampType()),
        StructField("op", StringType()),
        StructField("event_ts", TimestampType()),
        StructField("event_date", DateType()),
        StructField("source_ts_ms", LongType()),
        StructField("source_lsn", LongType()),
        StructField("kafka_topic", StringType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
    ]
)

SILVER_ORDER_SCHEMA = StructType(
    [
        StructField("order_id", LongType(), False),
        StructField("customer_id", LongType()),
        StructField("order_date", DateType()),
        StructField("delivery_date", DateType()),
        StructField("status", StringType()),
        StructField("updated_at", TimestampType()),
        StructField("created_at", TimestampType()),
        StructField("op", StringType()),
        StructField("event_ts", TimestampType()),
        StructField("event_date", DateType()),
        StructField("source_ts_ms", LongType()),
        StructField("source_lsn", LongType()),
        StructField("kafka_topic", StringType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
    ]
)

SILVER_ORDER_ITEM_SCHEMA = StructType(
    [
        StructField("order_item_id", LongType(), False),
        StructField("order_id", LongType()),
        StructField("product_id", LongType()),
        StructField("quantity", IntegerType()),
        StructField("updated_at", TimestampType()),
        StructField("created_at", TimestampType()),
        StructField("op", StringType()),
        StructField("event_ts", TimestampType()),
        StructField("event_date", DateType()),
        StructField("source_ts_ms", LongType()),
        StructField("source_lsn", LongType()),
        StructField("kafka_topic", StringType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
    ]
)


CURRENT_CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", LongType(), False),
        StructField("customer_name", StringType()),
        StructField("is_active", BooleanType()),
        StructField("customer_address", StringType()),
        StructField("updated_at", TimestampType()),
        StructField("created_at", TimestampType()),
        StructField("op", StringType()),
        StructField("source_ts_ms", LongType()),
        StructField("source_lsn", LongType()),
        StructField("kafka_offset", LongType()),
    ]
)

CURRENT_PRODUCT_SCHEMA = StructType(
    [
        StructField("product_id", LongType(), False),
        StructField("product_name", StringType()),
        StructField("barcode", StringType()),
        StructField("unity_price", DecimalType(18, 2)),
        StructField("is_active", BooleanType()),
        StructField("updated_at", TimestampType()),
        StructField("created_at", TimestampType()),
        StructField("op", StringType()),
        StructField("source_ts_ms", LongType()),
        StructField("source_lsn", LongType()),
        StructField("kafka_offset", LongType()),
    ]
)

CURRENT_ORDER_SCHEMA = StructType(
    [
        StructField("order_id", LongType(), False),
        StructField("customer_id", LongType()),
        StructField("order_date", DateType()),
        StructField("delivery_date", DateType()),
        StructField("status", StringType()),
        StructField("updated_at", TimestampType()),
        StructField("created_at", TimestampType()),
        StructField("op", StringType()),
        StructField("source_ts_ms", LongType()),
        StructField("source_lsn", LongType()),
        StructField("kafka_offset", LongType()),
    ]
)

CURRENT_ORDER_ITEM_SCHEMA = StructType(
    [
        StructField("order_item_id", LongType(), False),
        StructField("order_id", LongType()),
        StructField("product_id", LongType()),
        StructField("quantity", IntegerType()),
        StructField("updated_at", TimestampType()),
        StructField("created_at", TimestampType()),
        StructField("op", StringType()),
        StructField("source_ts_ms", LongType()),
        StructField("source_lsn", LongType()),
        StructField("kafka_offset", LongType()),
    ]
)


def compute_dimensions_and_facts(spark: SparkSession) -> dict[str, DataFrame]:
    """Read all current-state Delta tables (one row per entity) and compute the final fact/dim DataFrames."""
    customers = (
        read_silver(spark, Settings.current_customers_path())
        .where(col("op") != "d")
        .select(
            "customer_id", "customer_name", "is_active", "customer_address", "updated_at", "created_at"
        )
    )

    products = (
        read_silver(spark, Settings.current_products_path())
        .where(col("op") != "d")
        .select(
            "product_id", "product_name", "barcode", "unity_price", "is_active", "updated_at", "created_at"
        )
    )

    orders = (
        read_silver(spark, Settings.current_orders_path())
        .where(col("op") != "d")
        .select(
            "order_id",
            "customer_id",
            "order_date",
            "delivery_date",
            "status",
            "updated_at",
            "created_at",
        )
    )
    orders = orders.withColumn(
        "is_open",
        (col("status").isNotNull()) & (col("status").isNotNull()),
    )
    # Recompute is_open/is_pending explicitly using upper case.
    orders = orders.drop("is_open").withColumn(
        "is_open",
        (col("status").isNotNull()) & (col("status") != "COMPLETED"),
    ).withColumn(
        "is_pending",
        col("status").isin("PENDING", "PROCESSING", "REPROCESSING"),
    )

    order_items = (
        read_silver(spark, Settings.current_order_items_path())
        .where(col("op") != "d")
        .select(
            "order_item_id", "order_id", "product_id", "quantity", "updated_at", "created_at"
        )
    )

    return {
        "dim_customers": customers,
        "dim_products": products,
        "fact_orders_current": orders,
        "fact_order_items_current": order_items,
    }


def compute_marts(facts: dict[str, DataFrame]) -> dict[str, DataFrame]:
    orders = facts["fact_orders_current"]
    items = facts["fact_order_items_current"]

    open_orders = (
        orders.where(col("is_open") == True)
        .groupBy("delivery_date", "status")
        .count()
        .withColumnRenamed("count", "open_orders")
        .select("delivery_date", "status", "open_orders")
    )

    top3_delivery = (
        open_orders.groupBy("delivery_date")
        .sum("open_orders")
        .withColumnRenamed("sum(open_orders)", "open_orders")
        .orderBy(col("open_orders").desc(), col("delivery_date").asc())
        .limit(3)
    )
    top3_delivery = top3_delivery.coalesce(1).withColumn(
        "rank_position", row_number().over(Window.orderBy(col("open_orders").desc(), col("delivery_date").asc()))
    ).select("rank_position", "delivery_date", "open_orders")

    pending_order_ids = orders.where(col("is_pending") == True).select("order_id")
    pending_items = (
        items.join(pending_order_ids, "order_id")
        .groupBy("product_id")
        .sum("quantity")
        .withColumnRenamed("sum(quantity)", "pending_items")
        .select("product_id", "pending_items")
    )

    pending_orders_customers = (
        orders.where(col("is_pending") == True)
        .groupBy("customer_id")
        .count()
        .withColumnRenamed("count", "pending_orders")
    )
    top3_customers = (
        pending_orders_customers.orderBy(col("pending_orders").desc(), col("customer_id").asc())
        .limit(3)
    )
    top3_customers = top3_customers.coalesce(1).withColumn(
        "rank_position",
        row_number().over(Window.orderBy(col("pending_orders").desc(), col("customer_id").asc())),
    ).select("rank_position", "customer_id", "pending_orders")

    return {
        "mart_open_orders_by_delivery_status": open_orders,
        "mart_top3_delivery_dates_open_orders": top3_delivery,
        "mart_open_pending_items_by_product": pending_items,
        "mart_top3_customers_pending_orders": top3_customers,
    }


def _batch_is_empty(df: DataFrame) -> bool:
    return df.rdd.isEmpty()


def _ensure_ops_dir(spark: SparkSession) -> None:
    ops_path = Settings.ops_root()
    if not _delta_exists(spark, ops_path):
        spark.createDataFrame([], PROCESSED_OFFSETS_SCHEMA).write.format("delta").mode("overwrite").option("mergeSchema", "true").partitionBy("stream_name").save(ops_path)


def _update_processed_offsets(spark: SparkSession, batch_df: DataFrame, stream_name: str) -> None:
    """Append per-partition max offset to the processed_offsets Delta table (partitioned by stream_name, no cross-stream conflicts)."""
    if batch_df.rdd.isEmpty():
        return
    offsets = (
        batch_df.groupBy("kafka_partition")
        .agg(
            max(struct("source_lsn", "source_ts_ms", "kafka_offset", "kafka_topic", "event_ts")).alias(
                "latest"
            )
        )
        .select(
            col("kafka_partition"),
            col("latest.kafka_topic").alias("kafka_topic"),
            col("latest.kafka_offset").alias("kafka_offset"),
            col("latest.source_ts_ms").alias("source_ts_ms"),
            col("latest.source_lsn").alias("source_lsn"),
            col("latest.event_ts").alias("event_ts"),
        )
    )
    now = datetime.utcnow()
    offsets = offsets.withColumn("stream_name", lit(stream_name)).withColumn("updated_at", lit(now).cast(TimestampType()))

    ops_path = Settings.ops_root()
    offsets.write.format("delta").mode("append").save(ops_path)


def process_stream_batch(
    batch_df: DataFrame,
    batch_id: int,
    stream_name: str,
    key_col: str,
    delta_path: str,
) -> None:
    """Process one micro-batch: dedupe by event ID, append to Delta silver (partitioned by event_date), update processed_offsets, refresh Postgres final layer."""
    LOGGER.info("[%s:%s] received batch with %s row(s)", stream_name, batch_id, batch_df.count())
    if _batch_is_empty(batch_df):
        LOGGER.info("[%s:%s] empty batch; skipping", stream_name, batch_id)
        return

    spark = batch_df.sparkSession

    df = (
        batch_df.fillna(-1, subset=["source_lsn", "source_ts_ms", "kafka_offset"])
        .where(col("op").isin("c", "u", "d", "r"))
        .dropna(subset=[key_col, "op"])
    )
    if _batch_is_empty(df):
        return

    df = dedupe_batch_by_event(df)
    df = df.withColumn("event_date", to_date(col("event_ts")))

    append_to_silver(delta_path, df)
    LOGGER.info("[%s:%s] appended %s row(s) to Delta %s", stream_name, batch_id, df.count(), delta_path)

    try:
        _update_processed_offsets(spark, df, stream_name)
    except Exception as e:
        LOGGER.warning("[%s:%s] failed to update processed_offsets: %s", stream_name, batch_id, e)

    refresh_analytics_final_layer(spark, stream_name)


def refresh_analytics_final_layer(
    spark: SparkSession,
    stream_name: str,
) -> None:
    """Recompute and overwrite the Postgres final layer under an advisory lock."""
    facts = compute_dimensions_and_facts(spark)
    marts = compute_marts(facts)

    customers_count = facts["dim_customers"].count() if _delta_exists(spark, Settings.current_customers_path()) else 0
    if customers_count == 0:
        LOGGER.warning("[%s] current-state tables are empty (compact job may be catching up); skipping Postgres write", stream_name)
        return

    with db_connection(
        host=Settings.target_db_host,
        port=Settings.target_db_port,
        dbname=Settings.target_db_name,
        user=Settings.target_db_user,
        password=Settings.target_db_password,
    ) as conn:
        cur = conn.cursor()
        cur.execute("SELECT pg_advisory_xact_lock(424242)")

        # Publish final layer
        write_final_table(facts["dim_customers"], "dim_customers")
        write_final_table(facts["dim_products"], "dim_products")
        write_final_table(facts["fact_orders_current"], "fact_orders_current")
        write_final_table(facts["fact_order_items_current"], "fact_order_items_current")
        write_final_table(marts["mart_open_orders_by_delivery_status"], "mart_open_orders_by_delivery_status")
        write_final_table(marts["mart_top3_delivery_dates_open_orders"], "mart_top3_delivery_dates_open_orders")
        write_final_table(marts["mart_open_pending_items_by_product"], "mart_open_pending_items_by_product")
        write_final_table(marts["mart_top3_customers_pending_orders"], "mart_top3_customers_pending_orders")

        conn.commit()

    LOGGER.info("Final layer refreshed by %s", stream_name)
