from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

# Ensure project modules are importable inside notebook/spark-submit context
CURRENT_DIR = Path(__file__).resolve().parent
APP_ROOT = CURRENT_DIR.parent
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from common.delta_manager import (
    ensure_silver_tables,
    process_stream_batch,
)
from common.config import Settings
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, date_add, get_json_object, lit, to_timestamp, when
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _json(path: str):
    """Helper to extract a JSON field from the raw CDC value using its path."""
    return get_json_object(col("json_value"), path)


def _coalesce_before_after(before_path: str, after_path: str):
    """Return before value if op='d', otherwise after value."""
    return when(_json("$.op") == "d", _json(before_path)).otherwise(_json(after_path))


def normalize_customers(df: DataFrame) -> DataFrame:
    return (
        df.select(
            _json("$.op").alias("op"),
            _coalesce_before_after("$.before.customer_id", "$.after.customer_id").cast("long").alias("customer_id"),
            _coalesce_before_after("$.before.customer_name", "$.after.customer_name").alias("customer_name"),
            _coalesce_before_after("$.before.is_active", "$.after.is_active").cast("boolean").alias("is_active"),
            _coalesce_before_after("$.before.customer_address", "$.after.customer_address").alias("customer_address"),
            to_timestamp((_coalesce_before_after("$.before.updated_at", "$.after.updated_at").cast("long") / 1000)).alias("updated_at"),
            to_timestamp((_coalesce_before_after("$.before.created_at", "$.after.created_at").cast("long") / 1000)).alias("created_at"),
            to_timestamp((_json("$.ts_ms") / 1000).cast("double")).alias("event_ts"),
            _json("$.source.ts_ms").cast("long").alias("source_ts_ms"),
            _json("$.source.lsn").cast("long").alias("source_lsn"),
            col("kafka_partition").cast("int").alias("kafka_partition"),
            col("kafka_offset").cast("long").alias("kafka_offset"),
            col("kafka_topic"),
        )
        .fillna(-1, subset=["source_lsn", "source_ts_ms"])
        .dropna(subset=["customer_id", "op"])
        .where(col("op").isin("c", "u", "d", "r"))
    )


def normalize_products(df: DataFrame) -> DataFrame:
    return (
        df.select(
            _json("$.op").alias("op"),
            _coalesce_before_after("$.before.product_id", "$.after.product_id").cast("long").alias("product_id"),
            _coalesce_before_after("$.before.product_name", "$.after.product_name").alias("product_name"),
            _coalesce_before_after("$.before.barcode", "$.after.barcode").alias("barcode"),
            _coalesce_before_after("$.before.unity_price", "$.after.unity_price").cast("decimal(18,2)").alias("unity_price"),
            _coalesce_before_after("$.before.is_active", "$.after.is_active").cast("boolean").alias("is_active"),
            to_timestamp((_coalesce_before_after("$.before.updated_at", "$.after.updated_at").cast("long") / 1000)).alias("updated_at"),
            to_timestamp((_coalesce_before_after("$.before.created_at", "$.after.created_at").cast("long") / 1000)).alias("created_at"),
            to_timestamp((_json("$.ts_ms") / 1000).cast("double")).alias("event_ts"),
            _json("$.source.ts_ms").cast("long").alias("source_ts_ms"),
            _json("$.source.lsn").cast("long").alias("source_lsn"),
            col("kafka_partition").cast("int").alias("kafka_partition"),
            col("kafka_offset").cast("long").alias("kafka_offset"),
            col("kafka_topic"),
        )
        .fillna(-1, subset=["source_lsn", "source_ts_ms"])
        .dropna(subset=["product_id", "op"])
        .where(col("op").isin("c", "u", "d", "r"))
    )


def normalize_orders(df: DataFrame) -> DataFrame:
    return (
        df.select(
            _json("$.op").alias("op"),
            _coalesce_before_after("$.before.order_id", "$.after.order_id").cast("long").alias("order_id"),
            _coalesce_before_after("$.before.customer_id", "$.after.customer_id").cast("long").alias("customer_id"),
            date_add(
                lit("1970-01-01"),
                _coalesce_before_after("$.before.order_date", "$.after.order_date").cast("int"),
            ).alias("order_date"),
            date_add(
                lit("1970-01-01"),
                _coalesce_before_after("$.before.delivery_date", "$.after.delivery_date").cast("int"),
            ).alias("delivery_date"),
            _coalesce_before_after("$.before.status", "$.after.status").alias("status"),
            to_timestamp((_coalesce_before_after("$.before.updated_at", "$.after.updated_at").cast("long") / 1000)).alias("updated_at"),
            to_timestamp((_coalesce_before_after("$.before.created_at", "$.after.created_at").cast("long") / 1000)).alias("created_at"),
            to_timestamp((_json("$.ts_ms") / 1000).cast("double")).alias("event_ts"),
            _json("$.source.ts_ms").cast("long").alias("source_ts_ms"),
            _json("$.source.lsn").cast("long").alias("source_lsn"),
            col("kafka_partition").cast("int").alias("kafka_partition"),
            col("kafka_offset").cast("long").alias("kafka_offset"),
            col("kafka_topic"),
        )
        .fillna(-1, subset=["source_lsn", "source_ts_ms"])
        .dropna(subset=["order_id", "op"])
        .where(col("op").isin("c", "u", "d", "r"))
    )


def normalize_order_items(df: DataFrame) -> DataFrame:
    return (
        df.select(
            _json("$.op").alias("op"),
            _coalesce_before_after("$.before.order_item_id", "$.after.order_item_id").cast("long").alias("order_item_id"),
            _coalesce_before_after("$.before.order_id", "$.after.order_id").cast("long").alias("order_id"),
            _coalesce_before_after("$.before.product_id", "$.after.product_id").cast("long").alias("product_id"),
            _coalesce_before_after("$.before.quanity", "$.after.quanity").cast("int").alias("quantity"),
            to_timestamp((_coalesce_before_after("$.before.updated_at", "$.after.updated_at").cast("long") / 1000)).alias("updated_at"),
            to_timestamp((_coalesce_before_after("$.before.created_at", "$.after.created_at").cast("long") / 1000)).alias("created_at"),
            to_timestamp((_json("$.ts_ms") / 1000).cast("double")).alias("event_ts"),
            _json("$.source.ts_ms").cast("long").alias("source_ts_ms"),
            _json("$.source.lsn").cast("long").alias("source_lsn"),
            col("kafka_partition").cast("int").alias("kafka_partition"),
            col("kafka_offset").cast("long").alias("kafka_offset"),
            col("kafka_topic"),
        )
        .fillna(-1, subset=["source_lsn", "source_ts_ms"])
        .dropna(subset=["order_item_id", "op"])
        .where(col("op").isin("c", "u", "d", "r"))
    )


LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def _foreach_handler(stream_name: str, key_col: str, delta_path: str):
    def _write(batch_df: DataFrame, batch_id: int):
        process_stream_batch(batch_df, batch_id, stream_name, key_col, delta_path)

    return _write


def topic_stream(spark: SparkSession, topic: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", Settings.kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(
            col("value").cast("string").alias("json_value"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
        )
    )


def bootstrap_with_spark(spark: SparkSession) -> None:
    """Initial load from source DB into Delta silver tables using Spark JDBC."""
    source_props = {
        "user": Settings.source_db_user,
        "password": Settings.source_db_password,
        "driver": "org.postgresql.Driver",
    }

    customers = (
        spark.read.jdbc(
            Settings.source_jdbc_url(), "operations.customers", properties=source_props
        )
        .withColumn("op", lit("r"))
        .withColumn("event_ts", lit(None).cast(TimestampType()))
        .withColumn("source_ts_ms", lit(None).cast(LongType()))
        .withColumn("source_lsn", lit(None).cast(LongType()))
        .withColumn("kafka_topic", lit(None).cast(StringType()))
        .withColumn("kafka_partition", lit(None).cast(IntegerType()))
        .withColumn("kafka_offset", lit(None).cast(LongType()))
    )
    from common.delta_manager import SILVER_CUSTOMER_SCHEMA
    customers = customers.select([f.name for f in SILVER_CUSTOMER_SCHEMA.fields])

    products = (
        spark.read.jdbc(
            Settings.source_jdbc_url(), "operations.products", properties=source_props
        )
        .withColumn("unity_price", col("unity_price").cast("decimal(18,2)"))
        .withColumn("op", lit("r"))
        .withColumn("event_ts", lit(None).cast(TimestampType()))
        .withColumn("source_ts_ms", lit(None).cast(LongType()))
        .withColumn("source_lsn", lit(None).cast(LongType()))
        .withColumn("kafka_topic", lit(None).cast(StringType()))
        .withColumn("kafka_partition", lit(None).cast(IntegerType()))
        .withColumn("kafka_offset", lit(None).cast(LongType()))
    )
    from common.delta_manager import SILVER_PRODUCT_SCHEMA
    products = products.select([f.name for f in SILVER_PRODUCT_SCHEMA.fields])

    orders = (
        spark.read.jdbc(
            Settings.source_jdbc_url(), "operations.orders", properties=source_props
        )
        .withColumn("op", lit("r"))
        .withColumn("event_ts", lit(None).cast(TimestampType()))
        .withColumn("source_ts_ms", lit(None).cast(LongType()))
        .withColumn("source_lsn", lit(None).cast(LongType()))
        .withColumn("kafka_topic", lit(None).cast(StringType()))
        .withColumn("kafka_partition", lit(None).cast(IntegerType()))
        .withColumn("kafka_offset", lit(None).cast(LongType()))
    )
    from common.delta_manager import SILVER_ORDER_SCHEMA
    orders = orders.select([f.name for f in SILVER_ORDER_SCHEMA.fields])

    order_items = (
        spark.read.jdbc(
            Settings.source_jdbc_url(), "operations.order_items", properties=source_props
        )
        .withColumnRenamed("quanity", "quantity")
        .withColumn("op", lit("r"))
        .withColumn("event_ts", lit(None).cast(TimestampType()))
        .withColumn("source_ts_ms", lit(None).cast(LongType()))
        .withColumn("source_lsn", lit(None).cast(LongType()))
        .withColumn("kafka_topic", lit(None).cast(StringType()))
        .withColumn("kafka_partition", lit(None).cast(IntegerType()))
        .withColumn("kafka_offset", lit(None).cast(LongType()))
    )
    from common.delta_manager import SILVER_ORDER_ITEM_SCHEMA
    order_items = order_items.select([f.name for f in SILVER_ORDER_ITEM_SCHEMA.fields])

    def merge_or_init(df: DataFrame, key_col: str, path: str) -> None:
        df.write.format("delta").mode("append").save(path)

    merge_or_init(customers, "customer_id", Settings.silver_customers_path())
    merge_or_init(products, "product_id", Settings.silver_products_path())
    merge_or_init(orders, "order_id", Settings.silver_orders_path())
    merge_or_init(order_items, "order_item_id", Settings.silver_order_items_path())


def run() -> None:
    os.makedirs(Settings.checkpoint_root, exist_ok=True)
    os.makedirs(Settings.delta_root, exist_ok=True)

    spark = (
        SparkSession.builder.appName("deel-spark-cdc-pipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    class LoggingListener(StreamingQueryListener):
        def onQueryStarted(self, event):
            LOGGER.info("[%s] query started", event.name)
        def onQueryProgress(self, event):
            pass
        def onQueryTerminated(self, event):
            if event.exception:
                LOGGER.error("[%s] query FAILED: %s", event.name, event.exception)
            else:
                LOGGER.info("[%s] query stopped gracefully", event.name)
    spark.streams.addListener(LoggingListener())

    ensure_silver_tables(spark)

    LOGGER.info("Bootstrapping source data into Delta silver tables if needed...")
    bootstrap_with_spark(spark)
    LOGGER.info("Bootstrap complete.")

    customers_raw = topic_stream(
        spark, "finance_db.operations.customers"
    )
    products_raw = topic_stream(
        spark, "finance_db.operations.products"
    )
    orders_raw = topic_stream(
        spark, "finance_db.operations.orders"
    )
    order_items_raw = topic_stream(
        spark, "finance_db.operations.order_items"
    )

    customer_query = (
        normalize_customers(customers_raw)
        .writeStream.foreachBatch(_foreach_handler("operations.customers", "customer_id", Settings.silver_customers_path()))
        .option("checkpointLocation", f"{Settings.checkpoint_root}/customers")
        .trigger(processingTime="10 seconds")
        .start()
    )

    product_query = (
        normalize_products(products_raw)
        .writeStream.foreachBatch(_foreach_handler("operations.products", "product_id", Settings.silver_products_path()))
        .option("checkpointLocation", f"{Settings.checkpoint_root}/products")
        .trigger(processingTime="10 seconds")
        .start()
    )

    orders_query = (
        normalize_orders(orders_raw)
        .writeStream.foreachBatch(_foreach_handler("operations.orders", "order_id", Settings.silver_orders_path()))
        .option("checkpointLocation", f"{Settings.checkpoint_root}/orders")
        .trigger(processingTime="10 seconds")
        .start()
    )

    items_query = (
        normalize_order_items(order_items_raw)
        .writeStream.foreachBatch(_foreach_handler("operations.order_items", "order_item_id", Settings.silver_order_items_path()))
        .option("checkpointLocation", f"{Settings.checkpoint_root}/order_items")
        .trigger(processingTime="10 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()

    for q, name in [
        (customer_query, "customers"),
        (product_query, "products"),
        (orders_query, "orders"),
        (items_query, "order_items"),
    ]:
        exception = q.exception()
        if exception:
            LOGGER.error("[%s] stream terminated with exception: %s", name, exception)
        else:
            LOGGER.info("[%s] stream stopped (no exception)", name)

    customer_query.stop()
    product_query.stop()
    orders_query.stop()
    items_query.stop()


if __name__ == "__main__":
    run()
