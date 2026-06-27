from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, date_add, from_json, lit, to_date, to_timestamp, when
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)

CURRENT_DIR = Path(__file__).resolve().parent
APP_ROOT = CURRENT_DIR.parent
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from common.delta_manager import (
    _ensure_ops_dir,
    ensure_current_state_tables,
    ensure_silver_tables,
    process_stream_batch,
)
from common.config import Settings
from common.db_writer import (
    bootstrap_target_from_source,
    db_connection,
    upsert_customers,
    upsert_order_items,
    upsert_orders,
    upsert_products,
)


CUSTOMER_RECORD_SCHEMA = StructType(
    [
        StructField("customer_id", LongType()),
        StructField("customer_name", StringType()),
        StructField("is_active", BooleanType()),
        StructField("customer_address", StringType()),
        StructField("updated_at", StringType()),
        StructField("created_at", StringType()),
    ]
)

PRODUCT_RECORD_SCHEMA = StructType(
    [
        StructField("product_id", LongType()),
        StructField("product_name", StringType()),
        StructField("barcode", StringType()),
        StructField("unity_price", DecimalType(18, 2)),
        StructField("is_active", BooleanType()),
        StructField("updated_at", StringType()),
        StructField("created_at", StringType()),
    ]
)

ORDER_RECORD_SCHEMA = StructType(
    [
        StructField("order_id", LongType()),
        StructField("order_date", LongType()),
        StructField("delivery_date", LongType()),
        StructField("customer_id", LongType()),
        StructField("status", StringType()),
        StructField("updated_at", StringType()),
        StructField("created_at", StringType()),
    ]
)

ORDER_ITEM_RECORD_SCHEMA = StructType(
    [
        StructField("order_item_id", LongType()),
        StructField("order_id", LongType()),
        StructField("product_id", LongType()),
        StructField("quanity", LongType()),
        StructField("updated_at", StringType()),
        StructField("created_at", StringType()),
    ]
)


def debezium_schema(record_schema: StructType) -> StructType:
    return StructType(
        [
            StructField("before", record_schema),
            StructField("after", record_schema),
            StructField("op", StringType()),
            StructField("ts_ms", LongType()),
        ]
    )


def normalize_customers(df: DataFrame) -> DataFrame:
    return (
        df.select(
            col("payload.op").alias("op"),
            when(col("payload.op") == "d", col("payload.before.customer_id"))
            .otherwise(col("payload.after.customer_id"))
            .alias("customer_id"),
            when(col("payload.op") == "d", col("payload.before.customer_name"))
            .otherwise(col("payload.after.customer_name"))
            .alias("customer_name"),
            when(col("payload.op") == "d", col("payload.before.is_active"))
            .otherwise(col("payload.after.is_active"))
            .alias("is_active"),
            when(col("payload.op") == "d", col("payload.before.customer_address"))
            .otherwise(col("payload.after.customer_address"))
            .alias("customer_address"),
            to_timestamp(
                when(col("payload.op") == "d", col("payload.before.updated_at")).otherwise(
                    col("payload.after.updated_at")
                )
            ).alias("updated_at"),
            to_timestamp(
                when(col("payload.op") == "d", col("payload.before.created_at")).otherwise(
                    col("payload.after.created_at")
                )
            ).alias("created_at"),
            to_timestamp((col("payload.ts_ms") / 1000).cast("double")).alias("event_ts"),
        )
        .dropna(subset=["customer_id", "op"])
        .where(col("op").isin("c", "u", "d", "r"))
    )


def normalize_products(df: DataFrame) -> DataFrame:
    return (
        df.select(
            col("payload.op").alias("op"),
            when(col("payload.op") == "d", col("payload.before.product_id"))
            .otherwise(col("payload.after.product_id"))
            .alias("product_id"),
            when(col("payload.op") == "d", col("payload.before.product_name"))
            .otherwise(col("payload.after.product_name"))
            .alias("product_name"),
            when(col("payload.op") == "d", col("payload.before.barcode"))
            .otherwise(col("payload.after.barcode"))
            .alias("barcode"),
            when(col("payload.op") == "d", col("payload.before.unity_price"))
            .otherwise(col("payload.after.unity_price"))
            .alias("unity_price"),
            when(col("payload.op") == "d", col("payload.before.is_active"))
            .otherwise(col("payload.after.is_active"))
            .alias("is_active"),
            to_timestamp(
                when(col("payload.op") == "d", col("payload.before.updated_at")).otherwise(
                    col("payload.after.updated_at")
                )
            ).alias("updated_at"),
            to_timestamp(
                when(col("payload.op") == "d", col("payload.before.created_at")).otherwise(
                    col("payload.after.created_at")
                )
            ).alias("created_at"),
            to_timestamp((col("payload.ts_ms") / 1000).cast("double")).alias("event_ts"),
        )
        .dropna(subset=["product_id", "op"])
        .where(col("op").isin("c", "u", "d", "r"))
    )


def normalize_orders(df: DataFrame) -> DataFrame:
    raw_order_date = when(col("payload.op") == "d", col("payload.before.order_date")).otherwise(
        col("payload.after.order_date")
    )
    raw_delivery_date = when(col("payload.op") == "d", col("payload.before.delivery_date")).otherwise(
        col("payload.after.delivery_date")
    )

    return (
        df.select(
            col("payload.op").alias("op"),
            when(col("payload.op") == "d", col("payload.before.order_id"))
            .otherwise(col("payload.after.order_id"))
            .alias("order_id"),
            when(col("payload.op") == "d", col("payload.before.customer_id"))
            .otherwise(col("payload.after.customer_id"))
            .alias("customer_id"),
            date_add(lit("1970-01-01"), raw_order_date.cast("int")).alias("order_date"),
            date_add(lit("1970-01-01"), raw_delivery_date.cast("int")).alias("delivery_date"),
            when(col("payload.op") == "d", col("payload.before.status"))
            .otherwise(col("payload.after.status"))
            .alias("status"),
            to_timestamp(
                when(col("payload.op") == "d", col("payload.before.updated_at")).otherwise(
                    col("payload.after.updated_at")
                )
            ).alias("updated_at"),
            to_timestamp(
                when(col("payload.op") == "d", col("payload.before.created_at")).otherwise(
                    col("payload.after.created_at")
                )
            ).alias("created_at"),
            to_timestamp((col("payload.ts_ms") / 1000).cast("double")).alias("event_ts"),
        )
        .dropna(subset=["order_id", "op"])
        .where(col("op").isin("c", "u", "d", "r"))
    )


def normalize_order_items(df: DataFrame) -> DataFrame:
    return (
        df.select(
            col("payload.op").alias("op"),
            when(col("payload.op") == "d", col("payload.before.order_item_id"))
            .otherwise(col("payload.after.order_item_id"))
            .alias("order_item_id"),
            when(col("payload.op") == "d", col("payload.before.order_id"))
            .otherwise(col("payload.after.order_id"))
            .alias("order_id"),
            when(col("payload.op") == "d", col("payload.before.product_id"))
            .otherwise(col("payload.after.product_id"))
            .alias("product_id"),
            when(col("payload.op") == "d", col("payload.before.quanity"))
            .otherwise(col("payload.after.quanity"))
            .alias("quantity"),
            to_timestamp(
                when(col("payload.op") == "d", col("payload.before.updated_at")).otherwise(
                    col("payload.after.updated_at")
                )
            ).alias("updated_at"),
            to_timestamp(
                when(col("payload.op") == "d", col("payload.before.created_at")).otherwise(
                    col("payload.after.created_at")
                )
            ).alias("created_at"),
            to_timestamp((col("payload.ts_ms") / 1000).cast("double")).alias("event_ts"),
        )
        .dropna(subset=["order_item_id", "op"])
        .where(col("op").isin("c", "u", "d", "r"))
    )


def foreach_writer(writer_fn):
    def _write(batch_df: DataFrame, _: int):
        rows = [json.loads(row) for row in batch_df.toJSON().collect()]
        if not rows:
            return

        with db_connection(
            host=Settings.target_db_host,
            port=Settings.target_db_port,
            dbname=Settings.target_db_name,
            user=Settings.target_db_user,
            password=Settings.target_db_password,
        ) as conn:
            writer_fn(conn, rows)

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
        .withColumn("event_date", lit(None).cast(DateType()))
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
        .withColumn("event_date", lit(None).cast(DateType()))
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
        .withColumn("event_date", lit(None).cast(DateType()))
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
        .withColumn("event_date", lit(None).cast(DateType()))
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
    os.makedirs(Settings.ops_root(), exist_ok=True)

    spark = (
        SparkSession.builder.appName("deel-spark-cdc-pipeline")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    ensure_silver_tables(spark)
    ensure_current_state_tables(spark)
    _ensure_ops_dir(spark)

    LOGGER.info("Bootstrapping source data into Delta silver tables if needed...")
    bootstrap_with_spark(spark)
    LOGGER.info("Bootstrap complete.")

    customers_raw = topic_stream(
        "finance_db.operations.customers", debezium_schema(CUSTOMER_RECORD_SCHEMA)
    )
    products_raw = topic_stream(
        "finance_db.operations.products", debezium_schema(PRODUCT_RECORD_SCHEMA)
    )
    orders_raw = topic_stream("finance_db.operations.orders", debezium_schema(ORDER_RECORD_SCHEMA))
    order_items_raw = topic_stream(
        "finance_db.operations.order_items", debezium_schema(ORDER_ITEM_RECORD_SCHEMA)
    )

    customer_query = (
        normalize_customers(customers_raw)
        .writeStream.foreachBatch(foreach_writer(upsert_customers))
        .option("checkpointLocation", f"{Settings.checkpoint_root}/customers")
        .trigger(processingTime="10 seconds")
        .start()
    )

    product_query = (
        normalize_products(products_raw)
        .writeStream.foreachBatch(foreach_writer(upsert_products))
        .option("checkpointLocation", f"{Settings.checkpoint_root}/products")
        .trigger(processingTime="10 seconds")
        .start()
    )

    orders_query = (
        normalize_orders(orders_raw)
        .writeStream.foreachBatch(foreach_writer(upsert_orders))
        .option("checkpointLocation", f"{Settings.checkpoint_root}/orders")
        .trigger(processingTime="10 seconds")
        .start()
    )

    items_query = (
        normalize_order_items(order_items_raw)
        .writeStream.foreachBatch(foreach_writer(upsert_order_items))
        .option("checkpointLocation", f"{Settings.checkpoint_root}/order_items")
        .trigger(processingTime="10 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()

    customer_query.stop()
    product_query.stop()
    orders_query.stop()
    items_query.stop()


if __name__ == "__main__":
    run()
