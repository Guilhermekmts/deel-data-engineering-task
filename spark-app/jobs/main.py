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


def run() -> None:
    os.makedirs(Settings.checkpoint_root, exist_ok=True)

    source_cfg = {
        "host": Settings.source_db_host,
        "port": Settings.source_db_port,
        "dbname": Settings.source_db_name,
        "user": Settings.source_db_user,
        "password": Settings.source_db_password,
    }
    target_cfg = {
        "host": Settings.target_db_host,
        "port": Settings.target_db_port,
        "dbname": Settings.target_db_name,
        "user": Settings.target_db_user,
        "password": Settings.target_db_password,
    }

    bootstrap_target_from_source(source_cfg, target_cfg)

    spark = (
        SparkSession.builder.appName("deel-spark-cdc-pipeline")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    def topic_stream(topic: str, schema: StructType) -> DataFrame:
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", Settings.kafka_bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
            .select(from_json(col("value").cast("string"), schema).alias("payload"))
        )

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
