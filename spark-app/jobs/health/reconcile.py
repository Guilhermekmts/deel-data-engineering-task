from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
APP_ROOT = CURRENT_DIR.parent.parent
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from common.config import Settings
from common.delta_manager import _delta_exists
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import (
    LongType,
    NumericType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

RECONCILIATION_SCHEMA = StructType(
    [
        StructField("check_name", StringType(), False),
        StructField("window_start", TimestampType()),
        StructField("window_end", TimestampType()),
        StructField("source_value", NumericType()),
        StructField("target_value", NumericType()),
        StructField("status", StringType(), False),
        StructField("detected_at", TimestampType()),
        StructField("resolved_at", TimestampType()),
        StructField("details", StringType()),
    ]
)

ENTITY_CHECKS = [
    ("customers", Settings.current_customers_path(), "customer_id"),
    ("products", Settings.current_products_path(), "product_id"),
    ("orders", Settings.current_orders_path(), "order_id"),
    ("order_items", Settings.current_order_items_path(), "order_item_id"),
]


def _ensure_table(spark: SparkSession, path: str, schema: StructType) -> None:
    if not _delta_exists(spark, path):
        spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(path)


def reconcile_entity(spark: SparkSession, name: str, current_path: str, key_col: str) -> dict:
    now = datetime.utcnow()
    source_props = {
        "user": Settings.source_db_user,
        "password": Settings.source_db_password,
        "driver": "org.postgresql.Driver",
    }
    pg_table = f"operations.{name}"

    try:
        source_count = (
            spark.read.jdbc(Settings.source_jdbc_url(), pg_table, properties=source_props)
            .count()
        )
    except Exception as e:
        return {
            "check_name": f"count_{name}",
            "window_start": now,
            "window_end": now,
            "source_value": None,
            "target_value": None,
            "status": "FAILED",
            "detected_at": now,
            "resolved_at": None,
            "details": f"Source query failed: {e}",
        }

    try:
        target_count = (
            spark.read.format("delta").load(current_path)
            .where(col("op") != "d")
            .count()
        )
    except Exception as e:
        return {
            "check_name": f"count_{name}",
            "window_start": now,
            "window_end": now,
            "source_value": source_count,
            "target_value": None,
            "status": "FAILED",
            "detected_at": now,
            "resolved_at": None,
            "details": f"Target query failed: {e}",
        }

    diff = abs(source_count - target_count)
    if diff == 0:
        status = "OK"
        details = f"Counts match: source={source_count}, target={target_count}"
    else:
        status = "DRIFT"
        details = f"Counts differ by {diff}: source={source_count}, target={target_count}"

    return {
        "check_name": f"count_{name}",
        "window_start": now,
        "window_end": now,
        "source_value": source_count,
        "target_value": target_count,
        "status": status,
        "detected_at": now,
        "resolved_at": None,
        "details": details,
    }


def run() -> None:
    spark = (
        SparkSession.builder.appName("deel-reconciliation")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    audit_path = f"{Settings.ops_root()}/reconciliation_audit"
    _ensure_table(spark, audit_path, RECONCILIATION_SCHEMA)

    results = []
    for name, current_path, key_col in ENTITY_CHECKS:
        result = reconcile_entity(spark, name, current_path, key_col)
        results.append(result)
        LOGGER.info("Reconciliation %s: %s", result["check_name"], result["status"])

    import pandas as pd
    pdf = pd.DataFrame(results)
    df = spark.createDataFrame(pdf, schema=RECONCILIATION_SCHEMA)
    df.write.format("delta").mode("append").save(audit_path)

    LOGGER.info("Reconciliation complete")


if __name__ == "__main__":
    run()