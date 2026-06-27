from __future__ import annotations

import json
import logging
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
APP_ROOT = CURRENT_DIR.parent.parent
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from common.config import Settings
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

CDC_PROGRESS_SCHEMA = StructType(
    [
        StructField("slot_name", StringType(), False),
        StructField("source_db_lsn", LongType()),
        StructField("connector_lsn", LongType()),
        StructField("lag_bytes", LongType()),
        StructField("captured_at", TimestampType()),
    ]
)


def get_lsn_int(lsn_str: str) -> int | None:
    if not lsn_str or lsn_str == "null":
        return None
    parts = lsn_str.split("/")
    if len(parts) == 2:
        try:
            return (int(parts[0], 16) << 32) + int(parts[1], 16)
        except ValueError:
            return None
    return None


def run() -> None:
    spark = (
        SparkSession.builder.appName("deel-cdc-progress")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    ops_path = f"{Settings.ops_root()}/cdc_slot_progress"
    from common.delta_manager import _delta_exists
    if not _delta_exists(spark, ops_path):
        spark.createDataFrame([], CDC_PROGRESS_SCHEMA).write.format("delta").mode("overwrite").save(ops_path)

    target_db_host = Settings.target_db_host
    kafka_connect_url = f"http://{target_db_host}" if target_db_host != "analytics-db" else "http://kafka-connect"
    connector_url = f"{kafka_connect_url}:8083/connectors/finance-db-connector/status"

    timestamp = datetime.utcnow()
    rows = []

    try:
        with urllib.request.urlopen(connector_url, timeout=10) as response:
            data = json.loads(response.read().decode())
        tasks = data.get("tasks", [{}])
        connector_lsn = None
        for task in tasks:
            lsn = task.get("lsn", {}) if isinstance(task, dict) else None
            if isinstance(lsn, dict):
                lsn_val = lsn.get("last_committed_lsn")
                if lsn_val:
                    parsed = get_lsn_int(str(lsn_val))
                    if parsed:
                        connector_lsn = parsed

        source_db_lsn = None
        try:
            from common.delta_manager import db_connection
            with db_connection(
                host=Settings.source_db_host,
                port=Settings.source_db_port,
                dbname=Settings.source_db_name,
                user=Settings.source_db_user,
                password=Settings.source_db_password,
            ) as conn:
                cur = conn.cursor()
                cur.execute("SELECT pg_current_wal_lsn()")
                row = cur.fetchone()
                if row:
                    source_db_lsn = get_lsn_int(str(row[0]))
        except Exception as e:
            LOGGER.warning("Could not query source DB WAL LSN: %s", e)

        lag_bytes = None
        if source_db_lsn is not None and connector_lsn is not None:
            lag_bytes = max(0, source_db_lsn - connector_lsn)

        rows.append(("cdc_pgoutput", source_db_lsn, connector_lsn, lag_bytes, timestamp))
        LOGGER.info("CDC progress: source_lsn=%s, connector_lsn=%s, lag=%s bytes", source_db_lsn, connector_lsn, lag_bytes)

    except Exception as e:
        LOGGER.error("Failed to get connector status: %s", e)
        rows.append(("cdc_pgoutput", None, None, None, timestamp))

    import pandas as pd
    pdf = pd.DataFrame(rows, columns=["slot_name", "source_db_lsn", "connector_lsn", "lag_bytes", "captured_at"])
    df = spark.createDataFrame(pdf, schema=CDC_PROGRESS_SCHEMA)
    df.write.format("delta").mode("append").save(ops_path)
    LOGGER.info("CDC progress written to %s", ops_path)


if __name__ == "__main__":
    run()