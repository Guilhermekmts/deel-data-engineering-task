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
from kafka import KafkaConsumer, TopicPartition
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

HIGH_WATER_SCHEMA = StructType(
    [
        StructField("kafka_topic", StringType(), False),
        StructField("kafka_partition", IntegerType(), False),
        StructField("topic_offset", LongType()),
        StructField("scanned_at", TimestampType()),
    ]
)

TOPICS = [
    "finance_db.operations.customers",
    "finance_db.operations.products",
    "finance_db.operations.orders",
    "finance_db.operations.order_items",
]


def run() -> None:
    spark = (
        SparkSession.builder.appName("deel-kafka-high-water")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    ops_path = f"{Settings.ops_root()}/kafka_topic_high_water"
    if not _delta_exists(spark, ops_path):
        spark.createDataFrame([], HIGH_WATER_SCHEMA).write.format("delta").mode("overwrite").save(ops_path)

    consumer = KafkaConsumer(
        bootstrap_servers=Settings.kafka_bootstrap_servers,
        consumer_timeout_ms=5000,
        enable_auto_commit=False,
    )

    rows = []
    scanned_at = datetime.utcnow()

    for topic in TOPICS:
        try:
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                LOGGER.warning("No partitions found for topic %s", topic)
                continue
            for p in partitions:
                tp = TopicPartition(topic, p)
                consumer.assign([tp])
                consumer.seek_to_end(tp)
                end_offset = consumer.position(tp)
                rows.append((topic, p, end_offset, scanned_at))
                LOGGER.debug("Topic %s partition %s: high_water=%s", topic, p, end_offset)
        except Exception as e:
            LOGGER.error("Failed to get high-water for %s: %s", topic, e)

    consumer.close()

    import pandas as pd
    pdf = pd.DataFrame(rows, columns=["kafka_topic", "kafka_partition", "topic_offset", "scanned_at"])
    df = spark.createDataFrame(pdf, schema=HIGH_WATER_SCHEMA)
    df.write.format("delta").mode("append").save(ops_path)
    LOGGER.info("Kafka high-water written to %s", ops_path)


if __name__ == "__main__":
    run()