import os


class Settings:
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

    source_db_host = os.getenv("SOURCE_DB_HOST", "transactions-db")
    source_db_port = os.getenv("SOURCE_DB_PORT", "5432")
    source_db_name = os.getenv("SOURCE_DB_NAME", "finance_db")
    source_db_user = os.getenv("SOURCE_DB_USER", "finance_db_user")
    source_db_password = os.getenv("SOURCE_DB_PASSWORD", "1234")

    target_db_host = os.getenv("TARGET_DB_HOST", "analytics-db")
    target_db_port = os.getenv("TARGET_DB_PORT", "5432")
    target_db_name = os.getenv("TARGET_DB_NAME", "analytics_db")
    target_db_user = os.getenv("TARGET_DB_USER", "analytics_user")
    target_db_password = os.getenv("TARGET_DB_PASSWORD", "analytics_1234")

    checkpoint_root = os.getenv("CHECKPOINT_ROOT", "/workspace/.spark-checkpoints")
    delta_root = os.getenv("DELTA_ROOT", "/workspace/data/delta")

    target_jdbc_batchsize = int(os.getenv("TARGET_JDBC_BATCHSIZE", "10000"))
    target_jdbc_num_partitions = int(os.getenv("TARGET_JDBC_NUM_PARTITIONS", "4"))

    @classmethod
    def silver_customers_path(cls) -> str:
        return f"{cls.delta_root}/silver_customers"

    @classmethod
    def silver_products_path(cls) -> str:
        return f"{cls.delta_root}/silver_products"

    @classmethod
    def silver_orders_path(cls) -> str:
        return f"{cls.delta_root}/silver_orders"

    @classmethod
    def silver_order_items_path(cls) -> str:
        return f"{cls.delta_root}/silver_order_items"

    @classmethod
    def source_jdbc_url(cls) -> str:
        return f"jdbc:postgresql://{cls.source_db_host}:{cls.source_db_port}/{cls.source_db_name}"

    @classmethod
    def target_jdbc_url(cls) -> str:
        return f"jdbc:postgresql://{cls.target_db_host}:{cls.target_db_port}/{cls.target_db_name}"

