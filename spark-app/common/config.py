import os


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


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

    @classmethod
    def source_jdbc_url(cls) -> str:
        return f"jdbc:postgresql://{cls.source_db_host}:{cls.source_db_port}/{cls.source_db_name}"

    @classmethod
    def target_jdbc_url(cls) -> str:
        return f"jdbc:postgresql://{cls.target_db_host}:{cls.target_db_port}/{cls.target_db_name}"

    @classmethod
    def validate(cls) -> None:
        _required("KAFKA_BOOTSTRAP_SERVERS")
