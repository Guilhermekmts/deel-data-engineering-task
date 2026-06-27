import os
import streamlit as st
import psycopg2
import pandas as pd
from psycopg2 import OperationalError


def query(sql: str) -> pd.DataFrame:
    """Run a query on a short-lived connection (closed after use)."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "analytics-db"),
            port=int(os.getenv("DB_PORT", "5432")),
            dbname=os.getenv("DB_NAME", "analytics_db"),
            user=os.getenv("DB_USER", "analytics_user"),
            password=os.getenv("DB_PASSWORD", "analytics_1234"),
            connect_timeout=5,
        )
        return pd.read_sql(sql, conn)
    except OperationalError:
        st.warning("Database unavailable — analytics may still be initializing.")
        return pd.DataFrame()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_all_tables() -> list[str]:
    df = query("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'analytics'
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """)
    return df["table_name"].tolist()
