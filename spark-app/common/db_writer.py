from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Iterable

import psycopg2
from psycopg2.extras import execute_batch


@contextmanager
def db_connection(host: str, port: str, dbname: str, user: str, password: str):
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )
    try:
        yield conn
    finally:
        conn.close()


def bootstrap_target_from_source(source_cfg: dict[str, str], target_cfg: dict[str, str]) -> None:
    with db_connection(**source_cfg) as src_conn, db_connection(**target_cfg) as tgt_conn:
        src_cur = src_conn.cursor()
        tgt_cur = tgt_conn.cursor()

        src_cur.execute(
            """
            SELECT customer_id, customer_name, is_active, customer_address, updated_at, created_at
            FROM operations.customers
            """
        )
        customer_rows = src_cur.fetchall()

        execute_batch(
            tgt_cur,
            """
            INSERT INTO analytics.dim_customers (
                customer_id, customer_name, is_active, customer_address, updated_at, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (customer_id)
            DO UPDATE SET
                customer_name = EXCLUDED.customer_name,
                is_active = EXCLUDED.is_active,
                customer_address = EXCLUDED.customer_address,
                updated_at = EXCLUDED.updated_at,
                created_at = EXCLUDED.created_at
            """,
            customer_rows,
        )

        src_cur.execute(
            """
            SELECT product_id, product_name, barcode, unity_price, is_active, updated_at, created_at
            FROM operations.products
            """
        )
        product_rows = src_cur.fetchall()

        execute_batch(
            tgt_cur,
            """
            INSERT INTO analytics.dim_products (
                product_id, product_name, barcode, unity_price, is_active, updated_at, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id)
            DO UPDATE SET
                product_name = EXCLUDED.product_name,
                barcode = EXCLUDED.barcode,
                unity_price = EXCLUDED.unity_price,
                is_active = EXCLUDED.is_active,
                updated_at = EXCLUDED.updated_at,
                created_at = EXCLUDED.created_at
            """,
            product_rows,
        )

        src_cur.execute(
            """
            SELECT order_id, customer_id, order_date, delivery_date, status, updated_at, created_at
            FROM operations.orders
            """
        )
        order_rows = src_cur.fetchall()
        order_rows = [
            (
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                row[5],
                row[6],
                _is_open_status(row[4]),
                _is_pending_status(row[4]),
            )
            for row in order_rows
        ]

        execute_batch(
            tgt_cur,
            """
            INSERT INTO analytics.fact_orders_current (
                order_id, customer_id, order_date, delivery_date, status, updated_at, created_at, is_open, is_pending
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id)
            DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                order_date = EXCLUDED.order_date,
                delivery_date = EXCLUDED.delivery_date,
                status = EXCLUDED.status,
                updated_at = EXCLUDED.updated_at,
                created_at = EXCLUDED.created_at,
                is_open = EXCLUDED.is_open,
                is_pending = EXCLUDED.is_pending
            """,
            order_rows,
        )

        src_cur.execute(
            """
            SELECT order_item_id, order_id, product_id, quanity, updated_at, created_at
            FROM operations.order_items
            """
        )
        item_rows = src_cur.fetchall()

        execute_batch(
            tgt_cur,
            """
            INSERT INTO analytics.fact_order_items_current (
                order_item_id, order_id, product_id, quantity, updated_at, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_item_id)
            DO UPDATE SET
                order_id = EXCLUDED.order_id,
                product_id = EXCLUDED.product_id,
                quantity = EXCLUDED.quantity,
                updated_at = EXCLUDED.updated_at,
                created_at = EXCLUDED.created_at
            """,
            item_rows,
        )

        refresh_marts_cursor(tgt_cur)
        tgt_conn.commit()


def upsert_customers(conn, rows: list[dict]) -> None:
    if not rows:
        return
    cur = conn.cursor()

    to_upsert = [
        (
            r.get("customer_id"),
            r.get("customer_name"),
            r.get("is_active"),
            r.get("customer_address"),
            r.get("updated_at"),
            r.get("created_at"),
        )
        for r in rows
        if r.get("op") != "d" and r.get("customer_id") is not None
    ]
    to_delete = [r.get("customer_id") for r in rows if r.get("op") == "d" and r.get("customer_id") is not None]

    if to_upsert:
        execute_batch(
            cur,
            """
            INSERT INTO analytics.dim_customers (
                customer_id, customer_name, is_active, customer_address, updated_at, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (customer_id)
            DO UPDATE SET
                customer_name = EXCLUDED.customer_name,
                is_active = EXCLUDED.is_active,
                customer_address = EXCLUDED.customer_address,
                updated_at = EXCLUDED.updated_at,
                created_at = EXCLUDED.created_at
            """,
            to_upsert,
        )

    if to_delete:
        cur.execute("DELETE FROM analytics.dim_customers WHERE customer_id = ANY(%s)", (to_delete,))

    conn.commit()


def upsert_products(conn, rows: list[dict]) -> None:
    if not rows:
        return
    cur = conn.cursor()

    to_upsert = [
        (
            r.get("product_id"),
            r.get("product_name"),
            r.get("barcode"),
            r.get("unity_price"),
            r.get("is_active"),
            r.get("updated_at"),
            r.get("created_at"),
        )
        for r in rows
        if r.get("op") != "d" and r.get("product_id") is not None
    ]
    to_delete = [r.get("product_id") for r in rows if r.get("op") == "d" and r.get("product_id") is not None]

    if to_upsert:
        execute_batch(
            cur,
            """
            INSERT INTO analytics.dim_products (
                product_id, product_name, barcode, unity_price, is_active, updated_at, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id)
            DO UPDATE SET
                product_name = EXCLUDED.product_name,
                barcode = EXCLUDED.barcode,
                unity_price = EXCLUDED.unity_price,
                is_active = EXCLUDED.is_active,
                updated_at = EXCLUDED.updated_at,
                created_at = EXCLUDED.created_at
            """,
            to_upsert,
        )

    if to_delete:
        cur.execute("DELETE FROM analytics.dim_products WHERE product_id = ANY(%s)", (to_delete,))

    conn.commit()


def upsert_orders(conn, rows: list[dict]) -> None:
    if not rows:
        return
    cur = conn.cursor()

    upsert_rows = []
    delete_ids = []
    history_rows = []

    for r in rows:
        order_id = r.get("order_id")
        if order_id is None:
            continue

        status = r.get("status")
        event_ts = r.get("event_ts")
        op = r.get("op")
        if op is None:
            continue

        history_rows.append(
            (
                order_id,
                r.get("customer_id"),
                r.get("order_date"),
                r.get("delivery_date"),
                status,
                op,
                event_ts,
            )
        )

        if op == "d":
            delete_ids.append(order_id)
            continue

        upsert_rows.append(
            (
                order_id,
                r.get("customer_id"),
                r.get("order_date"),
                r.get("delivery_date"),
                status,
                r.get("updated_at"),
                r.get("created_at"),
                _is_open_status(status),
                _is_pending_status(status),
            )
        )

    if upsert_rows:
        execute_batch(
            cur,
            """
            INSERT INTO analytics.fact_orders_current (
                order_id, customer_id, order_date, delivery_date, status, updated_at, created_at, is_open, is_pending
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id)
            DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                order_date = EXCLUDED.order_date,
                delivery_date = EXCLUDED.delivery_date,
                status = EXCLUDED.status,
                updated_at = EXCLUDED.updated_at,
                created_at = EXCLUDED.created_at,
                is_open = EXCLUDED.is_open,
                is_pending = EXCLUDED.is_pending
            """,
            upsert_rows,
        )

    if delete_ids:
        cur.execute("DELETE FROM analytics.fact_orders_current WHERE order_id = ANY(%s)", (delete_ids,))

    if history_rows:
        execute_batch(
            cur,
            """
            INSERT INTO analytics.fact_orders_history (
                order_id, customer_id, order_date, delivery_date, status, op, event_ts
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            history_rows,
        )

    refresh_marts_cursor(cur)
    conn.commit()


def upsert_order_items(conn, rows: list[dict]) -> None:
    if not rows:
        return
    cur = conn.cursor()

    upsert_rows = []
    delete_ids = []
    history_rows = []

    for r in rows:
        order_item_id = r.get("order_item_id")
        if order_item_id is None:
            continue

        op = r.get("op")
        if op is None:
            continue

        history_rows.append(
            (
                order_item_id,
                r.get("order_id"),
                r.get("product_id"),
                r.get("quantity"),
                op,
                r.get("event_ts"),
            )
        )

        if op == "d":
            delete_ids.append(order_item_id)
            continue

        upsert_rows.append(
            (
                order_item_id,
                r.get("order_id"),
                r.get("product_id"),
                r.get("quantity"),
                r.get("updated_at"),
                r.get("created_at"),
            )
        )

    if upsert_rows:
        execute_batch(
            cur,
            """
            INSERT INTO analytics.fact_order_items_current (
                order_item_id, order_id, product_id, quantity, updated_at, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_item_id)
            DO UPDATE SET
                order_id = EXCLUDED.order_id,
                product_id = EXCLUDED.product_id,
                quantity = EXCLUDED.quantity,
                updated_at = EXCLUDED.updated_at,
                created_at = EXCLUDED.created_at
            """,
            upsert_rows,
        )

    if delete_ids:
        cur.execute("DELETE FROM analytics.fact_order_items_current WHERE order_item_id = ANY(%s)", (delete_ids,))

    if history_rows:
        execute_batch(
            cur,
            """
            INSERT INTO analytics.fact_order_items_history (
                order_item_id, order_id, product_id, quantity, op, event_ts
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            history_rows,
        )

    refresh_marts_cursor(cur)
    conn.commit()


def refresh_marts(conn) -> None:
    cur = conn.cursor()
    refresh_marts_cursor(cur)
    conn.commit()


def refresh_marts_cursor(cur) -> None:
    refreshed_at = datetime.utcnow()

    cur.execute("SELECT pg_advisory_xact_lock(424242)")

    cur.execute(
        """
        DELETE FROM analytics.mart_open_orders_by_delivery_status m
        WHERE NOT EXISTS (
            SELECT 1
            FROM analytics.fact_orders_current f
            WHERE f.is_open = TRUE
              AND f.delivery_date = m.delivery_date
              AND f.status = m.status
        )
        """
    )
    cur.execute(
        """
        INSERT INTO analytics.mart_open_orders_by_delivery_status (
            delivery_date, status, open_orders, updated_at
        )
        SELECT
            delivery_date,
            status,
            COUNT(*) AS open_orders,
            %s
        FROM analytics.fact_orders_current
        WHERE is_open = TRUE
        GROUP BY delivery_date, status
        ON CONFLICT (delivery_date, status)
        DO UPDATE SET
            open_orders = EXCLUDED.open_orders,
            updated_at = EXCLUDED.updated_at
        """,
        (refreshed_at,),
    )

    cur.execute("DELETE FROM analytics.mart_top3_delivery_dates_open_orders")
    cur.execute(
        """
        INSERT INTO analytics.mart_top3_delivery_dates_open_orders (
            rank_position, delivery_date, open_orders, updated_at
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY open_orders DESC, delivery_date ASC) AS rank_position,
            delivery_date,
            open_orders,
            %s
        FROM (
            SELECT delivery_date, COUNT(*) AS open_orders
            FROM analytics.fact_orders_current
            WHERE is_open = TRUE
            GROUP BY delivery_date
        ) q
        ORDER BY open_orders DESC, delivery_date ASC
        LIMIT 3
        ON CONFLICT (rank_position)
        DO UPDATE SET
            delivery_date = EXCLUDED.delivery_date,
            open_orders = EXCLUDED.open_orders,
            updated_at = EXCLUDED.updated_at
        """,
        (refreshed_at,),
    )

    cur.execute(
        """
        DELETE FROM analytics.mart_open_pending_items_by_product m
        WHERE NOT EXISTS (
            SELECT 1
            FROM analytics.fact_order_items_current oi
            INNER JOIN analytics.fact_orders_current o
                ON o.order_id = oi.order_id
            WHERE o.is_pending = TRUE
              AND oi.product_id = m.product_id
        )
        """
    )
    cur.execute(
        """
        INSERT INTO analytics.mart_open_pending_items_by_product (
            product_id, pending_items, updated_at
        )
        SELECT
            oi.product_id,
            COALESCE(SUM(oi.quantity), 0)::BIGINT AS pending_items,
            %s
        FROM analytics.fact_order_items_current oi
        INNER JOIN analytics.fact_orders_current o
            ON o.order_id = oi.order_id
        WHERE o.is_pending = TRUE
        GROUP BY oi.product_id
        ON CONFLICT (product_id)
        DO UPDATE SET
            pending_items = EXCLUDED.pending_items,
            updated_at = EXCLUDED.updated_at
        """,
        (refreshed_at,),
    )

    cur.execute("DELETE FROM analytics.mart_top3_customers_pending_orders")
    cur.execute(
        """
        INSERT INTO analytics.mart_top3_customers_pending_orders (
            rank_position, customer_id, pending_orders, updated_at
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY pending_orders DESC, customer_id ASC) AS rank_position,
            customer_id,
            pending_orders,
            %s
        FROM (
            SELECT customer_id, COUNT(*) AS pending_orders
            FROM analytics.fact_orders_current
            WHERE is_pending = TRUE
            GROUP BY customer_id
        ) q
        ORDER BY pending_orders DESC, customer_id ASC
        LIMIT 3
        ON CONFLICT (rank_position)
        DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            pending_orders = EXCLUDED.pending_orders,
            updated_at = EXCLUDED.updated_at
        """,
        (refreshed_at,),
    )


def _is_open_status(status: str | None) -> bool:
    if status is None:
        return False
    return status.upper() != "COMPLETED"


def _is_pending_status(status: str | None) -> bool:
    if status is None:
        return False
    return status.upper() in {"PENDING", "PROCESSING", "REPROCESSING"}
