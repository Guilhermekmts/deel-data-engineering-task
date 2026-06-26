CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.dim_customers (
    customer_id BIGINT PRIMARY KEY,
    customer_name VARCHAR(500),
    is_active BOOLEAN,
    customer_address VARCHAR(500),
    updated_at TIMESTAMP(3),
    created_at TIMESTAMP(3)
);

CREATE TABLE IF NOT EXISTS analytics.dim_products (
    product_id BIGINT PRIMARY KEY,
    product_name VARCHAR(500),
    barcode VARCHAR(26),
    unity_price DECIMAL,
    is_active BOOLEAN,
    updated_at TIMESTAMP(3),
    created_at TIMESTAMP(3)
);

CREATE TABLE IF NOT EXISTS analytics.fact_orders_current (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT,
    order_date DATE,
    delivery_date DATE,
    status VARCHAR(50),
    updated_at TIMESTAMP(3),
    created_at TIMESTAMP(3),
    is_open BOOLEAN,
    is_pending BOOLEAN
);

CREATE TABLE IF NOT EXISTS analytics.fact_order_items_current (
    order_item_id BIGINT PRIMARY KEY,
    order_id BIGINT,
    product_id BIGINT,
    quantity INTEGER,
    updated_at TIMESTAMP(3),
    created_at TIMESTAMP(3)
);

CREATE TABLE IF NOT EXISTS analytics.pipeline_watermark (
    stream_name VARCHAR(255) NOT NULL,
    kafka_topic VARCHAR(255),
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT,
    source_ts_ms BIGINT,
    source_lsn BIGINT,
    event_ts TIMESTAMP(3),
    updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (stream_name, kafka_partition)
);

CREATE TABLE IF NOT EXISTS analytics.reconciliation_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    check_name VARCHAR(255) NOT NULL,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    source_value NUMERIC,
    target_value NUMERIC,
    status VARCHAR(32) NOT NULL,
    detected_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP(3),
    details TEXT
);

CREATE TABLE IF NOT EXISTS analytics.mart_open_orders_by_delivery_status (
    delivery_date DATE NOT NULL,
    status VARCHAR(50) NOT NULL,
    open_orders BIGINT NOT NULL,
    updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (delivery_date, status)
);

CREATE TABLE IF NOT EXISTS analytics.mart_top3_delivery_dates_open_orders (
    rank_position INTEGER PRIMARY KEY,
    delivery_date DATE,
    open_orders BIGINT NOT NULL,
    updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.mart_open_pending_items_by_product (
    product_id BIGINT PRIMARY KEY,
    pending_items BIGINT NOT NULL,
    updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.mart_top3_customers_pending_orders (
    rank_position INTEGER PRIMARY KEY,
    customer_id BIGINT,
    pending_orders BIGINT NOT NULL,
    updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_orders_current_open ON analytics.fact_orders_current (is_open, is_pending);
CREATE INDEX IF NOT EXISTS idx_fact_orders_current_delivery_date ON analytics.fact_orders_current (delivery_date);
CREATE INDEX IF NOT EXISTS idx_fact_items_current_order ON analytics.fact_order_items_current (order_id);
CREATE INDEX IF NOT EXISTS idx_fact_items_current_product ON analytics.fact_order_items_current (product_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_watermark_updated_at ON analytics.pipeline_watermark (updated_at);
CREATE INDEX IF NOT EXISTS idx_reconciliation_audit_status_detected
    ON analytics.reconciliation_audit (status, detected_at);
