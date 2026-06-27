import streamlit as st
from db import query
from refresh import auto_refresh_sidebar

auto_refresh_sidebar()

METRICS = {
    "Open Orders by Delivery Status": {
        "sql": "SELECT * FROM analytics.mart_open_orders_by_delivery_status ORDER BY delivery_date, status;",
        "desc": "Open order count grouped by delivery date and order status.",
    },
    "Top 3 Delivery Dates (Open Orders)": {
        "sql": "SELECT * FROM analytics.mart_top3_delivery_dates_open_orders ORDER BY rank_position;",
        "desc": "Top 3 delivery dates with the most open orders.",
    },
    "Pending Items by Product": {
        "sql": "SELECT * FROM analytics.mart_open_pending_items_by_product ORDER BY pending_items DESC, product_id;",
        "desc": "Pending (non-delivered) item count per product.",
    },
    "Top 3 Customers (Pending Orders)": {
        "sql": "SELECT * FROM analytics.mart_top3_customers_pending_orders ORDER BY rank_position;",
        "desc": "Top 3 customers with the most pending orders.",
    },
}

st.title("Metrics")

for label, cfg in METRICS.items():
    with st.expander(label, expanded=True):
        st.caption(cfg["desc"])
        df = query(cfg["sql"])
        st.dataframe(df, use_container_width=True, hide_index=True)
