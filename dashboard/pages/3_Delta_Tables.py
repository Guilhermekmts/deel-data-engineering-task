import os
import streamlit as st
from deltalake import DeltaTable
from refresh import auto_refresh_sidebar

auto_refresh_sidebar()

DELTA_ROOT = os.getenv("DELTA_ROOT", "/data/delta")
TABLE_NAMES = ["silver_customers", "silver_orders", "silver_order_items", "silver_products"]


@st.cache_resource
def load_delta(table_name: str):
    path = os.path.join(DELTA_ROOT, table_name)
    return DeltaTable(path)


st.title("Delta Tables")

if not os.path.isdir(DELTA_ROOT):
    st.warning(f"Delta root {DELTA_ROOT} not found — is the volume mounted?")
    st.info("Run with `docker compose up dashboard` — data is mounted from `data/delta/`")
    st.stop()

selected = st.selectbox("Select a Delta table", TABLE_NAMES)

if selected:
    dt = load_delta(selected)

    col1, col2, col3 = st.columns(3)
    col1.metric("Version", dt.version())
    col2.metric("Parquet files", len(list(dt.file_uris())))
    col3.metric("Rows", dt.to_pyarrow_table().num_rows)

    with st.expander("Schema details"):
        schema = dt.schema().to_arrow()
        st.dataframe(
            [{"column": f.name, "type": str(f.type)} for f in schema],
            use_container_width=True,
            hide_index=True,
        )

    limit = st.slider("Rows to preview", 5, 100, 20)
    pdf = dt.to_pyarrow_table().to_pandas().head(limit)
    st.dataframe(pdf, use_container_width=True)