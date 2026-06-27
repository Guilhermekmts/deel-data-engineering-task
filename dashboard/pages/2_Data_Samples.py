import streamlit as st
from db import query, get_all_tables
from refresh import auto_refresh_sidebar

auto_refresh_sidebar()

st.title("Data Samples")

tables = get_all_tables()
selected = st.selectbox("Select a table", tables)

if selected:
    limit = st.slider("Number of rows", 5, 100, 20)
    full_name = f"analytics.{selected}"
    df = query(f"SELECT * FROM {full_name} LIMIT {limit};")
    st.dataframe(df, use_container_width=True, hide_index=True)

    with st.expander("Column info"):
        col_info = query(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'analytics' AND table_name = '{selected}'
            ORDER BY ordinal_position
        """)
        st.dataframe(col_info, use_container_width=True, hide_index=True)
