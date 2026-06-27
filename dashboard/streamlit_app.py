import streamlit as st

st.set_page_config(page_title="Deel Analytics Dashboard", layout="wide")

from refresh import auto_refresh_sidebar

auto_refresh_sidebar()

st.title("Deel Analytics Dashboard")
st.markdown("Browse analytics metrics and inspect raw table data.")

col1, col2 = st.columns(2)
with col1:
    st.metric("Pages available", "3")
    st.markdown("- **Metrics** — view curated mart tables")
    st.markdown("- **Data Samples** — peek into any analytics table")
    st.markdown("- **Delta Tables** — browse Silver Delta Lake tables")
with col2:
    st.metric("Schema", "analytics")
    st.markdown(f"Connect to **analytics-db** on port **5432**")
