"""Sidebar auto-refresh with short-lived Postgres connections.

Each refresh cycle opens a new connection, queries, and closes it immediately.
No persistent connection or transaction is held between cycles, so the
pipeline's TRUNCATE+INSERT under advisory lock is never blocked.
"""

import streamlit as st
from streamlit_autorefresh import st_autorefresh


def auto_refresh_sidebar():
    interval = st.sidebar.selectbox(
        "Auto-refresh", ["Off", "10s", "30s", "60s"], index=0
    )
    if interval != "Off":
        ms = int(interval.replace("s", "")) * 1000
        count = st_autorefresh(interval=ms, key="refresh")
        st.sidebar.caption(f"Refreshed {count + 1} time(s)")
        return True
    return False