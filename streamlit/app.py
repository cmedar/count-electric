"""
Count Electric — Streamlit Dashboard

Entry point. Reads from the Gold Delta layer via Databricks SQL connector
and renders multi-page navigation.
"""

import streamlit as st

st.set_page_config(
    page_title="Count Electric",
    page_icon="⚡",
    layout="wide",
)

st.title("Count Electric")
st.caption("Tracking global EV adoption trends — by country, manufacturer, and year.")

st.info("Dashboard views will be available once the Gold layer is populated (Phase 4).")
