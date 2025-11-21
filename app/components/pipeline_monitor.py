import streamlit as st
from src.database.db_utils import fetch_processed_data

def render_pipeline_monitor():
    st.title("Pipeline Monitor")

    df = fetch_processed_data(limit=20)

    if df.empty:
        st.info("No pipeline runs logged.")
    else:
        st.dataframe(df, use_container_width=True)
