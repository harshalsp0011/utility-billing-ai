import streamlit as st
from pathlib import Path

def render_reports_viewer():
    st.title("Reports")

    output_dir = Path("data/output")

    if not output_dir.exists():
        st.warning("No reports available.")
        return

    for file in output_dir.glob("*.xlsx"):
        st.download_button(
            label=f"⬇️ Download {file.name}",
            data=file.read_bytes(),
            file_name=file.name,
        )
