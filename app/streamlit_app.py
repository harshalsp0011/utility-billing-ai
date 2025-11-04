"""
streamlit_app.py
----------------
Main Streamlit entry point for Utility Billing AI.

Purpose:
--------
Provides a UI for Troy & Banks analysts to:
  • Upload bill and tariff files
  • Trigger the workflow
  • Monitor pipeline status
  • Download reports

Run:
-----
streamlit run app/streamlit_app.py
"""

# Ensure project root and src/ are on sys.path so imports like `app.*` and `src.*`
# work when Streamlit runs the script from the `app/` directory.
import sys
import os
from pathlib import Path

def _add_path_front(p: Path):
    p_str = str(p)
    if p_str and p.exists():
        if p_str in sys.path:
            sys.path.remove(p_str)
        sys.path.insert(0, p_str)

# Project root is one level up from this file (app/ -> project_root)
_THIS_FILE = Path(__file__).resolve()
project_root = _THIS_FILE.parent.parent
_add_path_front(project_root)
_add_path_front(project_root / "src")

# Allow overriding in environments (useful in Docker / Streamlit Cloud)
env_root = os.environ.get("UTIL_BILLING_PROJECT_ROOT")
if env_root:
    _add_path_front(Path(env_root).expanduser().resolve())
    _add_path_front(Path(env_root).expanduser().resolve() / "src")

# Import logger after we've ensured `project_root` and `src/` are on sys.path
try:
    from src.utils.logger import get_logger
    logger = get_logger(__name__)
except Exception:
    # Fallback: basic logging if package import still fails
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="Utility Billing AI",
    #page_icon="📊",
    layout="wide",
)

# Load logo robustly: resolve absolute path and read bytes to avoid
# Streamlit path-resolution issues when running from different CWDs.
try:
    image_rel = Path("app/assets/logo.jpeg")
    # If project_root is defined above, prefer its resolved path
    image_abs = (project_root / image_rel).resolve() if 'project_root' in globals() else image_rel.resolve()
    logger.info(f"cwd={Path.cwd()} | logo_resolved={image_abs}")
    if image_abs.exists():
        try:
            with image_abs.open("rb") as _f:
                img_bytes = _f.read()
            st.sidebar.image(img_bytes, width=True)
        except Exception as e:
            logger.warning(f"Failed to read logo image bytes: {e}")
            st.sidebar.write("Troy & Banks")
    else:
        logger.warning(f"Logo file not found at {image_abs}")
        st.sidebar.write("Troy & Banks")
except Exception as e:
    # If anything unexpected happens, avoid crashing the app
    try:
        logger.error(f"Error loading sidebar image: {e}")
    except Exception:
        pass
    st.sidebar.write("Troy & Banks")

st.sidebar.title("Troy & Banks – Utility Billing AI")

page = st.sidebar.radio(
    "Navigation",
    ["Upload Files", "Run Workflow", "Pipeline Monitor", "Reports"],
)

# -----------------------------------------------------
# 1. Upload Files
# -----------------------------------------------------
if "Upload" in page:
    from app.components.file_uploader import render_file_uploader
    render_file_uploader()

# -----------------------------------------------------
# 2. Run Workflow
# -----------------------------------------------------
elif "Run Workflow" in page:
    #from app.components.workflow_trigger import render_workflow_trigger
    #render_workflow_trigger()
    st.title("Run Workflow")
    st.write("Click below to start the full billing analysis pipeline.")

    from app.components.airflow_trigger import trigger_dag_run, monitor_dag_run

    if st.button(" Start Airflow Workflow"):
        logger.info("start of run_workflow")
        dag_run_id = trigger_dag_run()
        if dag_run_id:
            st.info(f"Triggered Airflow DAG Run: **{dag_run_id}**")
            monitor_dag_run(dag_run_id)
        else:
            st.warning("Could not trigger DAG. Try again later.")

# -----------------------------------------------------
# 3. Pipeline Monitor
# -----------------------------------------------------
elif "Pipeline Monitor" in page:
    st.title("Pipeline Monitor")
    st.write("Displays latest pipeline runs and processed data.")
    try:
        from src.database.db_utils import fetch_processed_data
        df = fetch_processed_data(limit=10)
        st.dataframe(df)
    except Exception as e:
        st.error(f"Database connection error: {e}")

# -----------------------------------------------------
# 4. Reports
# -----------------------------------------------------
elif "Reports" in page:
    st.title("Reports")
    st.write("Download generated reports.")
    output_dir = Path("data/output")
    if not output_dir.exists():
        st.warning("No reports found yet.")
    else:
        for file in output_dir.glob("*.xlsx"):
            st.download_button(
                label=f"Download {file.name}",
                data=file.read_bytes(),
                file_name=file.name,
            )

st.sidebar.markdown("---")
st.sidebar.caption("© 2025 Troy & Banks | Utility Billing AI Prototype")
