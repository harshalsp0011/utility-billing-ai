"""
streamlit_app.py
----------------
Main Streamlit entry point for Utility Billing AI.
"""

import sys
import os
from pathlib import Path

def _add_path_front(p: Path):
    p_str = str(p)
    if p_str and p.exists():
        if p_str in sys.path:
            sys.path.remove(p_str)
        sys.path.insert(0, p_str)

_THIS_FILE = Path(__file__).resolve()
project_root = _THIS_FILE.parent.parent

_add_path_front(project_root)
_add_path_front(project_root / "src")

env_root = os.environ.get("UTIL_BILLING_PROJECT_ROOT")
if env_root:
    _add_path_front(Path(env_root).expanduser().resolve())
    _add_path_front(Path(env_root).expanduser().resolve() / "src")

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv(str(project_root / ".env"))
except:
    pass

# Load Logger
try:
    from src.utils.logger import get_logger
    logger = get_logger(__name__)
except:
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

import streamlit as st

# -----------------------------------------------------
# PAGE SETTINGS
# -----------------------------------------------------
st.set_page_config(
    page_title="Utility Billing AI",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# -----------------------------------------------------
# CUSTOM CSS - LOAD FROM EXTERNAL FILE
# -----------------------------------------------------
css_path = project_root / "app/assets/sidebar_styles.css"
if css_path.exists():
    with open(css_path, 'r') as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
else:
    logger.warning(f"CSS file not found: {css_path}")

# -----------------------------------------------------
# SIDEBAR LOGO
# -----------------------------------------------------
try:
    logo_path = (project_root / "app/assets/logo.jpeg").resolve()
    logger.info(f"Logo path resolved: {logo_path}")

    if logo_path.exists():
        st.sidebar.image(str(logo_path), width=140)
    else:
        st.sidebar.write("Troy & Banks")

except Exception as e:
    logger.error(f"Error loading logo: {e}")
    st.sidebar.write("Troy & Banks")

st.sidebar.title("Troy & Banks – Utility Billing AI")

# -----------------------------------------------------
# NAVIGATION WITH ICONS
# -----------------------------------------------------
# Icon mapping for each page
page_icons = {
    "Upload Files": "📁",
    "User Bills": "📄",
    "Tariff Details": "📑",
    "Run Workflow": "▶️",
    "Pipeline Monitor": "📊",
    "Reports": "📋"
}

# Add custom HTML for tooltip support
st.sidebar.markdown("""
<script>
document.addEventListener('DOMContentLoaded', function() {
    const labels = {
        '📁': 'Upload Files',
        '📄': 'User Bills',
        '▶️': 'Run Workflow',
        '📊': 'Pipeline Monitor',
        '📋': 'Reports'
    };
    
    setTimeout(() => {
        document.querySelectorAll('[data-baseweb="radio"] label').forEach(label => {
            const icon = label.textContent.trim().substring(0, 2);
            if (labels[icon]) {
                label.setAttribute('title', labels[icon]);
            }
        });
    }, 100);
});
</script>
""", unsafe_allow_html=True)

# Create navigation with icon labels
page_options = list(page_icons.keys())

# Simple approach - show icon and text, CSS will handle visibility
page = st.sidebar.radio(
    "Navigation",
    page_options,
    format_func=lambda x: f"{page_icons[x]}  {x}",
    key="nav_radio",
    label_visibility="collapsed"
)

# -----------------------------------------------------
# ROUTING
# -----------------------------------------------------
if page == "Upload Files":
    from app.components.file_uploader import render_file_uploader
    render_file_uploader()

elif page == "User Bills":
    from app.components.user_bills_viewer import render_user_bills_viewer
    render_user_bills_viewer()

elif page == "Tariff Details":
    from app.components.tariff_details_viewer import render_tariff_details_viewer
    render_tariff_details_viewer()

elif page == "Run Workflow":
    from app.components.workflow_runner import render_workflow_runner
    render_workflow_runner()

elif page == "Pipeline Monitor":
    from app.components.pipeline_monitor import render_pipeline_monitor
    render_pipeline_monitor()

elif page == "Reports":
    from app.components.reports_viewer import render_reports_viewer
    render_reports_viewer()

# -----------------------------------------------------
# FOOTER
# -----------------------------------------------------
st.sidebar.markdown("---")
st.sidebar.caption("© 2025 Troy & Banks | Utility Billing AI Prototype")
