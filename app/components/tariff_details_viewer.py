import streamlit as st
import json
import os
from pathlib import Path

# Correct project root logic
PROJECT_ROOT = Path(__file__).resolve().parents[2]
# Canonical processed output location produced by group + extract pipeline
JSON_PATH = PROJECT_ROOT / "data" / "processed" / "tariff_definitions.json"

st.set_page_config(page_title="Tariff Logic Viewer", page_icon="📑", layout="wide")

def _load_tariffs():
    """Load tariff definitions honoring optional override and robust fallbacks."""
    override = os.getenv("TARIFF_DEFINITIONS_PATH")
    path = Path(override).expanduser() if override else JSON_PATH

    # If override supplied but invalid, fall back to canonical JSON_PATH
    if override and not path.exists():
        if JSON_PATH.exists():
            path = JSON_PATH
        elif Path("tariff_definitions.json").exists():
            path = Path("tariff_definitions.json")
        else:
            return []
    # If no override and canonical missing, try cwd local file
    elif not override and not path.exists():
        if Path("tariff_definitions.json").exists():
            path = Path("tariff_definitions.json")
        else:
            return []

    try:
        with path.open("r") as f:
            data = json.load(f)
        if isinstance(data, dict) and "tariffs" in data:
            return data["tariffs"]
        if isinstance(data, dict):
            return [data]
        return data if isinstance(data, list) else []
    except Exception as e:
        st.error(f"Failed to load tariff definitions: {e}")
        return []

def _render_logic_step(step):
    """Renders a single calculation step as a visual card."""
    name = step.get("step_name", "Unknown Step")
    c_type = step.get("charge_type", "N/A")
    condition = step.get("condition", "Always")
    
    # Icon selection
    icon = "💰" if c_type == "fixed_fee" else "⚡"
    
    st.markdown(f"#### {icon} {name}")
    
    c1, c2 = st.columns([1, 3])
    with c1:
        st.caption("Charge Type")
        st.markdown(f"**{c_type.replace('_',' ').title()}**")
    
    with c2:
        st.caption("Logic / Value")
        if c_type == "fixed_fee":
            val = step.get("value", 0)
            st.metric("Amount", f"${val}", label_visibility="collapsed")
        elif c_type == "formula":
            formula = step.get("python_formula", "N/A")
            st.code(formula, language="python")
    
    if condition != "Always":
        st.info(f"Order Condition: `{condition}`", icon="⚠️")
    
    st.divider()

def render_tariff_details_viewer():
    st.title("📑 Utility Tariff Inspector")
    
    # Load Data
    tariffs = _load_tariffs()
    if not tariffs:
        active_path = os.getenv("TARIFF_DEFINITIONS_PATH") or str(JSON_PATH)
        st.warning("⚠️ Tariff definitions not found.")
        st.caption(f"Expected location: `{active_path}`")
        st.markdown("Generate them with:")
        st.code("python src/agents/document_processor/group_tariffs.py\npython src/agents/document_processor/extract_logic.py", language="bash")
        with st.expander("Debug Path Info"):
            st.write({
                "env_override": os.getenv("TARIFF_DEFINITIONS_PATH"),
                "JSON_PATH": str(JSON_PATH),
                "JSON_PATH_exists": JSON_PATH.exists(),
                "cwd": str(Path.cwd()),
                "local_tariff_definitions_exists": Path("tariff_definitions.json").exists()
            })
        return

    # --- SINGLE DROPDOWN SELECTION ---
    # 1. Create list of options: "SC Code - Description"
    # We map the display string back to the full object
    tariff_map = {
        f"{t.get('sc_code', 'Unknown')} - {t.get('description', 'No Description')}": t 
        for t in tariffs
    }
    
    # Sort keys for dropdown and prepend placeholder
    options = ["-- Select a Service Class --"] + sorted(list(tariff_map.keys()))

    # Place dropdown and button in same row using columns
    col1, col2 = st.columns([4, 1])
    
    with col1:
        selected_option = st.selectbox(
            "Select Service Classification:",
            options,
            index=0,
            label_visibility="visible"
        )
    
    with col2:
        # Button always visible, disabled when placeholder selected
        is_valid_selection = selected_option != "-- Select a Service Class --"
        # Match selectbox label height exactly
        st.markdown('<p style="margin: 0; padding: 0; height: 1.5rem; line-height: 1.5rem;">&nbsp;</p>', unsafe_allow_html=True)
        show_button = st.button("Show Details", type="primary", disabled=not is_valid_selection, use_container_width=True)

    # Use session state to track if details should be shown
    if 'current_selection' not in st.session_state:
        st.session_state.current_selection = None
    if 'show_details' not in st.session_state:
        st.session_state.show_details = False
    
    # If selection changed, clear display
    if st.session_state.current_selection != selected_option:
        st.session_state.current_selection = selected_option
        st.session_state.show_details = False
    
    # If button clicked, show details
    if show_button:
        st.session_state.show_details = True
    
    # Display details only if session state indicates to show and valid selection
    if is_valid_selection and st.session_state.show_details:
        selected_tariff = tariff_map[selected_option]
        st.markdown("---")
        st.header(f"📂 {selected_tariff.get('sc_code')}")
        st.caption(f"Description: {selected_tariff.get('description')}")
        with st.container():
            st.subheader("Calculation Logic")
            steps = selected_tariff.get("logic_steps", [])
            if steps:
                for step in steps:
                    _render_logic_step(step)
            else:
                st.warning("No logic steps defined for this classification.")
        with st.expander("View Raw JSON Source"):
            st.json(selected_tariff)

if __name__ == "__main__":
    render_tariff_details_viewer()