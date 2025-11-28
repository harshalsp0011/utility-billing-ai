import streamlit as st
import json
import os
from pathlib import Path

# Correct project root logic
PROJECT_ROOT = Path(__file__).resolve().parents[2]
JSON_PATH = PROJECT_ROOT / "data" / "processed" / "tariff_definitions.json"

st.set_page_config(page_title="Tariff Logic Viewer", page_icon="📑", layout="wide")

def _load_tariffs():
    """Loads the tariff definitions from JSON."""
    # Check for env var override, else use default path
    override = os.getenv("TARIFF_DEFINITIONS_PATH")
    path = Path(override).expanduser() if override else JSON_PATH
    
    # Fallback check in current dir
    if not path.exists():
        if Path("tariff_definitions.json").exists():
            path = Path("tariff_definitions.json")
        else:
            return [] 

    try:
        with path.open("r") as f:
            data = json.load(f)
        # Ensure data is a list of objects
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
    
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        st.caption("Charge Type")
        st.markdown(f"**{c_type.replace('_',' ').title()}**")
    
    with c2:
        st.caption("Value")
        if c_type == "fixed_fee":
            val = step.get("value", 0)
            st.metric("Amount", f"${val}", label_visibility="collapsed")
        elif c_type == "per_kwh":
             val = step.get("value", 0)
             st.metric("Rate", f"${val:.5f}/kWh", label_visibility="collapsed")
        elif c_type == "formula":
            # Some formulas might be just code
            formula = step.get("python_formula", "N/A")
            st.code(formula, language="python")
        else:
             # Fallback for other types like 'per_kw' if present
             val = step.get("value", "N/A")
             st.write(val)

    with c3:
         # Show unit or formula details if available
         unit = step.get("unit")
         if unit:
             st.caption("Applied To")
             st.code(unit, language="python")

    
    if condition != "Always":
        st.info(f"Order Condition: `{condition}`", icon="⚠️")
    
    st.divider()

def render_tariff_details_viewer():
    st.title("📑 Utility Tariff Inspector")
    
    # Load Data
    tariffs = _load_tariffs()
    if not tariffs:
        active_path = os.getenv("TARIFF_DEFINITIONS_PATH") or str(JSON_PATH)
        st.warning(f"⚠️ Tariff definitions not found. Please run extraction pipeline first.")
        st.caption(f"Expected location: `{active_path}`")
        return

    # --- SINGLE DROPDOWN SELECTION ---
    # 1. Create list of options: "SC Code - Description"
    # We map the display string back to the full object
    tariff_map = {
        f"{t.get('sc_code', 'Unknown')} - {t.get('description', 'No Description')}": t 
        for t in tariffs
    }
    
    # Sort keys for the dropdown
    options = sorted(list(tariff_map.keys()))
    
    # 2. The Selector
    selected_option = st.selectbox(
        "Select Service Classification to Inspect:",
        options,
        index=0
    )
    
    # 3. Get the specific object based on selection
    selected_tariff = tariff_map[selected_option]

    st.markdown("---") # Visual separator

    # --- DETAIL VIEW ---
    # Header
    st.header(f"📂 {selected_tariff.get('sc_code')}")
    st.caption(f"Description: {selected_tariff.get('description')}")
    
    # Metadata Display
    meta = selected_tariff.get('metadata', {})
    if meta:
        m1, m2 = st.columns(2)
        m1.info(f"**Effective Date:** {meta.get('effective_date', 'N/A')}")
        m2.info(f"**Version ID:** {meta.get('version_id', 'N/A')}")

    # Logic Steps Container
    with st.container():
        st.subheader("Calculation Logic")
        steps = selected_tariff.get("logic_steps", [])
        
        if steps:
            for step in steps:
                _render_logic_step(step)
        else:
            st.warning("No logic steps defined for this classification.")
            
    # Notes Section
    notes = selected_tariff.get("notes")
    if notes:
        st.info(f"📝 **Notes:** {notes}")

    # Raw JSON Inspector (Optional, usually helpful for debugging)
    with st.expander("View Raw JSON Source"):
        st.json(selected_tariff)

if __name__ == "__main__":
    render_tariff_details_viewer()