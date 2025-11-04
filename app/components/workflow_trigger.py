"""
workflow_trigger.py
--------------------
Streamlit component for running the Utility Billing AI workflow
with live progress updates.

This version runs the local orchestrator (`workflow_manager.py`)
and displays step-by-step results directly in the UI.
"""

import streamlit as st
import time
from datetime import datetime

from src.orchestrator import workflow_manager

# -------------------------------------------------------------------
# Helper function to execute each agent with visible feedback
# -------------------------------------------------------------------
def _run_agent_with_status(agent_name, agent_func, progress_placeholder, logs):
    start = datetime.now()
    try:
        progress_placeholder.info(f"{agent_name}: running...")
        success = agent_func()
        elapsed = (datetime.now() - start).total_seconds()
        if success:
            msg = f"{agent_name}: completed in {elapsed:.2f}s"
            progress_placeholder.success(msg)
            logs.append(msg)
            return True
        else:
            msg = f"{agent_name}: finished with issues"
            progress_placeholder.warning(msg)
            logs.append(msg)
            return False
    except Exception as e:
        msg = f"{agent_name}: failed ({e})"
        progress_placeholder.error(msg)
        logs.append(msg)
        return False


# -------------------------------------------------------------------
# Public entry point
# -------------------------------------------------------------------
def render_workflow_trigger():
    """Renders the Streamlit UI section to trigger the workflow."""
    st.title("Run Workflow")
    st.caption("Execute the Utility Billing AI pipeline and monitor progress live.")

    if st.button("Start Workflow"):
        st.write("Starting pipeline...")
        logs = []
        progress_area = st.container()
        start_time = datetime.now()

        steps = [
            ("Document Processor", workflow_manager.run_document_processor),
            ("Tariff Analysis", workflow_manager.run_tariff_analysis),
            ("Bill Comparison", workflow_manager.run_bill_comparison),
            ("Error Detection", workflow_manager.run_error_detection),
            ("Reporting", workflow_manager.run_reporting),
        ]

        for step_name, step_func in steps:
            placeholder = progress_area.empty()
            ok = _run_agent_with_status(step_name, step_func, placeholder, logs)
            time.sleep(0.5)
            if not ok:
                st.error(f"Workflow stopped at {step_name}.")
                break

        end_time = datetime.now()
        total = (end_time - start_time).total_seconds()

        st.markdown("---")
        st.subheader("Run Summary")
        for line in logs:
            st.write("- " + line)

        st.success(f"Workflow finished. Total runtime: {total:.2f}s")


if __name__ == "__main__":
    # For standalone local test
    render_workflow_trigger()
