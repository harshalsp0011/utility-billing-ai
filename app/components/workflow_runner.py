import streamlit as st
from app.components.airflow_trigger import trigger_dag_run, monitor_dag_run

def render_workflow_runner():
    st.title("Run Workflow")
    st.write("Trigger the full billing analysis pipeline.")

    if st.button("▶️ Start Airflow Workflow"):
        dag_run_id = trigger_dag_run()

        if dag_run_id:
            st.success(f"Triggered DAG Run: {dag_run_id}")
            monitor_dag_run(dag_run_id, refresh_interval=5)
        else:
            st.error("Could not trigger workflow.")
