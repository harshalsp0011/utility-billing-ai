# app/components/airflow_trigger.py
"""
airflow_trigger.py
-------------------
Utility functions to interact with Airflow’s REST API
for DAG triggering and live status monitoring.
"""



from src.utils.logger import get_logger
logger = get_logger(__name__)
import requests
import time
import streamlit as st

AIRFLOW_API_URL = "http://localhost:8080/api/v2"
DAG_ID = "utility_billing_pipeline"
AUTH = ('admin', 'admin')   # default standalone user

def trigger_dag_run():
    """
    Triggers the Airflow DAG and returns the dag_run_id.
    """
    url = f"{AIRFLOW_API_URL}/dags/{DAG_ID}/dagRuns"
    logger.info(f"url: {url}")
    response = requests.post(url, auth=AUTH, json={})

    logger.info(f"response: {response}")

    if response.status_code in [200, 201]:
        dag_run = response.json()
        dag_run_id = dag_run.get("dag_run_id", dag_run.get("run_id"))
        st.success(f"Triggered DAG Run: {dag_run_id}")
        return dag_run_id
    elif response.status_code == 409:
        st.warning("DAG is already running — please wait.")
        return None
    else:
        st.error(f"Failed to trigger DAG: {response.status_code} → {response.text}")
        return None


def get_dag_status(dag_run_id):
    """
    Fetches DAG run status from Airflow 3.x API.
    """
    url = f"{AIRFLOW_API_URL}/dags/{DAG_ID}/dagRuns/{dag_run_id}"
    response = requests.get(url, auth=AUTH)
    if response.status_code == 200:
        return response.json().get("state", "unknown")
    return "unknown"


def get_task_statuses(dag_run_id):
    """
    Fetches list of task instances under the DAG run.
    """
    url = f"{AIRFLOW_API_URL}/dags/{DAG_ID}/dagRuns/{dag_run_id}/taskInstances"
    response = requests.get(url, auth=AUTH)
    if response.status_code == 200:
        return response.json().get("task_instances", [])
    return []


def monitor_dag_run(dag_run_id, refresh_interval=5):
    """
    Continuously polls Airflow for DAG progress.
    """
    progress_placeholder = st.empty()
    status = "running"

    while status in ["queued", "running"]:
        task_data = get_task_statuses(dag_run_id)
        status = get_dag_status(dag_run_id)
        
        with progress_placeholder.container():
            st.markdown(f"### DAG Status: **{status.upper()}**")
            for task in task_data:
                st.write(f"- {task['task_id']}: {task['state']}")
        
        if status in ["success", "failed"]:
            break
        time.sleep(refresh_interval)

    if status == "success":
        st.success("✅ Workflow completed successfully!")
    elif status == "failed":
        st.error("❌ Workflow failed. Check Airflow logs.")