# app/components/airflow_trigger.py
"""
airflow_trigger.py
-------------------
Utility functions to interact with Airflow's REST API (v2)
for DAG triggering and live status monitoring.

Updated for Airflow 3.1.0 with JWT token authentication.
"""
from datetime import datetime, timezone
from src.utils.logger import get_logger
from src.utils.config import (
    AIRFLOW_API_URL,
    AIRFLOW_API_USER,
    AIRFLOW_API_PASSWORD,
    AIRFLOW_DAG_ID,
)
import requests
import time
import streamlit as st

logger = get_logger(__name__)

# =====================================================================
# 🔐 JWT TOKEN AUTHENTICATION (NEW for Airflow 3.1.0)
# =====================================================================

_cached_token = None
_token_expires_at = 0


def get_jwt_token():
    """
    Obtain JWT token from Airflow 3.1 /auth/token endpoint.
    Required for all API v2 requests in Airflow 3.1.0+
    
    Returns:
        str: JWT access token or None if failed
    """
    # Construct auth endpoint URL
    base_url = AIRFLOW_API_URL.replace('/api/v2', '')
    auth_url = f"{base_url}/auth/token"
    
    payload = {
        "username": AIRFLOW_API_USER,
        "password": AIRFLOW_API_PASSWORD,
    }
    headers = {"Content-Type": "application/json"}
    
    logger.info(f"Requesting JWT token from: {auth_url}")
    
    try:
        response = requests.post(auth_url, json=payload, headers=headers)
        response.raise_for_status()
        
        token_data = response.json()
        token = token_data.get("access_token")
        
        if token:
            logger.info("✅ Successfully obtained JWT token")
            return token
        else:
            logger.error("Token not in response")
            st.error("❌ Failed to extract token from response")
            return None
            
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error: {e}")
        st.error(f"❌ Cannot connect to Airflow at {AIRFLOW_API_URL}")
        return None
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
        if e.response.status_code == 401:
            st.error("❌ Invalid username or password")
        else:
            st.error(f"❌ Authentication failed: {e.response.status_code}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error getting token: {e}")
        st.error(f"❌ Error: {e}")
        return None


def get_jwt_token_cached():
    """
    Cache JWT token to avoid repeated authentication requests.
    Reuses token if still valid (30s buffer).
    
    Returns:
        str: Cached or fresh JWT token
    """
    global _cached_token, _token_expires_at
    
    current_time = time.time()
    
    # Use cached token if still valid (30s safety buffer)
    if _cached_token and current_time < (_token_expires_at - 30):
        logger.info("Using cached JWT token")
        return _cached_token
    
    logger.info("Token expired or not cached, fetching new token")
    token = get_jwt_token()
    
    if token:
        _cached_token = token
        _token_expires_at = current_time + 3600  # Assume 1 hour expiry
    
    return token


# =====================================================================
# 🚀 TRIGGER DAG RUN (Updated for JWT)
# =====================================================================

def trigger_dag_run():
    """
    Triggers a DAG run using Airflow 3.1 REST API with JWT authentication.
    
    Returns:
        str: dag_run_id if successful, None otherwise
    """
    dag_id = AIRFLOW_DAG_ID
    url = f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns"
    
    # Get JWT token (using cached version)
    token = get_jwt_token_cached()
    if not token:
        logger.error("Failed to get JWT token")
        st.error("❌ Authentication failed. Cannot trigger DAG.")
        return None
    
    # Prepare request with JWT token
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    
    # Airflow 3.1 requires logical_date with timezone info
    payload = {
        "dag_run_id": f"manual__{int(time.time())}", 
        "logical_date": datetime.now(timezone.utc).isoformat(),  # ← With UTC timezone
        "conf": {}
    }
    
    logger.info(f"Triggering DAG via: {url}")
    logger.info(f"Payload: {payload}")
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        logger.info(f"Response Status: {response.status_code}")
        logger.info(f"Response Body: {response.text}")
        
        if response.status_code in (200, 201):
            dag_run = response.json()
            dag_run_id = dag_run.get("dag_run_id") or dag_run.get("run_id")
            st.success(f"✅ Triggered Airflow DAG Run: {dag_run_id}")
            logger.info(f"Successfully triggered DAG: {dag_run_id}")
            return dag_run_id
            
        elif response.status_code == 409:
            st.warning("⚠️ DAG is already running — please wait for it to finish.")
            logger.warning("DAG already running")
            
        elif response.status_code == 401:
            st.error("❌ Authentication failed. Check credentials or token expiry.")
            logger.error("401 Unauthorized - Invalid or expired token")
            
        else:
            st.error(f"❌ Failed to trigger DAG: {response.status_code}\n{response.text}")
            logger.error(f"Failed: {response.status_code} - {response.text}")
            
    except Exception as e:
        logger.error(f"Exception during trigger: {e}")
        st.error(f"❌ Error triggering DAG: {e}")
    
    return None




# =====================================================================
# 📊 DAG & TASK STATUS CHECKS (Updated for JWT)
# =====================================================================

def get_dag_status(dag_run_id: str, token: str) -> str:
    """
    Fetch overall DAG run state.
    
    Args:
        dag_run_id: The DAG run ID
        token: JWT token for authentication
        
    Returns:
        str: DAG state (success, failed, running, queued, etc.)
    """
    dag_id = AIRFLOW_DAG_ID
    url = f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns/{dag_run_id}"
    
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    try:
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            state = resp.json().get("state", "unknown")
            logger.info(f"DAG state: {state}")
            return state
        else:
            logger.error(f"get_dag_status error: {resp.status_code}")
            return "unknown"
    except Exception as e:
        logger.error(f"Exception getting DAG status: {e}")
        return "unknown"


def get_task_statuses(dag_run_id: str, token: str):
    """
    Fetch all task instances for a given DAG run.
    
    Args:
        dag_run_id: The DAG run ID
        token: JWT token for authentication
        
    Returns:
        list: List of task instances with their states
    """
    dag_id = AIRFLOW_DAG_ID
    url = f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
    
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    try:
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            tasks = resp.json().get("task_instances", [])
            logger.info(f"Fetched {len(tasks)} tasks")
            return tasks
        else:
            logger.error(f"get_task_statuses error: {resp.status_code}")
            return []
    except Exception as e:
        logger.error(f"Exception getting task statuses: {e}")
        return []


# =====================================================================
# 📡 LIVE MONITOR FOR STREAMLIT (Updated for JWT)
# =====================================================================

def monitor_dag_run(dag_run_id: str, refresh_interval: int = 5):
    """
    Polls Airflow 3.1 REST API for DAG run progress.
    Displays real-time task states in Streamlit.
    
    Args:
        dag_run_id: The DAG run ID to monitor
        refresh_interval: Seconds between status checks (default 5)
    """
    # Get JWT token once at the start
    token = get_jwt_token_cached()
    if not token:
        st.error("❌ Cannot monitor: failed to get token")
        return
    
    progress_placeholder = st.empty()
    status = "running"
    
    logger.info(f"Starting DAG monitor for: {dag_run_id}")

    while status in ("queued", "running"):
        # Fetch task and DAG status
        task_data = get_task_statuses(dag_run_id, token)
        status = get_dag_status(dag_run_id, token)

        # Display progress in Streamlit
        with progress_placeholder.container():
            st.subheader(f"📊 DAG Status: **{status.upper()}**")
            st.write("**Task Progress:**")
            
            if task_data:
                for task in task_data:
                    task_id = task.get('task_id', 'unknown')
                    task_state = task.get('state', 'unknown')
                    
                    # Color-coded status
                    if task_state == "success":
                        st.write(f"✅ {task_id}: {task_state}")
                    elif task_state == "failed":
                        st.write(f"❌ {task_id}: {task_state}")
                    elif task_state == "running":
                        st.write(f"⏳ {task_id}: {task_state}")
                    else:
                        st.write(f"• {task_id}: {task_state}")
            else:
                st.write("No tasks found yet...")

        # Exit loop if DAG completed
        if status in ("success", "failed"):
            break
        
        # Wait before next poll
        time.sleep(refresh_interval)

    # Final status message
    logger.info(f"DAG monitor ended with status: {status}")
    if status == "success":
        st.success("✅ Workflow completed successfully!")
    elif status == "failed":
        st.error("❌ Workflow failed — check Airflow logs for details.")
