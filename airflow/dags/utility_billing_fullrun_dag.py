"""
utility_billing_fullrun_dag.py
------------------------------
Runs the complete Utility Billing AI workflow as a single Airflow task
by calling the orchestrator (workflow_manager.py → run_full_workflow()).

✅ Best for:
- Full production runs
- End-to-end test of all agents via orchestrator
- Ensures DB logging + output files generated in one go
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta
import pendulum
import sys
from pathlib import Path
import logging

# -------------------- PATH SETUP --------------------
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

logger = logging.getLogger(__name__)
logger.info(f"[DAG INIT] Path setup complete → {project_root}")

# -------------------- TASK DEFINITION --------------------
def run_full_pipeline(**kwargs):
    """
    Wrapper to import and run the full orchestrator workflow.
    Automatically creates and updates a PipelineRun DB entry.
    """
    from src.database.db_utils import start_pipeline_run, update_pipeline_run
    from src.orchestrator.workflow_manager import run_full_workflow

    dag_id = kwargs["dag"].dag_id
    run_id = start_pipeline_run(dag_id)

    try:
        success = run_full_workflow()
        status = "success" if success else "failed"
        update_pipeline_run(run_id, status)
        return f"{dag_id} completed with status: {status}"
    except Exception as e:
        update_pipeline_run(run_id, "failed", str(e))
        raise

# -------------------- DAG CONFIGURATION --------------------
default_args = {
    'owner': 'troybanks',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='utility_billing_fullrun',
    default_args=default_args,
    description='Run entire Utility Billing AI workflow via orchestrator',
    start_date=pendulum.datetime(2025, 10, 27, tz="UTC"),
    schedule=None,  # manual trigger
    catchup=False,
    tags=['utility', 'billing', 'AI', 'orchestrator']
) as dag:

    run_workflow = PythonOperator(
        task_id='run_full_workflow',
        python_callable=run_full_pipeline,
    )

run_workflow
