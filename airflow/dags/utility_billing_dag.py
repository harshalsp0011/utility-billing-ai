"""
utility_billing_dag.py
----------------------
Main DAG that orchestrates the full Utility Billing AI workflow
via the Orchestrator (workflow_manager.py).

Flow:
1️⃣ Document Processor
2️⃣ Tariff Analysis
3️⃣ Bill Comparison
4️⃣ Error Detection
5️⃣ Validation (optional / placeholder)
6️⃣ Reporting
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta
import pendulum
import sys
from pathlib import Path

# Add project root to Python path so we can import from src/
# DAG file is at: <project_root>/airflow/dags/utility_billing_dag.py
# So project root is 3 levels up (dags -> airflow -> project_root)
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Also add <project_root>/src directly for robustness in some import contexts
src_path = project_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

# Import orchestrator functions (prefer fully qualified import, fallback to src on path)
try:
    from src.orchestrator.workflow_manager import (
        run_document_processor,
        run_tariff_analysis,
        run_bill_comparison,
        run_error_detection,
        run_reporting,
    )
except ModuleNotFoundError:
    # Fallback if 'src' is not recognized as a package but src/ is on PYTHONPATH
    from orchestrator.workflow_manager import (
        run_document_processor,
        run_tariff_analysis,
        run_bill_comparison,
        run_error_detection,
        run_reporting,
    )

# Optional placeholder if Validation becomes a standalone agent later
def validation(**kwargs):
    print("🔍 [Validation] Placeholder — no validation agent yet.")
    return "validated_results_ready"

# --------------- DAG DEFAULTS -------------------
default_args = {
    'owner': 'troybanks',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# --------------- DAG DEFINITION -----------------
with DAG(
    dag_id='utility_billing_pipeline',
    default_args=default_args,
    description='End-to-end Utility Billing AI pipeline via orchestrator',
    start_date=pendulum.datetime(2025, 10, 27, tz="UTC"),
    schedule=None,       # manual trigger only
    catchup=False,
    tags=['utility', 'billing', 'AI']
) as dag:

    # --------------- DEFINE TASKS -----------------
    t1 = PythonOperator(
        task_id='document_processing',
        python_callable=run_document_processor,
    )

    t2 = PythonOperator(
        task_id='tariff_analysis',
        python_callable=run_tariff_analysis,
    )

    t3 = PythonOperator(
        task_id='bill_comparison',
        python_callable=run_bill_comparison,
    )

    t4 = PythonOperator(
        task_id='error_detection',
        python_callable=run_error_detection,
    )

    t5 = PythonOperator(
        task_id='validation',
        python_callable=validation,
    )

    t6 = PythonOperator(
        task_id='reporting',
        python_callable=run_reporting,
    )

    # --------------- TASK DEPENDENCIES -------------
    t1 >> t2 >> t3 >> t4 >> t5 >> t6
