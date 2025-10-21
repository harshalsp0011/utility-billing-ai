"""
task_scheduler.py
-----------------
🕒 Lightweight task scheduler for running Utility Billing AI workflows.

Purpose:
--------
Handles periodic or manual task scheduling for:
    - Full workflow runs
    - Individual agent runs
    - One-time executions

Future Extensions:
------------------
✅ Integrate with Apache Airflow or Redis-based queues
✅ REST API trigger endpoint
✅ Email/Slack notifications on completion

Usage Example:
--------------
python -m src.orchestrator.task_scheduler
"""

import time
import threading
import schedule
from datetime import datetime
from src.utils.logger import get_logger
from src.orchestrator.workflow_manager import run_full_workflow
from src.orchestrator.workflow_manager import (
    run_document_processor,
    run_tariff_analysis,
    run_bill_comparison,
    run_error_detection,
    run_reporting
)


logger = get_logger(__name__)

def run_single_agent(agent_name: str):
    """
    Run one specific agent by name (manual trigger).
    """
    mapping = {
        "document": run_document_processor,
        "tariff": run_tariff_analysis,
        "comparison": run_bill_comparison,
        "error": run_error_detection,
        "reporting": run_reporting
    }

    if agent_name not in mapping:
        logger.error(f"❌ Unknown agent: {agent_name}")
        return

    run_task(f"Agent-{agent_name}", mapping[agent_name])


# ----------------------------------------------------------------------
# 1️⃣ Helper Function: Run and Log Task
# ----------------------------------------------------------------------
def run_task(task_name: str, task_func):
    """
    Executes a given function (task) and logs results with timing.
    """
    logger.info(f"🕓 Starting scheduled task: {task_name}")
    start_time = datetime.now()

    try:
        result = task_func()
        elapsed = (datetime.now() - start_time).seconds

        if result:
            logger.info(f"✅ Task '{task_name}' completed successfully in {elapsed}s.")
        else:
            logger.warning(f"⚠️ Task '{task_name}' finished with issues.")

    except Exception as e:
        logger.error(f"❌ Task '{task_name}' failed: {e}")

# ----------------------------------------------------------------------
# 2️⃣ Manual Run (on demand)
# ----------------------------------------------------------------------
def run_manual():
    """
    Lets a user manually run the full workflow.
    """
    logger.info("🧭 Manual trigger activated for full workflow.")
    run_task("Full Workflow", run_full_workflow)

# ----------------------------------------------------------------------
# 3️⃣ Scheduling Setup (Periodic Jobs)
# ----------------------------------------------------------------------
def schedule_daily(hour: int = 9, minute: int = 0):
    """
    Schedules the full workflow to run daily at a given hour/minute.
    """
    time_str = f"{hour:02d}:{minute:02d}"
    schedule.every().day.at(time_str).do(run_task, "Daily Full Workflow", run_full_workflow)
    logger.info(f"📅 Scheduled daily workflow at {time_str}")

def schedule_interval(minutes: int = 60):
    """
    Runs the workflow every X minutes.
    """
    schedule.every(minutes).minutes.do(run_task, f"Interval-{minutes}min", run_full_workflow)
    logger.info(f"⏱️ Scheduled workflow every {minutes} minutes.")

# ----------------------------------------------------------------------
# 4️⃣ Background Thread for Continuous Schedule Loop
# ----------------------------------------------------------------------
def start_scheduler():
    """
    Runs the scheduler loop in a background thread.
    """
    logger.info("🚀 Starting Utility Billing AI Task Scheduler...")
    def run():
        while True:
            schedule.run_pending()
            time.sleep(30)  # check every 30 seconds

    t = threading.Thread(target=run)
    t.daemon = True
    t.start()
    logger.info("✅ Scheduler thread started successfully.")

# ----------------------------------------------------------------------
# 5️⃣ Entry Point
# ----------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("🧪 Running task scheduler self-test...")

    # Example: Run full workflow manually now
    run_manual()

    # Example: Schedule periodic tasks
    schedule_interval(2)     # every 2 minutes
    schedule_daily(9, 0)     # every day at 9:00 AM

    start_scheduler()

    logger.info("🕒 Scheduler running — press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("🛑 Scheduler stopped manually.")
