"""
logger.py
----------
📄 Centralized logging utility for the Utility Billing AI System.

Purpose:
--------
Provides a consistent logging setup for all agents (document processing,
tariff analysis, error detection, etc.) to log events, errors, and actions.

Outputs:
---------
✅ Logs to console
✅ Logs to file at /logs/utility_billing.log

Usage Example:
---------------
from src.utils.logger import get_logger
logger = get_logger(__name__)
logger.info("DocumentProcessor started successfully.")
"""

import os
import logging
from datetime import datetime

# ----------------------------------------------------------------------
# 1️⃣ Define log directory (auto-created if missing)
# ----------------------------------------------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# ----------------------------------------------------------------------
# 2️⃣ Define log file name (with timestamp rotation)
# ----------------------------------------------------------------------
LOG_FILE = os.path.join(LOG_DIR, "utility_billing.log")

# ----------------------------------------------------------------------
# 3️⃣ Configure logging format
# ----------------------------------------------------------------------
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# ----------------------------------------------------------------------
# 4️⃣ Logging setup function
# ----------------------------------------------------------------------
def get_logger(name: str = "utility-billing-ai") -> logging.Logger:
    """
    Returns a configured logger instance that logs to both console and file.
    
    Parameters
    ----------
    name : str
        The name of the logger (typically the module name).

    Returns
    -------
    logging.Logger
        Configured logger object.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers
    if not logger.handlers:
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
        logger.addHandler(console_handler)

        # File handler
        file_handler = logging.FileHandler(LOG_FILE, mode="a")
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
        logger.addHandler(file_handler)

    return logger

# ----------------------------------------------------------------------
# 5️⃣ Self-test block (runs only if executed directly)
# ----------------------------------------------------------------------
if __name__ == "__main__":
    test_logger = get_logger("logger_test")
    test_logger.info("✅ Logger initialized successfully.")
    test_logger.warning("⚠️ This is a sample warning.")
    test_logger.error("❌ Example error message.")
    print(f"Logs saved to: {LOG_FILE}")
