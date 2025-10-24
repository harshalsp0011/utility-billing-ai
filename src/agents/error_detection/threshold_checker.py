"""
threshold_checker.py
---------------------
📏 Determines if a detected billing difference is significant.

Purpose:
--------
Acts as a validation layer for `error_detector`.
It ensures that only meaningful discrepancies (beyond a tolerance)
are flagged as potential errors.

Workflow:
---------
1️⃣ Receives a charge difference (actual - expected).
2️⃣ Compares against a defined tolerance (default $5).
3️⃣ Returns True for significant discrepancies.

Inputs:
-------
- Difference (float)
- Tolerance (float, default = 5.0)

Outputs:
--------
- Boolean (True if significant)

Depends On:
-----------
- src.utils.logger
"""

from src.utils.logger import get_logger

logger = get_logger(__name__)

def is_significant_difference(diff: float, tolerance: float = 5.0) -> bool:
    """
    Returns True if the billing difference exceeds the given tolerance.
    """
    try:
        if abs(diff) > tolerance:
            logger.debug(f"⚠️ Difference {diff} exceeds tolerance of {tolerance}")
            return True
        return False
    except Exception as e:
        logger.error(f"❌ Error in threshold check: {e}")
        return False
