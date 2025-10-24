"""
cross_checker.py
-----------------
🔍 Cross-validates detected errors against known reference statements.

Purpose:
--------
Acts as the logic layer for `validator.py`.
Uses reference billing data or known patterns to confirm whether
the detected discrepancy is legitimate or an acceptable variation.

Workflow:
---------
1️⃣ Receives one anomaly record and reference dataset.
2️⃣ Checks if rate_code, usage, or charge pattern matches expected values.
3️⃣ Returns True/False and a validation note.

Inputs:
-------
- One flagged issue (row)
- Reference DataFrame (baseline or sample bills)

Outputs:
--------
- Boolean (True if confirmed)
- String (validation note / reason)

Depends On:
-----------
- pandas
- src.utils.logger
"""

from src.utils.logger import get_logger

logger = get_logger(__name__)

def cross_validate_issue(issue_row, reference_df):
    """
    Compare flagged issue with reference data.
    """
    try:
        rate = issue_row.get("rate_code")
        diff = issue_row.get("difference")
        usage = issue_row.get("usage_kwh")

        ref = reference_df[reference_df["rate_code"] == rate]

        if ref.empty:
            note = f"No reference data for {rate}"
            return False, note

        avg_ref_usage = ref["usage_kwh"].mean()
        tolerance = avg_ref_usage * 0.1  # ±10% margin

        if abs(usage - avg_ref_usage) > tolerance:
            note = f"Usage anomaly confirmed (Δ {round(abs(usage - avg_ref_usage), 2)} > tolerance)"
            return True, note

        if diff > 10:
            note = f"Overcharge exceeds safe margin (${diff})"
            return True, note

        return False, "Within normal variance"

    except Exception as e:
        logger.error(f"❌ Cross-check failed: {e}")
        return False, "Cross-check error"
