"""
error_detector.py
-----------------
🚨 Detects anomalies and overcharges in utility billing data.

Purpose:
--------
Analyzes the "Bill_Comparison_Results" dataset to identify
accounts that show discrepancies (overcharge or undercharge)
beyond a defined tolerance.

Workflow:
---------
1️⃣ Load processed bill comparison data.
2️⃣ Apply anomaly detection logic (difference > threshold).
3️⃣ Classify issue type (Overcharge / Undercharge).
4️⃣ Save results to CSV and database.

Inputs:
-------
- Bill Comparison CSV: /data/processed/Bill_Comparison_Results.csv

Outputs:
--------
- Error Detection CSV: /data/processed/Error_Detection_Report.csv
- DB Inserts → validation_results

Depends On:
-----------
- pandas
- src.utils.helpers
- src.utils.logger
- src.database.db_utils
- src.agents.error_detection.threshold_checker
"""

import pandas as pd
from src.utils.helpers import load_csv, save_csv
from src.utils.logger import get_logger
from src.database.db_utils import insert_validation_result
from src.agents.error_detection.threshold_checker import is_significant_difference

logger = get_logger(__name__)

def run_error_detection(file_name="Bill_Comparison_Results.csv", tolerance=5.0):
    """
    Flags significant overcharges or undercharges from comparison results.
    """
    try:
        df = load_csv("processed", file_name)
        if df.empty:
            logger.warning("⚠️ No comparison data available for error detection.")
            return None

        # Step 1️⃣: Identify significant differences
        df["is_error"] = df["difference"].apply(lambda x: is_significant_difference(x, tolerance))

        # Step 2️⃣: Classify issue type
        df["issue_type"] = df["difference"].apply(
            lambda x: "Overcharge" if x > tolerance else ("Undercharge" if x < -tolerance else "None")
        )

        # Step 3️⃣: Filter only errors
        issues = df[df["is_error"] == True].copy()
        issues["status"] = "flagged"

        # Step 4️⃣: Save and insert into DB
        save_csv(issues, "processed", "Error_Detection_Report.csv")

        for _, row in issues.iterrows():
            record = {
                "account_id": row["account_id"],
                "issue_type": row["issue_type"],
                "description": f"Difference of ${round(row['difference'],2)} vs expected",
                "detected_on": pd.Timestamp.now().strftime("%Y-%m-%d"),
                "status": "flagged"
            }
            insert_validation_result(record)

        logger.info(f"✅ Detected {len(issues)} anomalies saved to Error_Detection_Report.csv.")
        return issues

    except Exception as e:
        logger.error(f"❌ Error detection failed: {e}")
        return None
