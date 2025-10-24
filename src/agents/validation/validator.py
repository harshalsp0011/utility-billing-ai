"""
validator.py
-------------
✔️ Validates detected billing anomalies against known rules or reference data.

Purpose:
--------
Takes all "flagged" anomalies from the Error Detection Agent and verifies
their authenticity by comparing them against:
    - Reference sample bills
    - Known rule-based exceptions
    - Historical billing consistency

Workflow:
---------
1️⃣ Load flagged issues (Error_Detection_Report.csv)
2️⃣ Load reference or baseline bills (if available)
3️⃣ Run cross-checks using cross_checker.py
4️⃣ Update validation status (Validated / False Positive)
5️⃣ Save to /data/processed/Validated_Errors_Report.csv and DB

Inputs:
-------
- Error_Detection_Report.csv
- Reference_Bills.csv or sample statement data

Outputs:
--------
- Validated_Errors_Report.csv
- Updated DB entries with validation status

Depends On:
-----------
- pandas
- src.utils.helpers
- src.utils.logger
- src.database.db_utils
- src.agents.validation.cross_checker
"""

import pandas as pd
from src.utils.helpers import load_csv, save_csv
from src.utils.logger import get_logger
from src.database.db_utils import update_validation_result
from src.agents.validation.cross_checker import cross_validate_issue

logger = get_logger(__name__)

def run_validation(file_name="Error_Detection_Report.csv", reference_file="Reference_Bills.csv"):
    """
    Validates flagged anomalies by cross-checking with reference bills or rules.
    """
    try:
        issues = load_csv("processed", file_name)
        if issues.empty:
            logger.warning("⚠️ No flagged issues found to validate.")
            return None

        reference_df = load_csv("samples", reference_file)

        validated_records = []
        for _, row in issues.iterrows():
            is_valid, reason = cross_validate_issue(row, reference_df)
            row["validation_status"] = "Validated" if is_valid else "False Positive"
            row["validation_notes"] = reason
            validated_records.append(row)

            update_validation_result(
                row["account_id"],
                row["validation_status"],
                row["validation_notes"]
            )

        validated_df = pd.DataFrame(validated_records)
        save_csv(validated_df, "processed", "Validated_Errors_Report.csv")

        logger.info(f"✅ Validation complete — {len(validated_df)} records processed.")
        return validated_df

    except Exception as e:
        logger.error(f"❌ Validation failed: {e}")
        return None
