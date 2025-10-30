"""
bill_comparator.py
------------------
📊 Compares actual utility bills with computed tariff-based expected bills.

Purpose:
--------
This module merges client billing data (actual usage/charges)
with tariff rule formulas to compute the "should-be" bill
and identify any mismatches.

Workflow:
---------
1️⃣ Load actual usage data (from /data/raw/ or database).
2️⃣ Load tariff rule definitions (from JSON).
3️⃣ Match each account’s rate_code to tariff rule.
4️⃣ Use calculation_engine to compute expected bill.
5️⃣ Save results to /data/processed/ and DB.

Inputs:
-------
- Actual billing CSV or DataFrame
- tariff_rules.json (structured rule set)

Outputs:
--------
- Bill Comparison CSV → /data/processed/
- Inserted into processed_data table

Depends On:
-----------
- pandas
- src.utils.helpers
- src.utils.logger
- src.database.db_utils
- src.agents.bill_comparison.calculation_engine
"""

import pandas as pd
from src.utils.helpers import load_csv, read_json, save_csv
from src.utils.logger import get_logger
from src.database.db_utils import insert_processed_data
from src.agents.bill_comparison.calculation_engine import compute_expected_charge

logger = get_logger(__name__)

def run_bill_comparison(actual_file="VA_Beach_Bills_Cleaned.csv", rules_file="tariff_rules.json"):
    """
    Compares actual vs expected bills for all accounts.
    """
    try:
        # Step 1️⃣: Load actual data
        actual_df = load_csv("raw", actual_file)
        if actual_df.empty:
            logger.warning("⚠️ No billing data found.")
            return None

        # Step 2️⃣: Load tariff rules
        rules = read_json("processed", rules_file).get("tariff_rules", [])
        rule_df = pd.DataFrame(rules)

        # Step 3️⃣: Merge actual with tariff rate info
        merged = actual_df.merge(rule_df, on="rate_code", how="left")

        # Step 4️⃣: Compute expected charge
        merged["expected_charge"] = merged.apply(compute_expected_charge, axis=1)

        # Step 5️⃣: Calculate difference
        merged["difference"] = merged["actual_charge"] - merged["expected_charge"]

        # Step 6️⃣: Save results
        save_csv(merged, "processed", "Bill_Comparison_Results.csv")
        insert_processed_data(merged)

        logger.info("✅ Bill comparison completed successfully.")
        return merged

    except Exception as e:
        logger.error(f"❌ Bill comparison failed: {e}")
        return None
