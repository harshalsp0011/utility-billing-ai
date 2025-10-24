"""
report_generator.py
--------------------
📑 Generates Excel and PDF reports summarizing validated billing errors and insights.

Purpose:
--------
Creates clean, client-friendly deliverables using the validated output data.
Summarizes:
    - Overcharges and savings
    - Count of validated issues by rate_code or location
    - Total refund potential

Workflow:
---------
1️⃣ Load validated data from /data/processed/.
2️⃣ Compute refund summaries (sum of overcharge amounts).
3️⃣ Create Excel workbook with multiple sheets.
4️⃣ Export summarized report to /data/output/.
5️⃣ Optionally generate PDF version for client sharing.

Inputs:
-------
- Validated_Errors_Report.csv

Outputs:
--------
- Excel report (Error_Summary_<DATE>.xlsx)
- Optional PDF summary

Depends On:
-----------
- pandas
- openpyxl
- src.utils.helpers
- src.utils.logger
"""

import os
import pandas as pd
from datetime import datetime
from src.utils.helpers import load_csv, save_excel
from src.utils.logger import get_logger

logger = get_logger(__name__)

def generate_error_report(file_name="Validated_Errors_Report.csv"):
    """
    Builds Excel report summarizing all validated issues.
    """
    try:
        df = load_csv("processed", file_name)
        if df.empty:
            logger.warning("⚠️ No validated records found for reporting.")
            return None

        report_date = datetime.now().strftime("%Y_%m_%d")
        output_name = f"Error_Summary_{report_date}.xlsx"

        # Summary 1️⃣: Overall refund potential
        total_refund = df[df["issue_type"] == "Overcharge"]["difference"].sum()

        # Summary 2️⃣: Count by rate code
        summary_rate = df.groupby("rate_code")["difference"].sum().reset_index()
        summary_rate.rename(columns={"difference": "total_overcharge"}, inplace=True)

        # Summary 3️⃣: Count by validation_status
        summary_status = df["validation_status"].value_counts().reset_index()
        summary_status.columns = ["status", "count"]

        # Prepare Excel sheets
        sheets = {
            "Validated Records": df,
            "Summary by Rate Code": summary_rate,
            "Validation Status Summary": summary_status,
        }

        save_excel(sheets, "output", output_name)

        logger.info(f"✅ Report generated: data/output/{output_name}")
        logger.info(f"💰 Total Refund Potential: ${round(total_refund,2)}")
        return output_name

    except Exception as e:
        logger.error(f"❌ Report generation failed: {e}")
        return None
