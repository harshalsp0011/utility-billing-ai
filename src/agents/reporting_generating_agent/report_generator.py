import os
import sys
from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd

# ---- Imports from helper / DB modules -------------------------------------

    # Project-style imports
from src.utils.logger import get_logger
from src.utils.data_paths import get_file_path
import pandas as pd
from src.utils.helpers import clean_column_names
from src.database.db_utils import (
        insert_bill_validation_result,
        fetch_user_bills,
    )


# ---- Domain logic: updated AuditEngine ------------------------------------
try:
    from src.agents.audit_calculation_agent.calc_engine_updated import AuditEngine
except ImportError as e:
    get_logger("AuditReporter").error(f"Missing domain modules: {e}")
    sys.exit(1)

logger = get_logger("AuditReporter")


class BillAuditReporter:
    def __init__(self, tariff_definitions_path: str):
        """
        Initialize the reporter with the path to the tariff definitions JSON.
        """
        self.tariff_path = tariff_definitions_path
        self.last_results: List[Dict] = []

        try:
            self.engine = AuditEngine(tariff_definitions_path)
        except Exception as e:
            logger.error(
                f"Tariff file initialization failed: {tariff_definitions_path} ({e})"
            )
            self.engine = None

    # ------------------------------------------------------------------ #
    # Core: audit directly from user_bills in DB
    # ------------------------------------------------------------------ #

    def generate_audit(self, account_id: Optional[str] = None) -> str:
        """
        Pull pre-existing user_bills from DB, run the calculation engine,
        persist discrepancies, and generate a text report.

        If account_id is provided, filter by that account.
        If None, run for ALL accounts in user_bills.
        """
        if not self.engine:
            return "Error: Audit Engine not initialized."

        logger.info(
            f" Starting DB-based audit for account: {account_id if account_id else 'ALL ACCOUNTS'}"
        )

        # 1. Fetch bills from DB
        try:
            df_bills = fetch_user_bills(account_id=account_id)
        except Exception as e:
            logger.error(f"Error fetching bills from DB: {e}")
            return f"Error fetching bills from DB: {str(e)}"

        if df_bills.empty:
            if account_id:
                return f"No bill data found in user_bills for account_id={account_id}."
            return "No bill data found in user_bills table."

        # 2. Data cleaning / normalization
        df_bills = clean_column_names(df_bills)

        # ensure service_class exists
        if "service_class" not in df_bills.columns:
            logger.info(
                "Service Class missing in user_bills. Defaulting to 'SC1' for all rows."
            )
            df_bills["service_class"] = "SC1"

        # numeric columns cleanup
        numeric_cols = [
            "billed_kwh",
            "billed_demand",
            "billed_rkva",
            "bill_amount",
            "days_used",
        ]
        for col in numeric_cols:
            if col in df_bills.columns:
                df_bills[col] = (
                    df_bills[col]
                    .astype(str)
                    .str.replace(r"[$,]", "", regex=True)
                )
                df_bills[col] = pd.to_numeric(df_bills[col], errors="coerce").fillna(0)

        # bill_date as datetime
        if "bill_date" in df_bills.columns:
            df_bills["bill_date"] = pd.to_datetime(
                df_bills["bill_date"], errors="coerce"
            )

        # mapping from df columns -> engine context keys
        column_mapping = {
            "billed_kwh": "billed_kwh",
            "billed_demand": "billed_demand",
            "billed_rkva": "billed_rkva",
            "days_used": "days_used",
            "bill_date": "bill_date",
            "bill_amount": "bill_amount",
            "service_class": "service_class",
            "delivery_voltage": "delivery_voltage",
            "delivery_voltage_kv": "delivery_voltage_kv",
        }

        audit_results: List[Dict] = []
        logger.info("Running calculation engine on DB rows...")

        # keep a copy for potential user_bill_id matching by date
        db_bills = df_bills.copy()
        if "bill_date" in db_bills.columns:
            db_bills["bill_date"] = db_bills["bill_date"].dt.date

        for _, row in df_bills.iterrows():
            engine_context: Dict = {}
            for df_col, engine_key in column_mapping.items():
                if df_col in row.index:
                    engine_context[engine_key] = row[df_col]

            # main engine call
            calc_result = self.engine.calculate_expected_bill(pd.Series(engine_context))

            # account_id per row
            account_number = str(row.get("bill_account", "")).strip() or (
                str(account_id).strip() if account_id else ""
            )

            # expected value: prefer expected_bill, fallback to expected_amount if needed
            expected_val = calc_result.get("expected_bill", None)
            if expected_val is None:
                expected_val = calc_result.get("expected_amount", 0.0)

            variance_val = calc_result.get("variance", 0.0)

            audit_entry = {
                "date": row.get("bill_date"),
                "sc_code": calc_result.get("sc_code", row.get("service_class")),
                "actual": float(engine_context.get("bill_amount", 0.0)),
                "expected": float(expected_val or 0.0),
                "variance": float(variance_val or 0.0),
                "status": calc_result.get("status", "UNKNOWN"),
                "trace": calc_result.get("trace", []),
                # keep DB identifiers for linking
                "user_bill_id": row.get("id"),
                "bill_account": account_number,
            }
            audit_results.append(audit_entry)

            # persist validation result if variance or status suspicious
            if abs(audit_entry["variance"]) > 0.05 or audit_entry["status"] != "SUCCESS":
                self._persist_validation_result(audit_entry, account_number, db_bills)

        logger.info("Audit generation complete.")
        self.last_results = audit_results
        return self._format_text_report(audit_results, account_id)

    # ------------------------------------------------------------------ #
    # DB writeback: BillValidationResult
    # ------------------------------------------------------------------ #

    def _persist_validation_result(
        self,
        entry: Dict,
        account_id: str,
        db_bills: pd.DataFrame,
    ):
        """
        Helper to save discrepancy to DB using db_utils.
        """
        user_bill_id = entry.get("user_bill_id")

        # If user_bill_id not already present, try to match by bill_date
        if not user_bill_id and not db_bills.empty:
            entry_date = (
                pd.to_datetime(entry["date"]).date()
                if entry["date"] is not None
                else None
            )
            if entry_date:
                match = db_bills[db_bills["bill_date"] == entry_date]
                if not match.empty and "id" in match.columns:
                    user_bill_id = int(match.iloc[0]["id"])

        issue_type = (
            "High Variance"
            if entry["status"] == "SUCCESS"
            else "Calculation Error or Skipped"
        )

        record = {
            "user_bill_id": user_bill_id,  # Can be None if link fails
            "account_id": account_id,
            "issue_type": issue_type,
            "description": (
                f"Variance: ${entry['variance']:.2f}. "
                f"Actual: ${entry['actual']:.2f}, Expected: ${entry['expected']:.2f}. "
                f"Status: {entry['status']}"
            ),
            "status": "open",
            "detected_on": datetime.utcnow(),
        }

        insert_bill_validation_result(record)

    # ------------------------------------------------------------------ #
    # Text report formatting
    # ------------------------------------------------------------------ #

    def _format_text_report(
        self,
        results: List[Dict],
        account_id: Optional[str],
    ) -> str:
        """Create a readable text report (DB-based)."""
        lines: List[str] = []
        lines.append("=" * 70)
        lines.append("UTILITY BILL AUDIT REPORT (DB-based)")
        lines.append(
            f"Account Filter: {account_id if account_id else 'ALL ACCOUNTS'}"
        )
        lines.append(
            f"Date Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        lines.append("=" * 70)
        lines.append("")

        header = (
            f"{'Bill Date':<12} | {'SC':<6} | {'Actual ($)':>12} | "
            f"{'Calc ($)':>12} | {'Diff ($)':>10} | {'Status':<10}"
        )
        lines.append(header)
        lines.append("-" * len(header))

        total_actual = 0.0
        total_expected = 0.0
        total_variance = 0.0

        for res in results:
            date_val = res["date"]
            if pd.isna(date_val):
                date_str = "N/A"
            else:
                date_str = str(date_val).split(" ")[0]

            act = res["actual"]
            exp = res["expected"]
            var = res["variance"]

            total_actual += act
            total_expected += exp
            total_variance += var

            flag = "*" if abs(var) > 0.05 or res["status"] != "SUCCESS" else " "
            lines.append(
                f"{date_str:<12} | {res['sc_code']:<6} | "
                f"{act:12.2f} | {exp:12.2f} | {var:10.2f} | "
                f"{res['status']:<10}{flag}"
            )

        lines.append("-" * len(header))
        lines.append(
            f"{'TOTALS':<12} | {'':<6} | "
            f"{total_actual:12.2f} | {total_expected:12.2f} | {total_variance:10.2f} |"
        )
        lines.append("")

        lines.append("=" * 70)
        lines.append("DETAILED CALCULATION LOGS (Discrepancies > $0.05 or non-SUCCESS)")
        lines.append("=" * 70)

        discrepancies_found = False
        for res in results:
            if abs(res["variance"]) > 0.05 or res["status"] != "SUCCESS":
                discrepancies_found = True
                date_val = res["date"]
                date_str = "N/A" if pd.isna(date_val) else str(date_val).split(" ")[0]
                lines.append(
                    f"\n[Bill Date: {date_str}] Variance: ${res['variance']:.2f}"
                )
                lines.append("-" * 40)
                for step in res.get("trace", []):
                    lines.append(f"  > {step}")

        if not discrepancies_found:
            lines.append("\nNo significant discrepancies found.")

        return "\n".join(lines)


# ==========================================
# Main Execution
# ==========================================


if __name__ == "__main__":
    # Example:
    #   ACCOUNT_ID = "1120031219"   # audit one account
    #   ACCOUNT_ID = None           # audit ALL accounts
    ACCOUNT_ID = "1120031219"

    # Tariff JSON from data/processed
    TARIFF_FILE = get_file_path("processed", "tariff_definitions.json")

    reporter = BillAuditReporter(TARIFF_FILE)

    print("Generating Audit Report from DB...")
    report_output = reporter.generate_audit(account_id=ACCOUNT_ID)

    # Still print the human-readable text report to the console
    print("\n" + report_output)

    # Build Excel filename & path under data/output
    suffix = ACCOUNT_ID if ACCOUNT_ID else "all_accounts"
    excel_filename = f"final_audit_report_{suffix}.xlsx"
    excel_path = get_file_path("output", excel_filename)

    # Convert the stored results to a DataFrame and export to Excel
    results_df = pd.DataFrame(reporter.last_results)
    results_df.to_excel(excel_path, index=False)

    logger.info(f"Excel report saved to {excel_path}")