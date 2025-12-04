# report_viewer.py

import os
from io import BytesIO

import pandas as pd
import streamlit as st

from src.utils.logger import get_logger
from src.utils.data_paths import get_file_path
from src.database.db_utils import fetch_user_bills
from src.agents.reporting_generating_agent.report_generator import BillAuditReporter

logger = get_logger("AuditReportViewer")


def _df_to_excel_bytes(df: pd.DataFrame, account_id: str | None) -> BytesIO:
    """
    Convert a DataFrame to an in-memory Excel file for download.
    """
    output = BytesIO()
    suffix = account_id if account_id else "all_accounts"
    sheet_name = "Audit Results"

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)

    output.seek(0)
    return output


def _get_available_accounts() -> list[str]:
    """
    Fetch all user_bills from DB and return unique bill_account values.
    """
    try:
        df = fetch_user_bills(account_id=None)
    except Exception as e:
        logger.error(f"Error fetching bills for account list: {e}")
        return []

    if df is None or df.empty:
        return []

    if "bill_account" not in df.columns:
        logger.warning("Column 'bill_account' missing in user_bills result.")
        return []

    accounts = (
        df["bill_account"]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )
    accounts = [a for a in accounts if a]  # remove empty strings
    accounts.sort()
    return accounts


def render_report_viewer():
    """
    Streamlit page: Audit Report Viewer

    - User selects an account number
    - We run BillAuditReporter.generate_audit(account_id=...)
    - Show the text report and tabular results
    - Provide an Excel download button
    """

    st.title("🧾 Audit Report Viewer")

    # ---------------------------------------------------------
    # 1. Account Selection
    # ---------------------------------------------------------
    accounts = _get_available_accounts()

    if not accounts:
        st.warning("No account numbers found in user_bills.")
        return

    selected_account = st.selectbox(
        "Select Account Number",
        options=accounts,
        index=0,
        help="Choose an account to generate and view its audit report.",
    )

    run_audit = st.button("Run Audit for Selected Account")

    if not run_audit:
        st.info("Select an account and click **Run Audit for Selected Account**.")
        return

    # ---------------------------------------------------------
    # 2. Initialize BillAuditReporter
    # ---------------------------------------------------------
    # Tariff JSON from data/processed (same as in your generator script)
    tariff_file = get_file_path("processed", "tariff_definitions.json")

    reporter = BillAuditReporter(tariff_file)

    # ---------------------------------------------------------
    # 3. Run audit for the chosen account
    # ---------------------------------------------------------
    with st.spinner(f"Running audit for account {selected_account}..."):
        text_report = reporter.generate_audit(account_id=selected_account)

    # If something went wrong, the reporter returns an error string
    if text_report.startswith("Error"):
        st.error(text_report)
        return
    if "No bill data found" in text_report:
        st.warning(text_report)
        return

    # Convert last_results (list of dicts) into DataFrame
    results = reporter.last_results or []
    if not results:
        st.info("Audit completed, but no results were produced.")
        return

    results_df = pd.DataFrame(results)

    # ---------------------------------------------------------
    # 4. Show text report + DataFrame preview
    # ---------------------------------------------------------
    st.subheader("📄 Audit Text Report")
    st.caption("This is the same human-readable report your CLI script prints.")
    st.text_area(
        "Report",
        value=text_report,
        height=300,
        label_visibility="collapsed",
    )

    st.subheader("🔍 Audit Results Table")
    st.caption(
        "Tabular view of per-bill audit results. Scroll horizontally/vertically to inspect."
    )

    # Optional: hide verbose columns like `trace` from the main preview
    preview_df = results_df.copy()
    if "trace" in preview_df.columns:
        # Keep trace only in the underlying data, not in the main table
        preview_df = preview_df.drop(columns=["trace"])

    st.dataframe(preview_df, use_container_width=True)

    # ---------------------------------------------------------
    # 5. Download as Excel
    # ---------------------------------------------------------
    excel_bytes = _df_to_excel_bytes(results_df, selected_account)

    st.download_button(
        label="⬇️ Download Audit Report (Excel)",
        data=excel_bytes,
        file_name=f"final_audit_report_{selected_account}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        help="Download the full audit report (including trace column) as an Excel file.",
    )
