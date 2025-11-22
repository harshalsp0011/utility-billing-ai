import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder

from src.database.db_utils import (
    fetch_all_account_numbers,
    fetch_user_bills,
    fetch_user_bills_with_issues,
)

# ---------------------------------------------------------
# Column mappings for display
# ---------------------------------------------------------
BILL_COLUMN_RENAMES = {
    "id": "Bill ID",
    "bill_account": "Bill Account",
    "customer": "Customer",
    "bill_date": "Bill Date",
    "read_date": "Read Date",
    "days_used": "Days Used",
    "billed_kwh": "Billed kWh",
    "billed_demand": "Billed Demand",
    "load_factor": "Load Factor",
    "billed_rkva": "Billed rKVA",
    "bill_amount": "Bill Amount",
    "sales_tax_amt": "Sales Tax Amount",
    "bill_amount_with_sales_tax": "Bill Amount (With Tax)",
    "retracted_amt": "Retracted Amount",
    "sales_tax_factor": "Sales Tax Factor",
    "created_at": "Uploaded At",
}

ISSUE_COLUMN_RENAMES = {
    "bill_id": "Bill ID",
    "fk_user_bill_id": "Bill ID (FK)",
    "issue_id": "Issue ID",
    "issue_type": "Issue Type",
    "description": "Description",
    "status": "Status",
    "detected_on": "Detected On",

    "bill_account": "Bill Account",
    "customer": "Customer",
    "bill_date": "Bill Date",
    "read_date": "Read Date",
    "days_used": "Days Used",
    "billed_kwh": "Billed kWh",
    "billed_demand": "Billed Demand",
    "load_factor": "Load Factor",
    "billed_rkva": "Billed rKVA",
    "bill_amount": "Bill Amount",
    "sales_tax_amt": "Sales Tax Amount",
    "bill_amount_with_sales_tax": "Bill Amount (With Tax)",
    "retracted_amt": "Retracted Amount",
    "sales_tax_factor": "Sales Tax Factor",
    "bill_created_at": "Uploaded At",
}


# ---------------------------------------------------------
# MAIN PAGE
# ---------------------------------------------------------
def render_user_bills_viewer():
    st.title("📄 User Billing Data Viewer")

    # ===========================
    # Account Selection
    # ===========================
    accounts = fetch_all_account_numbers()
    if not accounts:
        st.warning("No accounts found in database.")
        return

    account_id = st.selectbox("Select Account Number", accounts)

    # ===========================
    # Fetch Bills
    # ===========================
    bills_df = fetch_user_bills(account_id)
    if bills_df.empty:
        st.warning("No bills found for this account.")
        return

    # ===========================
    # Fetch Issues
    # ===========================
    issues_df = fetch_user_bills_with_issues(account_id)

    # Extract bill IDs that have anomalies
    issue_bill_ids = set()
    if not issues_df.empty and "bill_id" in issues_df.columns:
        issue_bill_ids = (
            pd.to_numeric(issues_df["bill_id"], errors="coerce")
            .dropna()
            .astype(int)
            .tolist()
        )
        issue_bill_ids = set(issue_bill_ids)

    # ===========================
    # Prepare Billing Data
    # ===========================
    display_df = bills_df.rename(columns=BILL_COLUMN_RENAMES).copy()

    # Convert date fields
    for date_col in ["Bill Date", "Read Date", "Uploaded At"]:
        if date_col in display_df.columns:
            display_df[date_col] = pd.to_datetime(display_df[date_col], errors="coerce")

    # Add helper column for AG-Grid styling
    display_df["_has_issue"] = display_df["Bill ID"].apply(
        lambda x: 1 if x in issue_bill_ids else 0
    )

    # ===========================
    # AG-GRID for Billing Table
    # ===========================
    st.subheader("🔍 Billing Data")

    gb = GridOptionsBuilder.from_dataframe(display_df)
    gb.configure_grid_options(
        rowClassRules={
            # highlight full row if issue
            "issue-row": "data._has_issue == 1"
        }
    )
    grid_options = gb.build()

    # CSS: Full red frame + faint red background + glow
    custom_css = {
        ".issue-row": {
            "background-color": "rgba(255, 0, 0, 0.10) !important;",
            "border-top": "3px solid #ff1a1a !important;",
            "border-bottom": "3px solid #ff1a1a !important;",
            "border-left": "6px solid #ff1a1a !important;",
            "border-right": "6px solid #ff1a1a !important;",
            "box-shadow": "inset 0 0 12px rgba(255, 0, 0, 0.45) !important;",
            "border-radius": "4px !important;",
        }
    }

    AgGrid(
        display_df,
        gridOptions=grid_options,
        custom_css=custom_css,
        allow_unsafe_jscode=True,
        fit_columns_on_grid_load=True,
        theme="streamlit",
        height=550,
    )

    # ===========================
    # Issues Table
    # ===========================
    st.subheader("⚠️ Anomalies Detected")

    if issues_df.empty:
        st.info("No anomalies detected for this account.")
        return

    issues_display = issues_df.rename(columns=ISSUE_COLUMN_RENAMES).copy()

    if "Detected On" in issues_display.columns:
        issues_display["Detected On"] = pd.to_datetime(
            issues_display["Detected On"], errors="coerce"
        )

    for col in ["Bill Date", "Read Date"]:
        if col in issues_display.columns:
            issues_display[col] = pd.to_datetime(issues_display[col], errors="coerce")

    st.dataframe(issues_display, width="stretch", height=350)
