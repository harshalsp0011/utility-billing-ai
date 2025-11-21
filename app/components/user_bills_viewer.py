import streamlit as st
import pandas as pd

from src.database.db_utils import (
    fetch_all_account_numbers,
    fetch_user_bills,
    fetch_user_bills_with_issues,
)

# ----------------------------------------------------------------------
# 1. Human-Readable Column Names
# ----------------------------------------------------------------------
COLUMN_RENAMES = {
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


# ----------------------------------------------------------------------
# 2. Highlight Function (uses renamed "Bill ID")
# ----------------------------------------------------------------------
def highlight_anomalies(row, issue_ids):
    if row["Bill ID"] in issue_ids:
        return ["background-color: yellow"] * len(row)
    return [""] * len(row)


# ----------------------------------------------------------------------
# 3. Main Viewer
# ----------------------------------------------------------------------
def render_user_bills_viewer():
    st.title("🔍 User Bills with Highlighted Issues)")

    # --------------------------------------------------------------
    # 1. Select Account
    # --------------------------------------------------------------
    accounts = fetch_all_account_numbers()
    if not accounts:
        st.warning("No accounts found.")
        return

    account_id = st.selectbox("Select Account Number", accounts)

    # --------------------------------------------------------------
    # 2. Fetch ALL bills for this account
    # --------------------------------------------------------------
    bills_df = fetch_user_bills(account_id)
    if bills_df.empty:
        st.warning("No bills found for this account.")
        return

    # --------------------------------------------------------------
    # 3. Fetch Validation Issues
    # --------------------------------------------------------------
    issues_df = fetch_user_bills_with_issues(account_id)

    # Detect correct FK column
    possible_fk_cols = [
        "validation_user_bill_id",
        "user_bill_id",
        "USER_BILL_ID",
        "bvr_user_bill_id",
    ]
    fk_col = next((c for c in possible_fk_cols if c in issues_df.columns), None)

    issue_ids = set()
    if fk_col:
        issue_ids = set(issues_df[fk_col].tolist())

    # --------------------------------------------------------------
    # 4. Prepare Display Table
    # --------------------------------------------------------------
    display_df = bills_df.copy()

    # Rename to human-readable
    display_df = display_df.rename(columns=COLUMN_RENAMES)

    # Convert dates
    for date_col in ["Bill Date", "Read Date", "Uploaded At"]:
        if date_col in display_df.columns:
            display_df[date_col] = pd.to_datetime(display_df[date_col])

    # Index starts from 1
    display_df.index = display_df.index + 1

    # Formatting: all floats → 2 decimals
    numeric_cols = display_df.select_dtypes(include="number").columns.tolist()

    # --------------------------------------------------------------
    # 5. Apply Highlight Style
    # --------------------------------------------------------------
    styled_df = (
        display_df.style
        .apply(highlight_anomalies, issue_ids=issue_ids, axis=1)
        .format({col: "{:.2f}" for col in numeric_cols})
    )

    # --------------------------------------------------------------
    # 6. Render Table
    # --------------------------------------------------------------
    st.subheader("📄 Billing Data (Highlighted Rows = Issues)")
    st.dataframe(styled_df, width='stretch')

    # --------------------------------------------------------------
    # 7. Show Anomaly Details
    # --------------------------------------------------------------
    st.subheader("⚠️ Anomaly Details")

    if issues_df.empty:
        st.info("No anomalies detected for this account.")
        return

    for bill_id in issue_ids:
        with st.expander(f"Bill ID {bill_id} — View Issues"):
            sub = issues_df[issues_df[fk_col] == bill_id][[
                "issue_type",
                "description",
                "status",
                "detected_on"
            ]]
            st.table(sub)
