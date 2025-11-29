import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode

# Import your DB utils
from src.database.db_utils import (
    fetch_all_account_numbers,
    fetch_user_bills,
    fetch_user_bills_with_issues,
)

# ---------------------------------------------------------
# POPUP COMPONENT (Streamlit Dialog)
# ---------------------------------------------------------
@st.dialog("⚠️ Anomaly Details")
def show_anomaly_popup(bill_id, bill_data, issues_data):
    """Modal popup showing full bill details plus all related issues (wide layout)."""

    # Inject CSS to widen dialog and format sections
    st.markdown(
        """
        <style>
        /* Widen the Streamlit dialog */
        div[role="dialog"] {width:95vw !important; max-width:1400px !important;}
        div[role="dialog"] .stDialog {width:95vw !important;}
        /* Compact responsive metric grid */
        .metrics-grid {display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:12px 18px; margin:8px 0 18px;}
        .metric-item {background:#fafafa; border:1px solid #eee; border-radius:6px; padding:8px 10px;}
        .metric-item .m-label {font-size:11px; letter-spacing:.5px; text-transform:uppercase; color:#555; margin-bottom:4px;}
        .metric-item .m-value {font-size:15px; font-weight:600; color:#111;}
        /* Raw record grid */
        .raw-grid {display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:6px 20px; margin-top:4px;}
        .raw-item {padding:2px 4px; background:transparent;}
        .raw-item .r-key {font-weight:600; color:#333; margin-right:4px; white-space:nowrap;}
        .raw-item .r-val {color:#111;}
        /* Issues */
        .issue-card {border:1px solid #ffdddd; padding:0.55rem 0.85rem; border-radius:6px; background:rgba(255,0,0,0.05); margin-bottom:0.55rem; font-size:0.82rem;}
        .issues-scroll {max-height:360px; overflow-y:auto; padding-right:4px;}
        .issues-scroll::-webkit-scrollbar {width:8px;}
        .issues-scroll::-webkit-scrollbar-track {background:transparent;}
        .issues-scroll::-webkit-scrollbar-thumb {background:#bbb; border-radius:4px;}
        .section-title {margin:0.25rem 0 0.55rem 0;}
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(f"### 🧾 Bill Details (ID: `{bill_id}`)")

    # --- Bill Summary Metrics (CSS grid for compact multi-column) ---
    summary_pairs = [
        ("Bill Date", bill_data.get("Bill Date")),
        ("Read Date", bill_data.get("Read Date")),
        ("Days Used", bill_data.get("Days Used")),
        ("Billed kWh", bill_data.get("Billed kWh")),
        ("Billed Demand", bill_data.get("Billed Demand")),
        ("Load Factor", bill_data.get("Load Factor")),
        ("Bill Amount", bill_data.get("Bill Amount")),
        ("Sales Tax Amount", bill_data.get("Sales Tax Amount")),
        ("Bill Amount (With Tax)", bill_data.get("Bill Amount (With Tax)")),
        ("Retracted Amount", bill_data.get("Retracted Amount")),
        ("Sales Tax Factor", bill_data.get("Sales Tax Factor")),
    ]
    metrics_html = ["<div class='metrics-grid'>"]
    for label, value in summary_pairs:
        val_display = value if value not in (None, "") else "-"
        if isinstance(val_display, str) and "T" in val_display:
            val_display = val_display.split("T")[0]
        metrics_html.append(f"<div class='metric-item'><div class='m-label'>{label}</div><div class='m-value'>{val_display}</div></div>")
    metrics_html.append("</div>")
    st.markdown("".join(metrics_html), unsafe_allow_html=True)

    st.markdown("---")

    # --- Raw Bill Record Key/Value (grid, compact) ---
    st.markdown("<h4 class='section-title'>🔍 Raw Bill Record</h4>", unsafe_allow_html=True)
    excluded = {"_has_issue"}
    detail_dict = {k: v for k, v in bill_data.items() if k not in excluded}
    # Prioritize frequently referenced keys first
    priority = [
        "Bill Account","Customer","Bill ID","Bill Date","Read Date","Uploaded At",
        "Bill Amount","Bill Amount (With Tax)","Billed kWh","Billed Demand","Days Used",
        "Load Factor","Sales Tax Amount","Sales Tax Factor","Retracted Amount","Billed rKVA"
    ]
    ordered_keys = [k for k in priority if k in detail_dict] + [k for k in detail_dict if k not in priority]
    kv_html = ["<div class='raw-grid'>"]
    for k in ordered_keys:
        v = detail_dict.get(k)
        val_display = v if v not in (None, "") else "-"
        if isinstance(val_display, str) and "T" in val_display:
            val_display = val_display.split("T")[0]
        kv_html.append(f"<div class='raw-item'><span class='r-key'>{k}:</span><span class='r-val'>{val_display}</span></div>")
    kv_html.append("</div>")
    st.markdown("".join(kv_html), unsafe_allow_html=True)

    st.markdown("---")

    # --- Issues Section ---
    st.markdown("<h4 class='section-title'>⚠️ Detected Issues</h4>", unsafe_allow_html=True)
    if issues_data.empty:
        st.success("No issue rows found for this bill in validation log.")
    else:
        st.markdown('<div class="issues-scroll">', unsafe_allow_html=True)
        for _, row in issues_data.iterrows():
            issue_type = row.get('issue_type','Unknown Issue')
            description = row.get('description','(No description)')
            status = row.get('status','N/A')
            detected = row.get('detected_on','N/A')
            st.markdown(
                f"<div class='issue-card'><strong>{issue_type}</strong><br/><em>{description}</em><br/><small>Status: {status} | Detected: {detected}</small></div>",
                unsafe_allow_html=True,
            )
        st.markdown('</div>', unsafe_allow_html=True)


# ---------------------------------------------------------
# Column mappings (Kept exactly as your original code)
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
    # Fetch Data
    # ===========================
    bills_df = fetch_user_bills(account_id)
    if bills_df.empty:
        st.warning("No bills found for this account.")
        return

    issues_df = fetch_user_bills_with_issues(account_id)

    # Extract anomalous bill IDs
    issue_bill_ids = set()
    if not issues_df.empty and "bill_id" in issues_df.columns:
        issue_bill_ids = (
            pd.to_numeric(issues_df["bill_id"], errors="coerce")
            .dropna()
            .astype(int)
            .tolist()
        )
        issue_bill_ids = set(issue_bill_ids)

    # Prepare Display Data
    display_df = bills_df.rename(columns=BILL_COLUMN_RENAMES).copy()

    for date_col in ["Bill Date", "Read Date", "Uploaded At"]:
        if date_col in display_df.columns:
            display_df[date_col] = pd.to_datetime(display_df[date_col], errors="coerce")

    # Flag for styling
    display_df["_has_issue"] = display_df["Bill ID"].apply(
        lambda x: 1 if x in issue_bill_ids else 0
    )

    # ===========================
    # AG-GRID SETUP
    # ===========================
    st.subheader("🔍 Billing Data")
    st.info("👆 Click on any red highlighted row to see anomaly details.")

    gb = GridOptionsBuilder.from_dataframe(display_df)
    
    # 1. Configure Row Styling (Red Highlights)
    gb.configure_grid_options(
        rowClassRules={
            "issue-row": "data._has_issue == 1"
        }
    )

    # 2. Configure Selection (New Addition)
    gb.configure_selection(
        selection_mode="single",  # Only allow one row to be selected
        use_checkbox=False,       # Select by clicking the row directly
        pre_selected_rows=[]      # Start with nothing selected
    )
    
    grid_options = gb.build()

    # CSS for styling (Same as your original)
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

    # 3. Render Grid with Update Mode
    # We must use SELECTION_CHANGED so Streamlit reruns the script when a user clicks a row
    grid_response = AgGrid(
        display_df,
        gridOptions=grid_options,
        custom_css=custom_css,
        allow_unsafe_jscode=True,
        fit_columns_on_grid_load=True,
        theme="streamlit",
        height=550,
        update_mode=GridUpdateMode.SELECTION_CHANGED, # Important!
        data_return_mode=DataReturnMode.FILTERED_AND_SORTED
    )

    # ===========================
    # POPUP LOGIC
    # ===========================
    selected_rows = grid_response['selected_rows']
    
    # Normalize selected rows into a single dict (or None)
    selected_row_dict = None
    if isinstance(selected_rows, pd.DataFrame):
        if not selected_rows.empty:
            selected_row_dict = selected_rows.iloc[0].to_dict()
    elif isinstance(selected_rows, list) and len(selected_rows) > 0:
        selected_row_dict = selected_rows[0]

    if selected_row_dict:
        selected_bill_id = selected_row_dict.get("Bill ID")
        has_issue = selected_row_dict.get("_has_issue")

        if has_issue == 1 and selected_bill_id is not None:
            try:
                selected_bill_id_int = int(selected_bill_id)
                relevant_issues = issues_df[issues_df['bill_id'] == selected_bill_id_int]
                show_anomaly_popup(selected_bill_id_int, selected_row_dict, relevant_issues)
            except Exception as e:
                st.error(f"Error loading details: {e}")

    # ===========================
    # Issues Table (Bottom View)
    # ===========================
    # (Kept for overview purposes, optional if you only want the popup)
    st.subheader("All Anomalies (Overview)")
    if not issues_df.empty:
        issues_display = issues_df.copy()
        # ... (Your existing formatting logic)
        st.dataframe(issues_display, width=1000, height=300)