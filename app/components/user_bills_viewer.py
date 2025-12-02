import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode

# Import your DB utils
# (Ensure these paths match your project structure)
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
    """
    Modal popup showing full bill details plus all related issues.
    Updates:
    1. WIDTH: Adjusted to 85vw with a max-width of 1200px (not full screen).
    2. JSON FIX: Compact HTML strings preventing raw code display.
    3. LAYOUT: Customer Name at top to prevent truncation.
    """

    # ---------------------------------------------------------
    # 1. EXTRACT CRITICAL INFO
    # ---------------------------------------------------------
    customer_name = str(bill_data.get("Customer", "N/A"))
    account_num = str(bill_data.get("Bill Account", "N/A"))
    
    # Remove them from the generic data dictionary
    excluded_keys = {"Customer", "Bill Account", "_has_issue", "id", "Bill ID"}
    grid_data = {k: v for k, v in bill_data.items() if k not in excluded_keys}

    # ---------------------------------------------------------
    # 2. CSS STYLING (UPDATED WIDTH)
    # ---------------------------------------------------------
    st.markdown(
        """
        <style>
        /* 1. ADJUST POPUP WIDTH */
        div[role="dialog"] {
            /* 85% of screen width, but stop growing at 1200px */
            width: 85vw !important;
            max-width: 1200px !important;
        }
        
        /* 2. GRID LAYOUT */
        .details-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
            gap: 12px;
            margin-top: 10px;
        }
        .detail-item {
            background-color: #f9f9f9;
            border: 1px solid #eee;
            border-radius: 6px;
            padding: 8px 10px;
        }
        .detail-key {
            display: block;
            font-size: 0.7rem;
            text-transform: uppercase;
            color: #666;
            font-weight: 700;
            margin-bottom: 2px;
        }
        .detail-val {
            display: block;
            font-size: 0.9rem;
            color: #111;
            font-weight: 500;
            word-wrap: break-word;
        }
        .issue-card {
            border: 1px solid #ffdddd; 
            padding: 10px; 
            border-radius: 6px; 
            background: rgba(255,0,0,0.05); 
            margin-bottom: 8px;
        }
        </style>
        """, 
        unsafe_allow_html=True
    )

    # ---------------------------------------------------------
    # 3. TOP HEADER
    # ---------------------------------------------------------
    st.markdown("### 🧾 Bill Summary")
    
    col1, col2 = st.columns([3, 1]) 
    with col1:
        st.caption("Customer Name")
        st.markdown(f"## 🏢 {customer_name}") 
    with col2:
        st.caption("Account Number")
        st.markdown(f"**{account_num}**")

    st.divider()

    # ---------------------------------------------------------
    # 4. DETAILS GRID (Compact HTML Generation)
    # ---------------------------------------------------------
    st.markdown("#### 🔍 Record Details")

    # Use compact HTML strings (no indentation inside f-strings) 
    # to ensure Streamlit renders them as HTML, not code.
    html_parts = []
    html_parts.append("<div class='details-grid'>")
    
    for k, v in grid_data.items():
        val_display = str(v) if v not in (None, "") else "-"
        
        # Clean up 'T' in dates
        if "T" in val_display and len(val_display) > 10 and any(c.isdigit() for c in val_display):
             try: val_display = val_display.split("T")[0]
             except: pass
        
        # Single line f-string to prevent indentation errors
        item_html = f"<div class='detail-item'><span class='detail-key'>{k}</span><span class='detail-val'>{val_display}</span></div>"
        html_parts.append(item_html)
    
    html_parts.append("</div>")
    
    # Render final string
    st.markdown("".join(html_parts), unsafe_allow_html=True)

    st.divider()

    # ---------------------------------------------------------
    # 5. ISSUES LIST
    # ---------------------------------------------------------
    st.markdown("#### ⚠️ Detected Issues")
    if issues_data.empty:
        st.success("No issues found.")
    else:
        with st.container(height=350):
            for _, row in issues_data.iterrows():
                issue_type = row.get('issue_type','Unknown')
                description = row.get('description','No description')
                status = row.get('status','N/A')
                st.markdown(
                    f"<div class='issue-card'><strong>{issue_type}</strong><br/>{description}<br/><small>Status: {status}</small></div>",
                    unsafe_allow_html=True,
                )

# ---------------------------------------------------------
# Column mappings
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

    # 2. Configure Selection
    gb.configure_selection(
        selection_mode="single",
        use_checkbox=False,
        pre_selected_rows=[]
    )
    
    grid_options = gb.build()

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

    grid_response = AgGrid(
        display_df,
        gridOptions=grid_options,
        custom_css=custom_css,
        allow_unsafe_jscode=True,
        fit_columns_on_grid_load=True,
        theme="streamlit",
        height=550,
        update_mode=GridUpdateMode.SELECTION_CHANGED, 
        data_return_mode=DataReturnMode.FILTERED_AND_SORTED
    )

    # ===========================
    # POPUP LOGIC
    # ===========================
    selected_rows = grid_response['selected_rows']
    
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
                original_rows = bills_df[bills_df['id'] == selected_bill_id_int]
                
                if not original_rows.empty:
                    original_row = original_rows.iloc[0].to_dict()
                    full_bill_dict = {BILL_COLUMN_RENAMES.get(k, k): v for k, v in original_row.items()}
                    
                    if 'customer' in original_row:
                        full_bill_dict['Customer'] = original_row['customer']
                        
                    for date_key in ("Bill Date", "Read Date", "Uploaded At"):
                        if date_key in full_bill_dict and pd.notna(full_bill_dict[date_key]):
                            try:
                                full_bill_dict[date_key] = pd.to_datetime(full_bill_dict[date_key]).strftime('%Y-%m-%d')
                            except Exception:
                                pass
                else:
                    full_bill_dict = selected_row_dict

                relevant_issues = issues_df[issues_df['bill_id'] == selected_bill_id_int]
                show_anomaly_popup(selected_bill_id_int, full_bill_dict, relevant_issues)
                
            except Exception as e:
                st.error(f"Error loading details: {e}")

    # ===========================
    # Issues Table (Overview)
    # ===========================
    st.subheader("All Anomalies (Overview)")
    if not issues_df.empty:
        issues_display = issues_df.copy()
        # UPDATED: Replaced use_container_width=True with width="stretch" per warning
        st.dataframe(issues_display, width="stretch")