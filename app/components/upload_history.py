import streamlit as st
import pandas as pd
from pathlib import Path

from src.database.db_utils import fetch_all_raw_bill_docs
from src.utils.aws_app import (
    get_s3_key,
    file_exists_in_s3,
    list_files_in_s3_with_meta,
)


def render_upload_history():
    """Render a simple upload history table for previously uploaded documents."""
    st.title("📜 Upload History")
    st.caption("Review previously uploaded documents")

    try:
        raw_docs = fetch_all_raw_bill_docs()

        # -------------------------
        # Table 1: DB records + S3 status
        # -------------------------
        if raw_docs:
            rows = []
            db_keys = set()

            for doc in raw_docs:
                s3_key = get_s3_key("raw", doc.file_name)
                exists = file_exists_in_s3(s3_key)
                db_keys.add(s3_key)

                rows.append({
                    "File Name": doc.file_name,
                    "Source": doc.source,
                    "Upload Date": doc.upload_date.strftime("%Y-%m-%d %H:%M") if doc.upload_date else "N/A",
                    "S3 Exists": "✅" if exists else "❌",
                })

            st.markdown("### 📄 Database Records")
            df = pd.DataFrame(rows)
            st.dataframe(df, width='stretch', hide_index=True)
        else:
            st.info("📭 No database records found")

        # -------------------------
        # Table 2: S3-only files (not in DB)
        # -------------------------
        s3_items = list_files_in_s3_with_meta("data/raw/")
        if s3_items:
            db_keys = db_keys if 'db_keys' in locals() else set()
            orphan_items = [item for item in s3_items if item.get("Key") not in db_keys]

            if orphan_items:
                st.markdown("### 🗂️ S3 Files Not In Database")
                orphan_rows = []
                for item in orphan_items:
                    key = item.get("Key")
                    last_modified = item.get("LastModified")
                    file_name = Path(key).name if key else ""
                    orphan_rows.append({
                        "File Name": file_name,
                        "Upload Date": last_modified.strftime("%Y-%m-%d %H:%M") if last_modified else "N/A",
                        "S3 Exists": "✅",
                    })
                df_orphan = pd.DataFrame(orphan_rows)
                st.dataframe(df_orphan, width='stretch', hide_index=True)
        
        if (not raw_docs) and (not s3_items):
            st.info("📭 No documents uploaded yet")

    except Exception as exc:
        st.error(f"Unable to load upload history: {exc}")
