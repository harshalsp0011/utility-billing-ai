"""
file_uploader.py
----------------
Handles file uploads for Utility Billing AI Streamlit app.

Sections:
---------
1. Bill File Upload → Stores in raw_documents
2. Tariff File Upload → Updates tariff_rules

Each upload:
    - Saves file to /data/raw
    - Logs metadata in the database
"""

import streamlit as st
from pathlib import Path
from datetime import datetime
from src.database.db_utils import insert_raw_document

def render_file_uploader():
    st.title("File Uploads")

    # -----------------------------
    # Section 1: Bill Upload
    # -----------------------------
    st.subheader("Upload Bill Files")
    st.caption("Upload monthly or quarterly billing statements (PDF or Excel).")

    bill_files = st.file_uploader(
        "Select billing files",
        type=["pdf", "xlsx", "xls"],
        accept_multiple_files=True,
        key="bill_uploader"
    )

    if bill_files:
        save_dir = Path("data/raw")
        save_dir.mkdir(parents=True, exist_ok=True)

        for file in bill_files:
            file_path = save_dir / file.name
            file_path.write_bytes(file.read())

            metadata = {
                "file_name": file.name,
                "file_type": Path(file.name).suffix.lower(),
                "upload_date": datetime.utcnow(),
                "source": "User Upload (Bill)",
                "status": "uploaded"
            }
            try:
                insert_raw_document(metadata)
                st.success(f"Bill file uploaded and logged: {file.name}")
            except Exception as e:
                st.error(f"Error logging bill file {file.name}: {e}")

    # -----------------------------
    # Section 2: Tariff Upload
    # -----------------------------
    st.subheader("Update Tariff Rules")
    st.caption("Upload the latest tariff document for your utility provider.")

    tariff_files = st.file_uploader(
        "Select tariff files",
        type=["pdf"],
        accept_multiple_files=True,
        key="tariff_uploader"
    )

    if tariff_files:
        tariff_dir = Path("data/raw/tariff")
        tariff_dir.mkdir(parents=True, exist_ok=True)

        for file in tariff_files:
            file_path = tariff_dir / file.name
            file_path.write_bytes(file.read())

            metadata = {
                "file_name": file.name,
                "file_type": Path(file.name).suffix.lower(),
                "upload_date": datetime.utcnow(),
                "source": "User Upload (Tariff)",
                "status": "uploaded"
            }
            try:
                insert_raw_document(metadata)
                st.success(f"Tariff file uploaded and logged: {file.name}")
            except Exception as e:
                st.error(f"Error logging tariff file {file.name}: {e}")
