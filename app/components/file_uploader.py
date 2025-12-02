import streamlit as st
from pathlib import Path
from datetime import datetime
import pandas as pd

from src.database.db_utils import insert_raw_bill_document
from src.agents.document_processor_agent.utility_bill_doc_processor import process_bill


def render_file_uploader():
    st.title("📤 File Upload Management")

    # Tab navigation for separate sections
    tab1, tab2 = st.tabs(["📄 Bill Documents", "⚡ Tariff Documents"])

    # -----------------------------
    # TAB 1: Bill Upload
    # -----------------------------
    with tab1:
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

            # Log upload in DB
            metadata = {
                "file_name": file.name,
                "file_type": Path(file.name).suffix.lower(),
                "upload_date": datetime.utcnow(),
                "source": "User Upload (Bill)",
                "status": "uploaded"
            }

            try:
                insert_raw_bill_document(metadata)
                st.success(f"Bill file uploaded and logged: {file.name}")
            except Exception as e:
                st.error(f"Error logging bill file {file.name}: {e}")

            # -------------------------
            # 🔥 AUTO-PROCESS THE FILE
            # -------------------------
            try:
                st.info(f"Processing: {file.name} ...")
                df = process_bill(file_path)

                st.success(f"Processed successfully → {len(df)} rows extracted.")
                st.dataframe(df)

            except Exception as e:
                st.error(f"❌ Failed to process {file.name}: {e}")

    # -----------------------------
    # TAB 2: Tariff Upload
    # -----------------------------
    with tab2:
        st.subheader("Upload Tariff Documents")
        st.caption("Upload the latest tariff document for your utility provider (PDF only).")

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
                insert_raw_bill_document(metadata)
                st.success(f"✅ Tariff file uploaded and logged: {file.name}")
            except Exception as e:
                st.error(f"❌ Error logging tariff file {file.name}: {e}")
        
        st.info("💡 Tip: After uploading tariff documents, navigate to the Tariff Analysis section to extract and analyze rate structures.")
