"""
pdf_parser.py
--------------
📄 Extracts and structures data from client utility bill PDFs.

Purpose:
--------
This module handles the ingestion of PDF billing statements (from Dominion, National Grid, etc.)
and converts them into structured tables ready for tariff analysis and comparison.

Workflow:
---------
1️⃣ Check if the file exists using `ensure_file_exists()` from utils.helpers.
2️⃣ Use `pdfplumber` or `camelot` to extract tabular data (charges, usage, demand).
3️⃣ Clean header names and numeric values.
4️⃣ Save extracted output as CSV → `data/raw/`.
5️⃣ Insert file metadata into DB via `db_utils.insert_raw_document()`.

Inputs:
-------
- PDF file path (from /data/incoming/)
- Configs from src/utils/config.py

Outputs:
--------
- Extracted CSV (saved under /data/raw/)
- Entry in raw_documents table (file_name, status, upload_date)

Depends On:
-----------
- src.utils.helpers (save_csv, ensure_file_exists, clean_column_names)
- src.utils.logger
- src.database.db_utils
- pdfplumber or camelot
"""

import os
import pdfplumber
import pandas as pd
from datetime import datetime
from src.utils.helpers import save_csv, ensure_file_exists, clean_column_names
from src.utils.logger import get_logger
from src.database.db_utils import insert_raw_document

logger = get_logger(__name__)

def parse_pdf(file_name: str):
    """
    Extracts text and tables from a given PDF billing statement.
    """
    file_path = f"data/incoming/{file_name}"

    # Step 1️⃣: Verify file exists
    if not ensure_file_exists(file_path):
        return None

    # Step 2️⃣: Read PDF with pdfplumber
    try:
        all_tables = []
        with pdfplumber.open(file_path) as pdf:
            for i, page in enumerate(pdf.pages):
                table = page.extract_table()
                if table:
                    df = pd.DataFrame(table[1:], columns=table[0])
                    df = clean_column_names(df)
                    all_tables.append(df)
        if not all_tables:
            logger.warning(f"⚠️ No tables found in {file_name}")
            return None

        result_df = pd.concat(all_tables, ignore_index=True)

        # Step 3️⃣: Save extracted CSV
        out_name = file_name.replace(".pdf", "_extracted.csv")
        save_csv(result_df, "raw", out_name)

        # Step 4️⃣: Insert DB record
        insert_raw_document({
            "file_name": file_name,
            "file_type": "PDF",
            "upload_date": str(datetime.now().date()),
            "source": "Client Upload",
            "status": "processed"
        })

        logger.info(f"✅ Successfully parsed {file_name}")
        return result_df

    except Exception as e:
        logger.error(f"❌ Failed to parse {file_name}: {e}")
        return None
