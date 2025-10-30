"""
excel_loader.py
----------------
📘 Loads and processes Excel usage data for each account.

Purpose:
--------
Handles the ingestion of Excel-based files (like usage histories and rate changes).
Converts sheets into DataFrames, cleans column names, and saves processed CSVs.

Workflow:
---------
1️⃣ Load Excel using `helpers.load_excel()`.
2️⃣ Clean headers via `helpers.clean_column_names()`.
3️⃣ Save output to /data/raw/.
4️⃣ Optionally insert metadata into DB.

Inputs:
-------
- Excel file path (from /data/incoming/ or /data/samples/)
- Sheet name (optional)

Outputs:
--------
- Cleaned CSV in /data/raw/
- DataFrame for next agents

Depends On:
-----------
- src.utils.helpers (load_excel, save_csv, clean_column_names)
- src.utils.logger
- src.database.db_utils
"""

from src.utils.helpers import load_excel, save_csv, clean_column_names
from src.utils.logger import get_logger
from src.database.db_utils import insert_raw_document
from datetime import datetime

logger = get_logger(__name__)

def process_excel(file_name: str, sheet_name=None):
    """
    Reads and cleans Excel-based usage or rate change data.
    """
    df = load_excel("samples", file_name, sheet_name)
    if df.empty:
        logger.warning(f"⚠️ No data found in {file_name}")
        return None

    df = clean_column_names(df)

    out_name = file_name.replace(".xlsx", "_cleaned.csv")
    save_csv(df, "raw", out_name)

    insert_raw_document({
        "file_name": file_name,
        "file_type": "Excel",
        "upload_date": str(datetime.now().date()),
        "source": "Client Upload",
        "status": "processed"
    })

    logger.info(f"✅ Processed Excel file: {file_name}")
    return df
