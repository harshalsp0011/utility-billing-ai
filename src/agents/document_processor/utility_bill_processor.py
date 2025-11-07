# ------------------------------------------------------------------
# Unified Data Ingestion and Cleaning Toolkit
# Handles Excel loading, PDF table extraction, and text/table cleaning.
# ------------------------------------------------------------------

import os
import pdfplumber
import pandas as pd
from datetime import datetime

# Import helper functions from your utility modules
from src.utils.helpers import load_excel, save_csv, clean_column_names, ensure_file_exists
from src.utils.logger import get_logger
from src.database.db_utils import insert_raw_document

# Set up logger for progress and error tracking
logger = get_logger(__name__)

# -----------------------------------------------------------------------------
# 1) TABLE/TEXT CLEANING LOGIC
# -----------------------------------------------------------------------------
def clean_text_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    🧹 Cleans a DataFrame by standardizing formatting:
      - Removes leading/trailing whitespace.
      - Strips out currency symbols and commas.
      - Consolidates extra spaces.
    Used after reading Excel or PDF tables.
    """
    try:
        # Strip whitespace from all string cells
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        for col in df.columns:
            if df[col].dtype == object:
                # Remove $, commas, and fix space formatting
                df[col] = df[col].replace(r"[\$,]", "", regex=True)
                df[col] = df[col].replace(r"\s+", " ", regex=True)
        logger.info(f"🧽 Cleaned text fields in DataFrame (Cols: {len(df.columns)})")
        return df
    except Exception as e:
        logger.error(f"❌ Error cleaning text data: {e}")
        return df

# -----------------------------------------------------------------------------
# 2) EXCEL LOADING, CLEANING, AND DB ENTRY
# -----------------------------------------------------------------------------
def process_excel(file_name: str, sheet_name=None):
    """
    📘 Loads and processes Excel usage data:
      - Reads Excel file from samples directory with optional sheet.
      - Cleans column names for consistency.
      - Cleans textual/table artifacts.
      - Saves cleaned output as CSV in raw data folder.
      - Records metadata in DB.
    Returns cleaned DataFrame if successful.
    """
    df = load_excel("samples", file_name, sheet_name)
    if df.empty:
        logger.warning(f"⚠️ No data found in {file_name}")
        return None

    # Clean headers then tabular/text artifacts
    df = clean_column_names(df)
    df = clean_text_data(df)

    # Save output as CSV
    out_name = file_name.replace(".xlsx", "_cleaned.csv")
    save_csv(df, "raw", out_name)

    # Insert record in database
    insert_raw_document({
        "file_name": file_name,
        "file_type": "Excel",
        "upload_date": str(datetime.now().date()),
        "source": "Client Upload",
        "status": "processed"
    })

    logger.info(f"✅ Processed Excel file: {file_name}")
    return df

# -----------------------------------------------------------------------------
# 3) PDF PARSING (TABLE EXTRACTION), CLEANING, AND DB ENTRY
# -----------------------------------------------------------------------------
def parse_pdf(file_name: str):
    """
    📄 Parses PDF bills:
      - Checks the PDF exists in incoming directory.
      - Extracts tables from each page using pdfplumber.
      - Cleans column names and table/text artifacts.
      - Concatenates all tables, saves as CSV in raw folder.
      - Inserts operation metadata in DB.
    Returns merged, cleaned DataFrame if successful.
    """
    file_path = f"data/incoming/{file_name}"

    # Step 1: Verify file exists
    if not ensure_file_exists(file_path):
        logger.warning(f"⚠️ File {file_name} does not exist.")
        return None

    all_tables = []
    try:
        with pdfplumber.open(file_path) as pdf:
            for i, page in enumerate(pdf.pages):
                # Step 2: Extract table if present on this page
                table = page.extract_table()
                if table:
                    # Step 3: Convert table to DataFrame, clean headers, and text
                    df = pd.DataFrame(table[1:], columns=table[0])
                    df = clean_column_names(df)
                    df = clean_text_data(df)
                    all_tables.append(df)

        if not all_tables:
            logger.warning(f"⚠️ No tables found in {file_name}")
            return None

        # Step 4: Concatenate all extracted tables into single DataFrame
        result_df = pd.concat(all_tables, ignore_index=True)

        # Step 5: Save extracted and cleaned table as CSV
        out_name = file_name.replace(".pdf", "_extracted.csv")
        save_csv(result_df, "raw", out_name)

        # Step 6: Insert operation metadata into DB
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

# ------------------------------------------------------------------
# End of unified data pipeline code
# ------------------------------------------------------------------
