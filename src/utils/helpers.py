"""
helpers.py
-----------
🧰 Common utility functions for file handling, reading, and saving data.

Purpose:
--------
Centralized helper methods used by all agents and modules.
Includes:
- Safe file reading/writing
- JSON and CSV utilities
- Basic cleaning functions
- Logging integration

Dependencies:
-------------
- pandas
- os
- json
- src.utils.data_paths
- src.utils.logger

Usage Example:
--------------
from src.utils.helpers import load_csv, save_csv
df = load_csv("raw", "example_data.csv")
save_csv(df, "processed", "clean_data.csv")
"""

import os
import json
import pandas as pd
from src.utils.data_paths import get_file_path
from src.utils.logger import get_logger

logger = get_logger(__name__)

# ----------------------------------------------------------------------
# 1️⃣ CSV / Excel File Handlers
# ----------------------------------------------------------------------
def load_csv(subdir: str, filename: str) -> pd.DataFrame:
    """
    Loads a CSV file from a given data subdirectory (raw, processed, etc.)
    """
    file_path = get_file_path(subdir, filename)
    try:
        df = pd.read_csv(file_path)
        logger.info(f"📄 Loaded CSV file: {file_path} | Rows: {len(df)}")
        return df
    except Exception as e:
        logger.error(f"❌ Failed to load CSV {filename}: {e}")
        return pd.DataFrame()

def save_csv(df: pd.DataFrame, subdir: str, filename: str):
    """
    Saves a pandas DataFrame as CSV to a data subdirectory.
    """
    file_path = get_file_path(subdir, filename)
    try:
        df.to_csv(file_path, index=False)
        logger.info(f"💾 Saved CSV file: {file_path} | Rows: {len(df)}")
    except Exception as e:
        logger.error(f"❌ Failed to save CSV {filename}: {e}")

def load_excel(subdir: str, filename: str, sheet_name=None) -> pd.DataFrame:
    """
    Loads an Excel file safely using pandas.
    """
    file_path = get_file_path(subdir, filename)
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        logger.info(f"📘 Loaded Excel file: {file_path} | Rows: {len(df)}")
        return df
    except Exception as e:
        logger.error(f"❌ Failed to load Excel {filename}: {e}")
        return pd.DataFrame()

# ----------------------------------------------------------------------
# 2️⃣ JSON File Handlers
# ----------------------------------------------------------------------
def read_json(subdir: str, filename: str):
    """
    Reads a JSON file and returns a Python dictionary.
    """
    file_path = get_file_path(subdir, filename)
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"📜 Loaded JSON file: {file_path}")
        return data
    except Exception as e:
        logger.error(f"❌ Failed to read JSON {filename}: {e}")
        return {}

def save_json(data: dict, subdir: str, filename: str):
    """
    Saves a dictionary as a JSON file.
    """
    file_path = get_file_path(subdir, filename)
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        logger.info(f"💾 JSON saved successfully: {file_path}")
    except Exception as e:
        logger.error(f"❌ Failed to save JSON {filename}: {e}")

# ----------------------------------------------------------------------
# 3️⃣ General Utilities
# ----------------------------------------------------------------------
def ensure_file_exists(file_path: str) -> bool:
    """
    Checks if a given file exists and logs result.
    """
    if os.path.exists(file_path):
        logger.info(f"✅ File found: {file_path}")
        return True
    else:
        logger.warning(f"⚠️ File not found: {file_path}")
        return False

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans column names (strips whitespace, lowercases, replaces spaces).
    """
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    logger.info(f"🧹 Cleaned column names for DataFrame.")
    return df

# ----------------------------------------------------------------------
# 4️⃣ Self-Test
# ----------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("✅ helpers.py self-test started.")
    test_data = {"city": "VA Beach", "rate": 0.15}
    save_json(test_data, "output", "test_helpers.json")
    read_json("output", "test_helpers.json")
    logger.info("✅ helpers.py self-test completed.")
