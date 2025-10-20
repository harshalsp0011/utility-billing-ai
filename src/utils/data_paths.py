"""
data_paths.py
--------------
Centralized file-path management utility.

📍 Purpose:
This module standardizes where all data files are stored or read from.
All agents (DocumentProcessor, TariffAnalysis, etc.) will import these
paths instead of hard-coding directories.

Example:
    from src.utils.data_paths import INCOMING_DIR, get_file_path
"""

import os

# ---------------------------------------------------------------------
# 1️⃣  Define the root 'data' directory relative to the project
# ---------------------------------------------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
DATA_DIR = os.path.join(BASE_DIR, "data")

# ---------------------------------------------------------------------
# 2️⃣  Define sub-directories for different pipeline stages
# ---------------------------------------------------------------------
INCOMING_DIR  = os.path.join(DATA_DIR, "incoming")   # Temporary uploaded files
RAW_DIR       = os.path.join(DATA_DIR, "raw")        # Extracted OCR / parsed data
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")  # Cleaned & structured outputs
SAMPLES_DIR   = os.path.join(DATA_DIR, "samples")    # Demo/reference files
OUTPUT_DIR    = os.path.join(DATA_DIR, "output")     # Final reports, exports, dashboards

# ---------------------------------------------------------------------
# 3️⃣  Ensure all folders exist (creates them automatically if missing)
# ---------------------------------------------------------------------
for folder in [INCOMING_DIR, RAW_DIR, PROCESSED_DIR, SAMPLES_DIR, OUTPUT_DIR]:
    os.makedirs(folder, exist_ok=True)

# ---------------------------------------------------------------------
# 4️⃣  Helper function to build safe file paths
# ---------------------------------------------------------------------
def get_file_path(subdir: str, filename: str) -> str:
    """
    Returns a full path for a given filename inside one of the known sub-folders.
    Example: get_file_path("raw", "VA_Beach_Oct2025.csv")
    """
    folders = {
        "incoming": INCOMING_DIR,
        "raw": RAW_DIR,
        "processed": PROCESSED_DIR,
        "samples": SAMPLES_DIR,
        "output": OUTPUT_DIR,
    }

    if subdir not in folders:
        raise ValueError(f"❌ Invalid subdir '{subdir}'. Must be one of: {list(folders.keys())}")

    return os.path.join(folders[subdir], filename)

# ---------------------------------------------------------------------
# 5️⃣  Diagnostics (run this file directly to verify folder setup)
# ---------------------------------------------------------------------
if __name__ == "__main__":
    print("✅ Data path configuration loaded successfully!\n")
    print(f"Base Directory   : {BASE_DIR}")
    print(f"Data Directory   : {DATA_DIR}")
    print(f"Incoming Folder  : {INCOMING_DIR}")
    print(f"Raw Folder       : {RAW_DIR}")
    print(f"Processed Folder : {PROCESSED_DIR}")
    print(f"Samples Folder   : {SAMPLES_DIR}")
    print(f"Output Folder    : {OUTPUT_DIR}")

    # Example usage demo
    example_path = get_file_path("incoming", "sample_bill.pdf")
    print(f"\nExample file path → {example_path}")
