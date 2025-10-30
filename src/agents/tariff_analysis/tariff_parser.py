"""
tariff_parser.py
----------------
📜 Parses raw tariff PDFs into extracted text and table sections.

Purpose:
--------
This module reads official tariff rulebooks (PDFs) and extracts
plain text + structured rate tables for further analysis.

Workflow:
---------
1️⃣ Verify PDF file exists.
2️⃣ Extract text using `pdfplumber`.
3️⃣ Split text by rate schedule headers (e.g., 'Rate Schedule VE-100').
4️⃣ Optionally extract tables using `camelot`.
5️⃣ Save extracted text to /data/raw/ for auditing.

Inputs:
-------
- Tariff PDF file (e.g., VEPGA_Tariff_Virginia.pdf)

Outputs:
--------
- Raw text file: /data/raw/VEPGA_Tariff_Text.txt
- Optional CSVs: /data/raw/VEPGA_Tariff_Tables.csv

Depends On:
-----------
- pdfplumber
- src.utils.helpers
- src.utils.logger
"""

import os
import pdfplumber
from src.utils.helpers import ensure_file_exists, save_csv
from src.utils.logger import get_logger

logger = get_logger(__name__)

def parse_tariff_pdf(file_name: str):
    """
    Extracts full text and tables from a tariff PDF.
    """
    file_path = f"data/incoming/{file_name}"

    if not ensure_file_exists(file_path):
        return None

    try:
        all_text = ""
        all_tables = []

        with pdfplumber.open(file_path) as pdf:
            for i, page in enumerate(pdf.pages):
                text = page.extract_text() or ""
                all_text += f"\n--- PAGE {i+1} ---\n{text}"
                table = page.extract_table()
                if table:
                    all_tables.append(table)

        # Save raw text for reference
        out_txt = f"data/raw/{file_name.replace('.pdf', '_Text.txt')}"
        with open(out_txt, "w", encoding="utf-8") as f:
            f.write(all_text)

        logger.info(f"📄 Tariff text extracted to {out_txt}")
        return all_text, all_tables

    except Exception as e:
        logger.error(f"❌ Error parsing tariff PDF {file_name}: {e}")
        return None, None
    