# src/agents/tariff_analysis/extract_pdf.py
"""
Step 1.3 – Extract text and tables from PDF (config-driven).
This creates a machine-readable JSON used by later stages.
"""

import logging
import pdfplumber
import camelot
import json
from pathlib import Path
import sys
import glob

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from src.utils.aws_app import upload_json_to_s3, get_s3_key

# Resolve project root and make input/output paths absolute so the script
# always writes to the repo-level data/processed directory regardless of cwd.
PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Accept PDF path from command line argument, otherwise use default
if len(sys.argv) > 1:
    PDF_PATH = Path(sys.argv[1])
    print(f" Using PDF from argument: {PDF_PATH}")
    logging.info(f" Using PDF from argument: {PDF_PATH}")
else:
    print("No PDF path provided as argument; using default.")
    logging.info(" No PDF path provided as argument; using default.")
    

OUTPUT_PATH = PROJECT_ROOT / Path("data/processed/raw_extracted_tarif.json")

def extract_with_pdfplumber(pdf_path: Path):
    pages_data = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            pages_data.append({
                "page_number": page.page_number,
                "text": text.strip(),
                "tables": []  # placeholder to merge Camelot tables
            })
    return pages_data

def extract_with_pdfplumber(pdf_path: Path, start_page: int = None, end_page: int = None):
    pages_data = []
    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        start_page = start_page or 1
        end_page = end_page or total_pages

        for i in range(start_page - 1, end_page):
            page = pdf.pages[i]
            text = page.extract_text() or ""
            pages_data.append({
                "page_number": page.page_number,
                "text": text.strip(),
                "tables": []
            })
    return pages_data


def extract_tables_with_camelot(pdf_path: Path):
    # Try both 'lattice' (grid lines) and 'stream' (whitespace)
    tables = []
    for flavor in ["lattice", "stream"]:
        try:
            tlist = camelot.read_pdf(str(pdf_path), pages="all", flavor=flavor)
            for t in tlist:
                tables.append({
                    "page": t.page,
                    "data": t.df.values.tolist()
                })
        except Exception as e:
            print(f"Camelot {flavor} failed:", e)
    return tables

def merge_text_and_tables(pages_data, tables):
    for t in tables:
        for p in pages_data:
            if int(p["page_number"]) == int(t["page"]):
                p["tables"].append(t["data"])
    return pages_data

def save_output(data, path: Path):
    # Upload directly to S3 (no local storage)
    s3_key = get_s3_key("processed", path.name)
    if upload_json_to_s3({"pages": data}, s3_key):
        print(f"✅ Uploaded to S3: {s3_key}")
    else:
        raise Exception(f"Failed to upload to S3: {s3_key}")

if __name__ == "__main__":
    # Allow passing a PDF path on the command line: `python extract_pdf.py /path/to/file.pdf`
    cli_pdf = Path(sys.argv[1]) if len(sys.argv) > 1 else None

    if cli_pdf and cli_pdf.exists():
        pdf_to_use = cli_pdf
    elif PDF_PATH.exists():
        pdf_to_use = PDF_PATH
    else:
        # Try to auto-find any PDF under repo-root data/raw/
        candidates = sorted(glob.glob(str(PROJECT_ROOT / "data" / "raw" / "*.pdf")))
        if len(candidates) == 1:
            pdf_to_use = Path(candidates[0])
            print(f"ℹ️  Using found PDF: {pdf_to_use}")
        elif len(candidates) > 1:
            print("❌ No configured PDF found. Multiple PDFs exist under data/raw/; please pass the desired file as an argument or update PDF_PATH.")
            print("Available PDFs:")
            for p in candidates:
                print(" -", p)
            sys.exit(1)
        else:
            print(f"❌ File not found: {PDF_PATH}\nExpected a PDF at that path, or provide one as an argument.\nChecked directory: {PROJECT_ROOT / 'data' / 'raw'}")
            # show files present in data/raw for debugging
            existing = sorted(glob.glob(str(PROJECT_ROOT / "data" / "raw" / "*")))
            if existing:
                print("Files in data/raw/:")
                for p in existing:
                    print(" -", p)
            else:
                print("data/raw/ is empty or missing. Place PDFs there or pass a path to the script.")
            sys.exit(1)

    print("🔍 Extracting text with pdfplumber...")
    pages_data = extract_with_pdfplumber(pdf_to_use)

    print("📊 Extracting tables with Camelot...")
    tables = extract_tables_with_camelot(pdf_to_use)

    print("🧩 Merging results...")
    merged = merge_text_and_tables(pages_data, tables)

    print("💾 Saving structured output...")
    save_output(merged, OUTPUT_PATH)

    print("✅ Done. Proceed to Step 1.4 – Dynamic Section Segmentation.")