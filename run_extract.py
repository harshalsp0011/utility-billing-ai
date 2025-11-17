#!/usr/bin/env python
"""Extract SC1 tariff data from PDF and save JSON"""

from pathlib import Path
from pypdf import PdfReader
from src.agents.tariff_analysis.sc_llm_extractor import extract_scheme

# Path to the extracted SC1 PDF
pdf_path = Path('data/processed/SC1_raw.pdf')

if not pdf_path.exists():
    print(f"❌ PDF not found: {pdf_path}")
    print("First run extractor_avipsa.py to generate SC1_raw.pdf")
    exit(1)

print(f"📄 Reading {pdf_path}...")
reader = PdfReader(str(pdf_path))

# Extract text from all pages
texts = []
for i, page in enumerate(reader.pages):
    text = page.extract_text() or ""
    texts.append(text)
    print(f"  Page {i+1}: {len(text)} chars")

full_text = "\n".join(texts)
print(f"\n✅ Total extracted: {len(full_text)} characters")

# Call extract_scheme to generate JSON via LLM
print("\n🚀 Calling LLM to extract tariff data...")
try:
    data = extract_scheme('SC1', full_text)
    print(f"✅ Success! Saved SC1.json")
    print(f"   Keys: {list(data.keys())}")
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
