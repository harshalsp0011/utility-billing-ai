#!/usr/bin/env python
import sys
import traceback

print("Testing imports step-by-step...\n")

steps = [
    ("os", "import os"),
    ("json", "import json"),
    ("pathlib.Path", "from pathlib import Path"),
    ("pypdf", "from pypdf import PdfReader, PdfWriter"),
    ("llm_client", "from src.llm_client import llm"),
]

for name, code in steps:
    try:
        exec(code)
        print(f"✓ {name}")
    except Exception as e:
        print(f"✗ {name}: {type(e).__name__}: {e}")
        traceback.print_exc()
        break

print("\n--- Now trying full module import ---")
try:
    import src.agents.tariff_analysis.extractor_avipsa as ea
    print(f"✓ Module loaded")
    # Get all non-magic attributes
    attrs = [x for x in dir(ea) if not x.startswith('_')]
    print(f"Public attributes: {attrs}")
except Exception as e:
    print(f"✗ Module import failed: {type(e).__name__}: {e}")
    traceback.print_exc()
