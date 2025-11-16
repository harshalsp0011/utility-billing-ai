"""
filter_schemes.py
-----------------------------------------------
Step 1: Clean classification of each page into:

 - SC1
 - SC1C
 - SC2
 - SC2D
 - SC3
 - SC3A

No rate extraction here.
This is ONLY scheme filtering.
"""

import json
import re
from pathlib import Path


# ============================================================
# PATH TO RAW JSON
# ============================================================

RAW_JSON = Path(r"D:\utility-billing-ai\data\processed\raw_extracted_tarif.json")

with open(RAW_JSON, "r", encoding="utf-8") as f:
    raw = json.load(f)

pages = raw["pages"]


# ============================================================
# REGEX PATTERNS FOR DETECTION
# ============================================================

SC1_RE   = re.compile(r"SERVICE\s+CLASSIFICATION\s+NO\.?\s*1(\D|$)", re.I)
SC1C_RE  = re.compile(r"SERVICE\s+CLASSIFICATION\s+NO\.?\s*1[\-\s]?C\b", re.I)

SC2_HEAD_RE  = re.compile(r"SERVICE\s+CLASSIFICATION\s+NO\.?\s*2(\D|$)", re.I)
SC2_ND_RE    = re.compile(r"METERED\s+NON[- ]?DEMAND\s+SERVICE", re.I)
SC2_D_RE     = re.compile(r"METERED\s+DEMAND\s+SERVICE", re.I)

# REAL SC2D pattern found inside your PDF
SC2D_RE = re.compile(r"\bSC[\s\-]?2D\b|\b2[\s\-]?D\b", re.I)

SC3_HEAD_RE  = re.compile(r"SERVICE\s+CLASSIFICATION\s+NO\.?\s*3(\D|$)", re.I)
SC3A_RE      = re.compile(r"SERVICE\s+CLASSIFICATION\s+NO\.?\s*3[\-\s]?A\b", re.I)


# ============================================================
# CLASSIFIER
# ============================================================

def classify_page(text):
    # SC1C before SC1
    if SC1C_RE.search(text):
        return "SC1C"
    if SC1_RE.search(text):
        return "SC1"

    # SC2D (detected anywhere)
    if SC2D_RE.search(text):
        return "SC2D"

    # SC2 header
    if SC2_HEAD_RE.search(text):
        # Look for subsections
        if SC2_D_RE.search(text):
            return "SC2D"  # Metered Demand belongs to SC2D
        if SC2_ND_RE.search(text):
            return "SC2"   # SC2 Non-Demand
        return "SC2"       # fallback

    # SC3A must be checked before SC3
    if SC3A_RE.search(text):
        return "SC3A"

    # SC3
    if SC3_HEAD_RE.search(text):
        return "SC3"

    return None


# ============================================================
# RUN CLASSIFICATION
# ============================================================

schemes = {
    "SC1": [],
    "SC1C": [],
    "SC2": [],
    "SC2D": [],
    "SC3": [],
    "SC3A": []
}

for p in pages:
    pg = p["page_number"]
    text = p["text"]

    sc = classify_page(text)

    # store page if classified
    if sc:
        schemes[sc].append(pg)


# ============================================================
# PRINT RESULTS
# ============================================================

print("\n======= FILTERED SCHEMES =======\n")
for sc, pg in schemes.items():
    print(f"{sc}: {pg}")

print("\n================================\n")
