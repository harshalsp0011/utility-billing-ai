"""
filter_and_extract_schemes.py
-----------------------------------------------------------
UPDATED VERSION — UNIVERSAL SECTION EXTRACTOR

STEP 1 — Classify pages by Service Classification:
   - SC1
   - SC1C
   - SC2
   - SC2D
   - SC3
   - SC3A

STEP 2 — Combine all pages for each SC into one block

STEP 3 — Extract ALL SECTIONS dynamically:
   - APPLICABLE
   - APPLICATION FOR SERVICE
   - CHARACTER OF SERVICE
   - MONTHLY RATE
   - MONTHLY MINIMUM CHARGE
   - DETERMINATION OF DEMAND
   - SPECIAL PROVISIONS
   - ADJUSTMENTS
   - TERMS OF PAYMENT
   - TERM
   - etc.

OUTPUT:
   data/processed/scheme_pages.json
   data/processed/sc_text_blocks.json
"""

import json
import re
import sys
from pathlib import Path

# Ensure the repository root is on sys.path for `from src...` imports
repo_root = Path(__file__).resolve().parents[3]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))


# =====================================================================
# PATHS (Repo-root relative)
# =====================================================================

RAW_JSON_PATH = repo_root / "data" / "processed" / "raw_extracted_tarif.json"
SCHEME_PAGES_OUT = repo_root / "data" / "processed" / "harshal_scheme_pages.json"
SC_TEXT_BLOCKS_OUT = repo_root / "data" / "processed" / "harshal_sc.json"


# =====================================================================
# PAGE CLASSIFICATION (FIXED)
# =====================================================================

# IMPORTANT FIX: SC3A must be matched BEFORE SC3
SC3A_RE = re.compile(r"SERVICE CLASSIFICATION NO\.?\s*3A\b", re.I)
SC3_RE  = re.compile(r"SERVICE CLASSIFICATION NO\.?\s*3(\s|$)(?!A)", re.I)

SC1C_RE = re.compile(r"SERVICE CLASSIFICATION NO\.?\s*1[\-\s]?C\b", re.I)
SC1_RE  = re.compile(r"SERVICE CLASSIFICATION NO\.?\s*1(\D|$)", re.I)

SC2_HEAD_RE = re.compile(r"SERVICE CLASSIFICATION NO\.?\s*2(\D|$)", re.I)
SC2_D_RE    = re.compile(r"METERED\s+DEMAND\s+SERVICE", re.I)
SC2_ND_RE   = re.compile(r"METERED\s+NON[- ]?DEMAND\s+SERVICE", re.I)
SC2D_RE     = re.compile(r"\bSC[\s\-]?2D\b|\b2[\s\-]?D\b", re.I)


def classify_page(text: str):
    """Identify the service classification for a given PDF page."""

    # PRIORITY: SC3A → SC3
    if SC3A_RE.search(text):
        return "SC3A"
    if SC3_RE.search(text):
        return "SC3"

    if SC1C_RE.search(text):
        return "SC1C"
    if SC1_RE.search(text):
        return "SC1"

    if SC2D_RE.search(text):
        return "SC2D"

    if SC2_HEAD_RE.search(text):
        if SC2_D_RE.search(text):
            return "SC2D"
        if SC2_ND_RE.search(text):
            return "SC2"
        return "SC2"

    return None


# =====================================================================
# SECTION HEADER DEFINITIONS (UNIVERSAL FOR ALL SC)
# =====================================================================

SECTION_HEADERS = [
    r"APPLICABLE TO USE OF SERVICE FOR",
    r"APPLICATION FOR SERVICE",
    r"CHARACTER OF SERVICE",
    r"MONTHLY RATE",
    r"MONTHLY MINIMUM CHARGE",
    r"DETERMINATION OF DEMAND",
    r"SPECIAL PROVISIONS",
    r"ADJUSTMENTS TO STANDARD RATES",
    r"ADJUSTMENTS TO STANDARD RATES AND CHARGES",
    r"INCREASE IN RATES",
    r"TERMS OF PAYMENT",
    r"TERM",
    r"HIGH VOLTAGE DELIVERY"
]

SECTION_HEADERS_RE = re.compile("|".join(SECTION_HEADERS), re.I)


# =====================================================================
# SECTION EXTRACTOR (NEW)
# =====================================================================

def extract_sections(full_text: str):
    """
    Extracts all SC sections dynamically.
    Returns: { "SECTION NAME": "text block", ... }
    """

    sections = {}
    current_header = None
    buffer = []

    for line in full_text.splitlines():
        line_stripped = line.strip()

        if not line_stripped:
            continue

        header_match = SECTION_HEADERS_RE.search(line_stripped)

        if header_match:
            # save previous section
            if current_header:
                sections[current_header] = "\n".join(buffer).strip()

            # new section start
            current_header = header_match.group(0)
            buffer = []
        else:
            if current_header:
                buffer.append(line_stripped)

    # Save last section
    if current_header:
        sections[current_header] = "\n".join(buffer).strip()

    return sections


# =====================================================================
# MAIN SCRIPT — UPDATED
# =====================================================================

def main():

    # Load raw JSON
    with open(RAW_JSON_PATH, "r", encoding="utf-8") as f:
        raw = json.load(f)

    pages = raw["pages"]

    # Classify pages
    scheme_pages = {sc: [] for sc in ["SC1", "SC1C", "SC2", "SC2D", "SC3", "SC3A"]}

    for p in pages:
        sc = classify_page(p["text"])
        if sc:
            scheme_pages[sc].append(p["page_number"])

    # Save mapping
    with open(SCHEME_PAGES_OUT, "w", encoding="utf-8") as f:
        json.dump(scheme_pages, f, indent=2)

    print(f"✔ Saved scheme pages → {SCHEME_PAGES_OUT}")

    # Build page lookup table
    page_lookup = {p["page_number"]: p["text"] for p in pages}

    final = {}

    # For each SC combine pages + extract sections
    for scheme, pg_list in scheme_pages.items():

        if not pg_list:
            continue

        # Combine text from all pages
        combined_text = "\n".join(page_lookup[pg] for pg in sorted(pg_list))

        # Extract structured sections
        sections = extract_sections(combined_text)

        final[scheme] = {
            "pages": pg_list,
            "combined_text": combined_text,
            "sections": sections
        }

    # Save final SC text blocks
    with open(SC_TEXT_BLOCKS_OUT, "w", encoding="utf-8") as f:
        json.dump(final, f, indent=2)

    print(f"✔ Saved structured SC text blocks → {SC_TEXT_BLOCKS_OUT}")


# RUN
if __name__ == "__main__":
    main()
