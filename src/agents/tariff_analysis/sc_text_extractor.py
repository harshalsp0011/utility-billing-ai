"""
filter_and_extract_schemes.py
-----------------------------------------------------------
STEP 1 — Classify pages by Service Classification:
   - SC1
   - SC1C
   - SC2 (Non-Demand)
   - SC2D (Demand-Metered)
   - SC3
   - SC3A

STEP 2 — Extract applicable text + valid rate lines for each SC

OUTPUTS:
   data/processed/scheme_pages.json
   data/processed/sc_text_blocks.json
"""

import json
import re
from pathlib import Path


# =====================================================================
# PATHS
# =====================================================================

RAW_JSON_PATH = Path(r"D:\utility-billing-ai\data\processed\raw_extracted_tarif.json")
SCHEME_PAGES_OUT = Path(r"D:\utility-billing-ai\data\processed\scheme_pages.json")
SC_TEXT_BLOCKS_OUT = Path(r"D:\utility-billing-ai\data\processed\sc_text_blocks.json")


# =====================================================================
# STEP 1 — PAGE CLASSIFICATION
# =====================================================================

SC1C_RE = re.compile(r"SERVICE CLASSIFICATION NO\.?\s*1[\-\s]?C\b", re.I)
SC1_RE  = re.compile(r"SERVICE CLASSIFICATION NO\.?\s*1(\D|$)", re.I)

SC2_HEAD_RE = re.compile(r"SERVICE CLASSIFICATION NO\.?\s*2(\D|$)", re.I)
SC2_ND_RE   = re.compile(r"METERED\s+NON[- ]?DEMAND\s+SERVICE", re.I)
SC2_D_RE    = re.compile(r"METERED\s+DEMAND\s+SERVICE", re.I)

SC2D_RE = re.compile(r"\bSC[\s\-]?2D\b|\b2[\s\-]?D\b", re.I)

SC3A_RE = re.compile(r"SERVICE CLASSIFICATION NO\.?\s*3[\-\s]?A\b", re.I)
SC3_RE  = re.compile(r"SERVICE CLASSIFICATION NO\.?\s*3(\D|$)", re.I)


def classify_page(text: str):
    """Identify the service classification for a given PDF page."""
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

    if SC3A_RE.search(text):
        return "SC3A"

    if SC3_RE.search(text):
        return "SC3"

    return None


# =====================================================================
# Utility Functions
# =====================================================================

def clean_lines(text: str):
    """Split PDF text into clean, non-empty lines."""
    out = []
    for ln in text.splitlines():
        ln = ln.strip()
        if ln:
            out.append(ln)
    return out


DOLLAR = r"\$[0-9,]+\.[0-9]{2}"
DOLLAR_RE = re.compile(DOLLAR)

SC3_OR_3A_VOLTAGE = r"(0-2\.2 kV|Up to 2\.2 kV|2\.2-15 kV|22-50 kV|Over 60 kV|Over 60kV)"


def _parse_dollars(line):
    """Return list of numeric dollar values in a line."""
    vals = []
    for m in DOLLAR_RE.findall(line):
        vals.append(float(m.replace("$", "").replace(",", "")))
    return vals


# =====================================================================
# RATE FILTERS PER SCHEME
# =====================================================================

SC1_RATE_RE = re.compile(
    fr"(Basic Service Charge.*{DOLLAR})"
    fr"|(.+kWh.+{DOLLAR})"
    fr"|((MONTHLY MINIMUM CHARGE).+{DOLLAR})",
    re.I
)

SC1C_RATE_RE = SC1_RATE_RE

SC2_RATE_RE = re.compile(
    fr"(Basic Service Charge.*{DOLLAR})"
    fr"|(Special Provision O.*{DOLLAR})"
    fr"|(.+kWh.+{DOLLAR})"
    fr"|((MONTHLY MINIMUM CHARGE).+{DOLLAR})",
    re.I
)

SC2D_RATE_RE = re.compile(
    fr"(Basic Service Charge.*{DOLLAR})"
    fr"|(Special Provision P.*{DOLLAR})"
    fr"|(.+kW.+{DOLLAR})"
    fr"|((MONTHLY MINIMUM CHARGE).+{DOLLAR})",
    re.I
)

def extract_sc2_rates(lines):
    """
    SC2 (Non-Demand):
    - Basic Service Charge
    - Per kWh distribution delivery
    - Special Provision O
    - Monthly minimum charge

    DO NOT KEEP:
        - On-Peak
        - Off-Peak
        - Super-Peak
        - Any TOU table rows
    """

    out = []

    for ln in lines:
        low = ln.lower()

        # Skip any TOU labels
        if ("on-peak" in low or "off-peak" in low or "super-peak" in low):
            continue

        # Skip TOU numeric rows (0.xx, 0.1xx)
        dollars = _parse_dollars(ln)
        if dollars and max(dollars) < 1.0:
            continue

        # Basic Service Charge
        if "basic service charge" in low and DOLLAR_RE.search(ln):
            out.append(ln)
            continue

        # Per kWh distribution charge
        if "kwh" in low and DOLLAR_RE.search(ln):
            out.append(ln)
            continue

        # Special Provision O rates
        if "special provision o" in low and DOLLAR_RE.search(ln):
            out.append(ln)
            continue

        # Monthly minimum charge
        if "monthly minimum" in low and DOLLAR_RE.search(ln):
            out.append(ln)
            continue

    # Deduplicate
    seen, cleaned = set(), []
    for ln in out:
        if ln not in seen:
            seen.add(ln)
            cleaned.append(ln)

    return cleaned


# =====================================================================
# SC3 RATE EXTRACTION
# =====================================================================

def extract_sc3_rates(lines):
    """
    SC3 (Demand-Metered, 100 kW+)
    Extract the EXACT items in your screenshot:

    - Customer Charge (Distribution Delivery)
    - Special Provision L & N (extra customer charges)
    - Minimum Demand Charges (First 40 kW)
    - Additional Demand Charges (Over 40 kW)
    - Reactive Demand Charge
    """

    out = []

    for ln in lines:
        low = ln.lower()
        dollars = _parse_dollars(ln)

        # Skip TOU or irrelevant decimal rows (On/Off/Super Peak)
        if "on peak" in low or "off peak" in low or "super peak" in low:
            continue

        if dollars and max(dollars) < 1.0:
            continue  # removes 0.0x rows entirely

        # ------------------------------
        # 1. CUSTOMER CHARGE (by voltage)
        # ------------------------------
        if ("distribution delivery" in low or "customer charge" in low) and DOLLAR_RE.search(ln):
            out.append(ln)
            continue

        # Special Provision L / N rows
        if "special provision" in low and DOLLAR_RE.search(ln):
            out.append(ln)
            continue

        # ------------------------------
        # 2. MINIMUM DEMAND CHARGES
        # ------------------------------
        if "minimum demand" in low and DOLLAR_RE.search(ln):
            out.append(ln)
            continue

        # Voltage-tier minimum demand row:
        if ("0-2.2" in low or "2.2-15" in low or "22-50" in low or "over 60" in low) and \
            "minimum" not in low and DOLLAR_RE.search(ln):
            # Check if the numbers match minimum demand kW shape
            if dollars and max(dollars) > 100:  # these rows are > $100 for minimum demand
                out.append(ln)
                continue

        # ------------------------------
        # 3. ADDITIONAL DEMAND CHARGES
        # ------------------------------
        if "additional demand" in low and DOLLAR_RE.search(ln):
            out.append(ln)
            continue

        # Additional demand row (small $ but > $4)
        if ("0-2.2" in low or "2.2-15" in low or "22-50" in low or "over 60" in low) and \
            DOLLAR_RE.search(ln) and dollars and 1.0 < min(dollars) < 20:
            out.append(ln)
            continue

        # ------------------------------
        # 4. REACTIVE DEMAND
        # ------------------------------
        if "reactive demand" in low and DOLLAR_RE.search(ln):
            out.append(ln)
            continue

        if "rkva" in low and DOLLAR_RE.search(ln):
            out.append(ln)
            continue

    # Dedupe while preserving order
    seen, cleaned = set(), []
    for ln in out:
        if ln not in seen:
            seen.add(ln)
            cleaned.append(ln)

    return cleaned




# =====================================================================
# SC3A RATE EXTRACTION
# =====================================================================

def extract_sc3a_rates(lines):
    out = []

    for ln in lines:
        low = ln.lower()
        dollars = _parse_dollars(ln)

        # Filter out TOU 0.xx rows
        if dollars and max(dollars) < 1.0:
            continue

        if ("distribution delivery" in low or "all delivery voltages" in low or "charges; per kw" in low) and dollars:
            out.append(ln)
            continue

        if "charges; per kw" in low and not dollars:
            out.append(ln)
            continue

    # Dedup
    seen = set()
    cleaned = []
    for ln in out:
        if ln not in seen:
            seen.add(ln)
            cleaned.append(ln)
    return cleaned


# =====================================================================
# MASTER RATE SELECTOR
# =====================================================================

def filter_rates_for_scheme(scheme, lines):
    if scheme == "SC1":
        return [ln for ln in lines if SC1_RATE_RE.search(ln)]
    if scheme == "SC1C":
        return [ln for ln in lines if SC1C_RATE_RE.search(ln)]
    if scheme == "SC2":
        return extract_sc2_rates(lines)
    if scheme == "SC2D":
        return [ln for ln in lines if SC2D_RATE_RE.search(ln)]
    if scheme == "SC3":
        return extract_sc3_rates(lines)
    if scheme == "SC3A":
        return extract_sc3a_rates(lines)
    return []


# =====================================================================
# APPLICABLE TEXT EXTRACTION
# =====================================================================

APPLICABLE_RE = re.compile(
    r"APPLICABLE TO USE OF SERVICE FOR"
    r"|CHARACTER OF SERVICE"
    r"|APPLICATION FOR SERVICE",
    re.I
)

SECTION_STOP_RE = re.compile(
    r"STANDARD TARIFF CHARGES"
    r"|MONTHLY RATE"
    r"|CHARGES:",
    re.I
)

def extract_applicable(lines):
    out, capturing = [], False

    for ln in lines:
        if APPLICABLE_RE.search(ln):
            capturing = True
            out.append(ln)
            continue

        if capturing:
            if SECTION_STOP_RE.search(ln):
                break
            out.append(ln)

    return out


# =====================================================================
# MAIN SCRIPT
# =====================================================================

def main():

    with open(RAW_JSON_PATH, "r", encoding="utf-8") as f:
        raw = json.load(f)
    pages = raw["pages"]

    # CLASSIFY PAGES
    scheme_pages = {sc: [] for sc in ["SC1", "SC1C", "SC2", "SC2D", "SC3", "SC3A"]}

    for p in pages:
        sc = classify_page(p["text"])
        if sc:
            scheme_pages[sc].append(p["page_number"])

    with open(SCHEME_PAGES_OUT, "w", encoding="utf-8") as f:
        json.dump(scheme_pages, f, indent=2)

    print(f"✔ Saved scheme pages → {SCHEME_PAGES_OUT}")

    # EXTRACT TEXT BLOCKS
    page_lookup = {p["page_number"]: p["text"] for p in pages}
    final = {}

    for scheme, pg_list in scheme_pages.items():
        applicable, rates = [], []

        for pg in pg_list:
            lines = clean_lines(page_lookup[pg])

            app = extract_applicable(lines)
            rts = filter_rates_for_scheme(scheme, lines)

            if app: applicable.extend(app)
            if rts: rates.extend(rts)

        final[scheme] = {
            "applicable_text": list(dict.fromkeys(applicable)),
            "rate_lines": list(dict.fromkeys(rates))
        }

    with open(SC_TEXT_BLOCKS_OUT, "w", encoding="utf-8") as f:
        json.dump(final, f, indent=2)

    print(f"✔ Saved clean SC text blocks → {SC_TEXT_BLOCKS_OUT}")


# RUN
if __name__ == "__main__":
    main()
