#!/usr/bin/env python3
"""
filter_and_extract_schemes.py — Dynamic universal section extractor

Outputs:
 - data/processed/harshal_scheme_pages.json
 - data/processed/harshal_sc.json

Usage:
 - place next to repo root where data/processed/raw_extracted_tarif.json exists
 - run: python filter_and_extract_schemes.py
"""

import json
import re
import sys
from pathlib import Path
from typing import List, Dict, Any, Tuple

# repo-root (same pattern as your previous scripts)
repo_root = Path(__file__).resolve().parents[3]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

RAW_JSON_PATH = repo_root / "data" / "processed" / "raw_extracted_tarif.json"
SCHEME_PAGES_OUT = repo_root / "data" / "processed" / "harshal_scheme_pages.json"
SC_TEXT_BLOCKS_OUT = repo_root / "data" / "processed" / "harshal_sc.json"

# -------------------------
# Tunable heuristics
# -------------------------
MIN_HEADER_WORDS = 1
MAX_HEADER_WORDS = 8
HEADER_UPPERCASE_RATIO = 0.6   # fraction of characters uppercase to treat as header
TABLE_DOLLAR_COUNT = 2        # a line with >= 2 dollar matches likely table row
TABLE_NUMERIC_COUNT = 3       # a line with >= 3 numeric (decimal) tokens likely table row
MULTI_SPACE_COL_SEP = re.compile(r"\s{2,}")  # used to split columns
SHORT_LINE_LEN = 60           # for header merging heuristics
MAX_LOOKAHEAD_LINES = 2       # merge headers up to this many following lines


# -------------------------
# Classification regexes
# -------------------------
SC3A_RE = re.compile(r"SERVICE CLASSIFICATION\s+NO\.?\s*3A\b", re.I)
SC3_RE  = re.compile(r"SERVICE CLASSIFICATION\s+NO\.?\s*3(\s|$)(?!A)", re.I)

SC1C_RE = re.compile(r"SERVICE CLASSIFICATION\s+NO\.?\s*1[\-\s]?C\b", re.I)
SC1_RE  = re.compile(r"SERVICE CLASSIFICATION\s+NO\.?\s*1(\D|$)", re.I)

SC2_HEAD_RE = re.compile(r"SERVICE CLASSIFICATION\s+NO\.?\s*2(\D|$)", re.I)
SC2D_RE     = re.compile(r"\bSC[\s\-]?2D\b|\b2[\s\-]?D\b", re.I)

# demand vs non-demand markers (for scoring)
DEMAND_MARKERS = [
    r"DEMAND",
    r"CONTRACT DEMAND",
    r"AS USED",
    r"CONTRACT DEMAND CHARGE",
    r"MINIMUM DEMAND",
    r"ADDITIONAL DEMAND",
    r"REACTIVE DEMAND",
    r"RkVA",
    r"RkVA",
    r"PER KW",
    r"PER K W",
    r"PER K W",
    r"DAILY DEMAND",
    r"METERED\s+DEMAND",
]
NON_DEMAND_MARKERS = [
    r"NON[- ]?DEMAND",
    r"PER KWH",
    r"PER KWH",
    r"PER KWH",
    r"MONTHLY MINIMUM CHARGE",
    r"BASIC SERVICE CHARGE",
    r"RESIDENTIAL",
    r"SINGLE PHASE",
]


# -------------------------
# Utility helpers
# -------------------------
def normalize_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def find_dollar_tokens(line: str) -> List[str]:
    return re.findall(r"\$[0-9\.,]+", line)


def find_numeric_tokens(line: str) -> List[str]:
    # finds numbers with decimals or integers (allows commas)
    return re.findall(r"[0-9]{1,3}(?:[,][0-9]{3})*(?:\.[0-9]+)?", line)


def is_short_upper_line(line: str) -> bool:
    """Heuristic: a short-ish line with many uppercase chars -> header candidate."""
    if len(line.strip()) == 0:
        return False
    letters = re.sub(r"[^A-Za-z]", "", line)
    if not letters:
        return False
    upper_ratio = sum(1 for c in letters if c.isupper()) / len(letters)
    word_count = len(line.split())
    return (upper_ratio >= HEADER_UPPERCASE_RATIO) and (word_count <= MAX_HEADER_WORDS)


def is_possible_header(line: str) -> bool:
    """
    Heuristic header detection:
    - All caps short line
    - Contains words like 'CHARGE', 'RATE', 'DEMAND', 'SERVICE'
    - Ends with ':' or is mostly uppercase
    """
    ln = line.strip()
    if not ln:
        return False

    # Likely header if lines end with ":" or are short and uppercase-ish
    if ln.endswith(":"):
        return True
    if is_short_upper_line(ln):
        return True

    # common header terms
    header_terms = ["CHARGE", "CHARGES", "RATE", "RATES", "DEMAND", "SERVICE", "MINIMUM", "TERMS", "PROVISION", "APPLICABLE", "MONTHLY", "APPLICATION", "CHARACTER"]
    upper = ln.upper()
    for t in header_terms:
        if t in upper and len(ln.split()) <= MAX_HEADER_WORDS + 4:
            return True

    # fallback: line with few words and trailing uppercase token
    if len(ln.split()) <= 6 and re.match(r"^[A-Z0-9\-\s\/\(\)]+$", ln):
        return True

    return False


def merge_multiline_headers(lines: List[str]) -> List[str]:
    """
    Merge headers that are split across lines.
    Example:
      'DETERMINATION OF'
      'DEMAND'
    -> 'DETERMINATION OF DEMAND'
    """
    out = []
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i].strip()
        if is_possible_header(line):
            # look ahead a couple lines and merge small uppercase lines
            merged = [line]
            look = 1
            while look <= MAX_LOOKAHEAD_LINES and (i + look) < n:
                nxt = lines[i + look].strip()
                if not nxt:
                    look += 1
                    continue
                # if next line also a short uppercase or short all-caps token, merge
                if is_possible_header(nxt) and len(nxt.split()) <= 4:
                    merged.append(nxt)
                    i += 1
                    look += 1
                    continue
                # if next line is tiny (<=3 chars) and uppercase, merge
                if len(nxt) <= 3 and nxt.upper() == nxt:
                    merged.append(nxt)
                    i += 1
                    look += 1
                    continue
                break
            out.append(" ".join(merged).strip())
        else:
            out.append(line)
        i += 1
    return out


def detect_table_line(line: str) -> bool:
    """Return True if line looks like a table row (multiple numeric/dollar tokens or multi-column spacing)."""
    if len(find_dollar_tokens(line)) >= TABLE_DOLLAR_COUNT:
        return True
    if len(find_numeric_tokens(line)) >= TABLE_NUMERIC_COUNT:
        return True
    # multi-space column separator heuristic
    if MULTI_SPACE_COL_SEP.search(line) and len(line.split()) > 2:
        return True
    return False


def parse_table_block(block_lines: List[str]) -> Dict[str, Any]:
    """
    Parse a block of table-like lines into columns heuristically.
    Returns: { "headers": [...], "rows": [[...],[...]], "raw": "..." }
    Heuristic approach:
      - If pipes '|' present, split on '|'
      - Else if multiple spaces separate, split on multi-space regex
      - Else fallback to whitespace split and then align by numeric/dollar tokens
    """
    raw = "\n".join(block_lines)
    rows = []
    for ln in block_lines:
        # clean leading/trailing separators
        ln = ln.strip().strip("| ")
        if "|" in ln:
            cols = [c.strip() for c in ln.split("|") if c.strip() != ""]
        elif MULTI_SPACE_COL_SEP.search(ln):
            cols = [c.strip() for c in MULTI_SPACE_COL_SEP.split(ln) if c.strip() != ""]
        else:
            # final fallback: split by space but try to keep numeric tokens separate
            parts = ln.split()
            cols = parts
        rows.append(cols)

    # Attempt to detect headers: if first row contains non-numeric tokens and later rows contain numbers
    headers = []
    if rows:
        first = rows[0]
        numeric_ratio_first = sum(1 for c in first if re.search(r"[0-9\$]", c)) / max(1, len(first))
        # if first row mostly non-numeric and next rows have numeric => treat first as header
        if len(rows) > 1:
            next_row = rows[1]
            numeric_ratio_next = sum(1 for c in next_row if re.search(r"[0-9\$]", c)) / max(1, len(next_row))
            if numeric_ratio_first < 0.5 and numeric_ratio_next > 0.5:
                headers = first
                data_rows = rows[1:]
            else:
                # no clear header; create generic col names
                headers = [f"col_{i}" for i in range(len(first))]
                data_rows = rows
        else:
            headers = [f"col_{i}" for i in range(len(first))]
            data_rows = rows
    else:
        headers = []
        data_rows = []

    return {
        "raw": raw,
        "headers": headers,
        "rows": data_rows
    }


# -------------------------
# Classification: page -> SC
# -------------------------
def score_text_for_markers(text: str, markers: List[str]) -> int:
    count = 0
    for m in markers:
        count += len(re.findall(m, text, re.I))
    return count


def classify_page(text: str) -> str:
    """
    Improved page classifier with scoring for SC2 vs SC2D.
    Order/priority:
      SC3A -> SC3 -> SC1C -> SC1 -> SC2D -> SC2
    If nothing matches, returns None.
    """
    txt = text or ""
    # priority exact matches
    if SC3A_RE.search(txt):
        return "SC3A"
    if SC3_RE.search(txt):
        return "SC3"

    if SC1C_RE.search(txt):
        return "SC1C"
    if SC1_RE.search(txt):
        return "SC1"

    # SC2 family detection
    if SC2D_RE.search(txt):
        return "SC2D"
    if SC2_HEAD_RE.search(txt):
        # run scoring
        demand_score = score_text_for_markers(txt, DEMAND_MARKERS)
        nond_score = score_text_for_markers(txt, NON_DEMAND_MARKERS)

        # prefer SC2D if demand markers more common or explicit 'demand' words present
        if demand_score > nond_score:
            return "SC2D"
        else:
            # fallback: if "NON-DEMAND" phrase present, choose SC2
            if re.search(r"NON[- ]?DEMAND", txt, re.I):
                return "SC2"
            # fallback by presence of 'per kwh' -> non-demand
            if re.search(r"per\s*kwh", txt, re.I):
                return "SC2"
            return "SC2"

    # fallback: not identified
    return None


# -------------------------
# Section extraction (dynamic)
# -------------------------
def extract_sections(full_text: str) -> Dict[str, Any]:
    """
    Extract sections dynamically from combined text.
    Returns dict: header -> {"header": header, "raw_text": ..., "lines": [...], "tables":[...], "meta": {...}}
    """
    # normalize CRLFs, preserve lines
    raw_lines = [ln.rstrip() for ln in full_text.splitlines()]
    # first, merge multi-line headers
    merged_lines = merge_multiline_headers(raw_lines)

    sections: Dict[str, Any] = {}
    current_header = None
    current_buffer: List[str] = []
    current_table_buffer: List[str] = []
    in_table = False
    header_order: List[str] = []

    def flush_buffer():
        nonlocal current_header, current_buffer, current_table_buffer, in_table
        if current_header is None:
            return
        raw_block = "\n".join(current_buffer).strip()
        lines = [l for l in current_buffer if l.strip() != ""]
        # extract tables inside the block by scanning contiguous lines detected as table rows
        tables = []
        non_table_lines = []
        temp_table = []
        for ln in lines:
            if detect_table_line(ln):
                temp_table.append(ln)
            else:
                if temp_table:
                    # flush table
                    tables.append(parse_table_block(temp_table))
                    temp_table = []
                non_table_lines.append(ln)
        if temp_table:
            tables.append(parse_table_block(temp_table))
            temp_table = []

        sections[current_header] = {
            "header": current_header,
            "raw_text": raw_block,
            "lines": lines,
            "tables": tables,
            "meta": {
                "line_count": len(lines)
            }
        }

    # iterate lines and decide headers
    for ln in merged_lines:
        if is_possible_header(ln):
            # new header encountered
            # flush previous
            if current_header is not None:
                flush_buffer()
            # set new header normalized
            current_header = ln.strip().upper()
            header_order.append(current_header)
            current_buffer = []
            continue
        # not header: add to current buffer if header exists, else accumulate into a 'preamble' header
        if current_header is None:
            # create a Preamble header if needed
            current_header = "PREAMBLE"
            header_order.append(current_header)
            current_buffer = []
        current_buffer.append(ln)

    # flush final
    if current_header is not None:
        flush_buffer()

    # add header_order in meta
    return {
        "order": header_order,
        "sections": sections
    }


# -------------------------
# Main script
# -------------------------
def main():
    if not RAW_JSON_PATH.exists():
        print(f"❌ Input file not found: {RAW_JSON_PATH}")
        sys.exit(1)

    with open(RAW_JSON_PATH, "r", encoding="utf-8") as f:
        raw = json.load(f)

    pages = raw.get("pages", [])
    if not pages:
        print("❌ No pages in input JSON.")
        sys.exit(1)

    # classify pages
    scheme_pages = {sc: [] for sc in ["SC1", "SC1C", "SC2", "SC2D", "SC3", "SC3A"]}
    page_lookup = {}

    for p in pages:
        pg_num = p.get("page_number")
        txt = p.get("text", "") or ""
        page_lookup[pg_num] = txt
        sc = classify_page(txt)
        if sc:
            scheme_pages.setdefault(sc, []).append(pg_num)

    # To avoid missing pages, optionally add pages that mention "SERVICE CLASSIFICATION NO." generically
    # (not implemented by default - uncomment to enable)
    # for p in pages:
    #     if "SERVICE CLASSIFICATION NO." in (p.get("text","") or "").upper():
    #         ...

    # Save mapping (pages)
    with open(SCHEME_PAGES_OUT, "w", encoding="utf-8") as f:
        json.dump(scheme_pages, f, indent=2)

    print(f"✔ Saved scheme pages → {SCHEME_PAGES_OUT}")

    final: Dict[str, Any] = {}

    # For each scheme combine sorted pages and extract sections
    for scheme, pg_list in scheme_pages.items():
        if not pg_list:
            continue
        sorted_pages = sorted(pg_list)
        combined_text = "\n".join(page_lookup[p] for p in sorted_pages)
        sections_struct = extract_sections(combined_text)

        final[scheme] = {
            "pages": sorted_pages,
            "combined_text": combined_text,
            "sections_order": sections_struct.get("order", []),
            "sections": sections_struct.get("sections", {})
        }

        print(f"-> Processed {scheme}: pages={len(sorted_pages)} sections={len(final[scheme]['sections'])}")

    # Save final
    with open(SC_TEXT_BLOCKS_OUT, "w", encoding="utf-8") as f:
        json.dump(final, f, indent=2)

    print(f"✔ Saved structured SC text blocks → {SC_TEXT_BLOCKS_OUT}")


if __name__ == "__main__":
    main()
