# ------------------------------------------------------------------
# Unified Data Ingestion and Cleaning Toolkit
# Handles Excel loading, PDF table extraction, and text/table cleaning.
# ------------------------------------------------------------------

"""
utility_bill_processor.py
-------------------------
Parses utility-bill PDFs using pdfplumber + an LLM extraction chain and exports:
- llm_page_outputs.ndjson  (raw per-page with _raw_text)
- bill_extraction_summary.csv  (flattened top-level fields)
- bill_extraction.xlsx  (Summary + one sheet per nested list)
- bill_extraction_summary_formatted.csv (Monthly Electric History table)

Integrations:
- src.utils.helpers      → save_csv, ensure_file_exists, clean_column_names
- src.utils.data_paths   → get_file_path (standardized data folders)
- src.utils.logger       → get_logger

Functions (≤5):
1) extract_page_texts
2) parse_pdf
3) normalize_outputs
4) save_outputs
5) format_monthly_history  ← full robust logic (Buffalo fixes, ghost money, factor)
"""

import os, re, json
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

import pdfplumber
import pandas as pd

# Project helpers / paths / logger
from src.utils.helpers import save_csv, ensure_file_exists, clean_column_names
from src.utils.data_paths import get_file_path
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Globals
DATE_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}$")


# Parse PDF and extract text blocks

def extract_page_texts(pdf_path: str):
    """
    Extract per-page raw_text and table_text using pdfplumber and a simple table crop.
    Returns: list[{"page": int, "raw_text": str, "table_text": str}]
    """
    assert ensure_file_exists(pdf_path)
    pages = []

    def rebuild_text_from_words(page):
        words = page.extract_words(
            x_tolerance=1.5, y_tolerance=3,
            keep_blank_chars=True, use_text_flow=True,
            extra_attrs=["size"]
        )
        # Reconstruct full text
        lines = {}
        for w in words:
            y = round(w["top"], 1)
            lines.setdefault(y, []).append(w)
        raw_text = "\n".join(
            " ".join(w["text"] for w in sorted(lines[y], key=lambda z: z["x0"]))
            for y in sorted(lines)
        )

        # Crop to table region (date-to-footer heuristic)
        y_with_date = [w["top"] for w in words if DATE_RE.match(w["text"])]
        y_page_lines = [w["top"] for w in words if w["text"].lower() == "page"]
        table_top = min(y_with_date) - 2 if y_with_date else 0
        table_bottom = min(y_page_lines) - 2 if y_page_lines else page.height
        tbl_words = [w for w in words if table_top <= w["top"] <= table_bottom]
        tbl_lines = {}
        for w in tbl_words:
            y = round(w["top"], 1)
            tbl_lines.setdefault(y, []).append(w)
        table_text = "\n".join(
            " ".join(w["text"] for w in sorted(tbl_lines[y], key=lambda z: z["x0"]))
            for y in sorted(tbl_lines)
        )
        return raw_text, table_text

    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            try:
                raw, tbl = rebuild_text_from_words(page)
                pages.append({"page": i, "raw_text": raw, "table_text": tbl})
                logger.info(f"📄 Extracted page {i}")
            except Exception as e:
                logger.error(f"⚠️ Failed to extract page {i}: {e}")
    return pages



# Run LLM chain & collect outputs

def parse_pdf(pdf_path: str, llm, SchemaMain, create_extraction_chain):
    """
    Runs the LLM extraction chain per page and collects outputs with _raw_text.
    Returns: list[dict]
    """
    pages = extract_page_texts(pdf_path)
    chain = create_extraction_chain(SchemaMain, llm)
    outputs = []

    for p in pages:
        text_input = p["table_text"].strip() or p["raw_text"]
        try:
            result = chain.run(text_input)
            outputs.append({
                "_page_index": p["page"],
                "_raw_text": p["raw_text"],
                **(result if isinstance(result, dict) else {"_raw_response": result})
            })
        except Exception as e:
            outputs.append({
                "_page_index": p["page"],
                "_raw_text": p["raw_text"],
                "_error": str(e)
            })
            logger.error(f"⚠️ Error on page {p['page']}: {e}")
    return outputs



# Normalize & flatten JSON responses (Summary + list sheets)

def normalize_outputs(outputs):
    """
    Flattens the top-level fields into a Summary dataframe and collects
    nested lists into separate dataframes keyed by list name.
    Returns: (objs, sheets_dict)
    """
    def extract_json(obj):
        if isinstance(obj, (dict, list)): 
            return obj
        if isinstance(obj, str):
            s = obj.strip()
            try:
                return json.loads(s)
            except Exception:
                # try object slice
                start, end = s.find("{"), s.rfind("}")
                if start != -1 and end > start:
                    try: 
                        return json.loads(s[start:end+1])
                    except: 
                        pass
                # try array slice
                start, end = s.find("["), s.rfind("]")
                if start != -1 and end > start:
                    try:
                        return json.loads(s[start:end+1])
                    except:
                        pass
        return {"_raw_response": str(obj)}

    objs = [extract_json(o) for o in outputs]
    summary = pd.json_normalize(objs, max_level=1)

    # drop nested structures from Summary
    nested_cols = [
        c for c in summary.columns
        if summary[c].apply(lambda v: isinstance(v, (list, dict))).any()
    ]
    summary = summary.drop(columns=nested_cols, errors="ignore")

    # light type normalization
    def to_num(x):
        if pd.isna(x): return pd.NA
        s = str(x).replace("$", "").replace(",", "").strip()
        return pd.to_numeric(s, errors="coerce")

    for c in summary.columns:
        if re.search(r"(date|period|start|end|due)", c, re.I):
            try:
                summary[c] = pd.to_datetime(summary[c], errors="coerce")
            except: 
                pass
        if re.search(r"(kwh|amount|charge|tax|rate|demand|rkva|usage|total|balance|price|qty)", c, re.I):
            summary[c] = summary[c].apply(to_num)

    sheets = {"Summary": summary}

    # collect nested lists into sheets
    for o in objs:
        if not isinstance(o, dict): 
            continue
        for k, v in o.items():
            if isinstance(v, list):
                df = pd.json_normalize(o, record_path=[k])
                df["SourcePageIndex"] = o.get("_page_index")
                name = k[:31] or "Items"
                if name in sheets:
                    sheets[name] = pd.concat([sheets[name], df], ignore_index=True)
                else:
                    sheets[name] = df

    return objs, sheets



# Save outputs (NDJSON, CSV, Excel)

def save_outputs(objs, sheets):
    """
    Writes:
      - /data/output/llm_page_outputs.ndjson
      - /data/output/bill_extraction_summary.csv
      - /data/output/bill_extraction.xlsx (Summary + list sheets)
    """
    # NDJSON (raw per-page)
    ndjson_path = get_file_path("output", "llm_page_outputs.ndjson")
    with open(ndjson_path, "w", encoding="utf-8") as f:
        for o in objs:
            f.write(json.dumps(o, ensure_ascii=False) + "\n")
    logger.info(f"🧾 NDJSON saved → {ndjson_path}")

    # Summary CSV
    summary_df = sheets.get("Summary", pd.DataFrame())
    save_csv(summary_df, "output", "bill_extraction_summary.csv")

    # Excel workbook
    excel_path = get_file_path("output", "bill_extraction.xlsx")
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        for name, df in sheets.items():
            clean = clean_column_names(df.copy())
            safe = name[:31] or "Sheet"
            clean.to_excel(writer, sheet_name=safe, index=False)
    logger.info(f"📘 Excel workbook saved → {excel_path}")



# Full Monthly Electric History formatter (robust logic)

def format_monthly_history():
    """
    Recreates the full 'Monthly Electric History -> CSV' logic with:
    - Buffalo-style header/date shard fixes
    - robust customer inference
    - ghost-money skipping
    - footer detection
    - Sales Tax Factor detection
    Saves → /data/processed/bill_extraction_summary_formatted.csv
    """
    import re, unicodedata
    import pandas as pd

    INPUT_TEXT_CSV = get_file_path("output", "bill_extraction_summary.csv")
    OUTPUT_CSV_PATH = get_file_path("processed", "bill_extraction_summary_formatted.csv")

    logger.info("⚙️ Running full Monthly Electric History formatter...")

    # helpers (verbatim behavior) 
    def normspace(s: str) -> str:
        if s is None: return ""
        s = unicodedata.normalize("NFKC", str(s)).replace("\u00A0"," ")
        return re.sub(r"\s+", " ", s).strip()

    DATE      = r"\d{1,2}/\d{1,2}/\d{2,4}"
    ACCOUNT   = r"\b\d{7,12}\b"
    DATE_REX  = re.compile(rf"^{DATE}$")

    def as_date(tok: str):
        tok = tok.strip()
        if not DATE_REX.match(tok): return None
        for fmt in ("%m/%d/%Y","%m/%d/%y","%Y-%m-%d","%Y/%m/%d"):
            try:
                dt = datetime.strptime(tok, fmt)
                return f"{dt.month}/{dt.day}/{dt.year}"
            except:
                pass
        return tok

    def is_intlike(tok: str):  return re.fullmatch(r"^-?[\d,]+$", tok) is not None
    def parse_intlike(tok: str):
        m = re.sub(r"[^\d\-]", "", tok)
        if m in ("", "-",): return ""
        return f"{int(m):,}"

    def is_num(tok: str):      return re.fullmatch(r"^-?[\d,]*\.?\d+$", tok.replace(",", "")) is not None
    def parse_float(tok: str, nd=2):
        s = tok.replace(",", "")
        try:
            v = float(s)
            out = f"{v:.{nd}f}".rstrip("0").rstrip(".")
            return out
        except:
            return tok

    def is_money(tok: str):    return re.fullmatch(r"^\$?-?[\d,]*\.?\d+$", tok) is not None
    def parse_money(tok: str):
        s = tok.replace("$","").replace(",","")
        if s in ("", "-", ".", "-."): return ""
        try:
            return f"${float(s):,.2f}"
        except:
            return tok

    def pull(text, pats, group=1, default=""):
        for pat in pats:
            m = re.search(pat, text, flags=re.IGNORECASE)
            if m: return normspace(m.group(group))
        return default

    def is_page_footer(tokens, k):
        return (k + 3 < len(tokens) and tokens[k].lower() == "page"
                and re.fullmatch(r"[\d,]+", tokens[k+1] or "")
                and tokens[k+2].lower() == "of"
                and re.fullmatch(r"[\d,]+", tokens[k+3] or ""))

    # -load & normalize text 
    df_raw = pd.read_csv(INPUT_TEXT_CSV, encoding="utf-8-sig", engine="python", dtype=str, keep_default_na=False)
    full_text = "\n".join(df_raw.apply(lambda r: " ".join(r.astype(str)), axis=1))
    full_text = normspace(full_text)

    # Buffalo-style header shards invading dates
    full_text = re.sub(r"A/1R", "/1", full_text)   # 4A/1R6/2021 -> 4/16/2021
    full_text = re.sub(r"A0R/", "0/", full_text)   # 1A0R/14/2021 -> 10/14/2021
    # generic variants
    full_text = re.sub(r"(?<=\d)\s*[A-Z]{1,3}\s*/\s*(?=\d{1,2}/\d{2,4})", "/", full_text)
    full_text = re.sub(r"(?<=\d)\s*A/?(\d)R(?=\d)", r"\1", full_text)

    # ensure spacing around accounts, dates
    full_text = re.sub(rf"({ACCOUNT})(?=[A-Z])", r"\1 ", full_text)
    full_text = re.sub(rf"([A-Z])(?={DATE})", r"\1 ", full_text)
    full_text = re.sub(rf"({DATE})(?={DATE})", r"\1 ", full_text)
    full_text = re.sub(rf"\s*(?={ACCOUNT})", "\n", full_text)

    # robust customer inference 
    def _norm_caps(s: str) -> str:
        s = re.sub(r"\s+", " ", s or "").strip()
        return re.sub(r"[^A-Z0-9 &'.,\-\/]", "", s.upper())

    def _sim(a, b): 
        return SequenceMatcher(None, a, b).ratio()

    def _extract_header_customer(text: str) -> str:
        m = re.search(
            r"Customer[:\s]+(.+?)(?=\s+(?:Post Office:|Service Address:|Bill Account:|Monthly|Page\s+\d+|$))",
            text, flags=re.IGNORECASE|re.DOTALL
        )
        return _norm_caps(m.group(1)) if m else ""

    def _extract_row_customers(text: str):
        cands = []
        for m in re.finditer(rf"{ACCOUNT}\s*([A-Z0-9 &'.,\-\/]{{3,}}?)\s+(?={DATE})", text):
            cands.append(_norm_caps(m.group(1)))
        return cands

    def _merge_customer(header_cand: str, row_cands):
        if not row_cands:
            return header_cand
        freq = {}
        for c in row_cands:
            if c: 
                freq[c] = freq.get(c, 0) + 1
        best = sorted(freq.items(), key=lambda kv: (kv[1], len(kv[0])), reverse=True)[0][0]
        if header_cand and best.startswith(header_cand) and len(best) > len(header_cand):
            return best
        if header_cand:
            scored = sorted(freq.keys(), key=lambda c: (_sim(header_cand, c), len(c)), reverse=True)
            if scored and _sim(header_cand, scored[0]) >= 0.80 and len(scored[0]) >= len(header_cand):
                return scored[0]
        return best

    def infer_customer_name(text: str) -> str:
        header = _extract_header_customer(text)
        rows   = _extract_row_customers(text)
        return _merge_customer(header, rows)

    bill_account = pull(full_text, [rf"Bill Account[:\s]+({ACCOUNT})"])
    customer     = infer_customer_name(full_text)

    # diagnostics
    acc_hits = len(re.findall(ACCOUNT, full_text))
    date_pairs = list(re.finditer(rf"{DATE}\s+{DATE}", full_text))
    logger.info(f"🔎 accounts found: {acc_hits} | date-pairs found: {len(date_pairs)}")
    logger.debug(f"first 300 chars after cleanup:\n{full_text[:300]}")

    # token walk + parsing
    tokens = full_text.split()
    rows = []
    i, N = 0, len(tokens)

    while i + 1 < N:
        d1 = as_date(tokens[i])
        d2 = as_date(tokens[i+1]) if d1 else None
        if not (d1 and d2):
            i += 1
            continue

        j = i + 2
        if j >= N or not is_intlike(tokens[j]): i += 1; continue
        days = parse_intlike(tokens[j]); j += 1

        if j >= N or not is_intlike(tokens[j].lstrip("-")): i += 1; continue
        kwh = parse_intlike(tokens[j]); j += 1

        if j >= N or not is_num(tokens[j]): i += 1; continue
        demand = parse_float(tokens[j], nd=1); j += 1

        if j >= N or not is_num(tokens[j]): i += 1; continue
        load_factor = parse_float(tokens[j], nd=2); j += 1

        if j >= N or not is_intlike(tokens[j]): i += 1; continue
        rkva = parse_intlike(tokens[j]); j += 1

        if j >= N or not is_money(tokens[j]): i += 1; continue
        bill_amt = parse_money(tokens[j]); j += 1

        if j >= N or not is_money(tokens[j]): i += 1; continue
        sales_tax_amt = parse_money(tokens[j]); j += 1

        if j >= N or not (is_money(tokens[j]) or is_num(tokens[j])): i += 1; continue
        bill_with_tax = parse_money(tokens[j]) if is_money(tokens[j]) else parse_money(tokens[j]); j += 1

        # ghost money BEFORE retracted
        while (j + 2 < N and (is_money(tokens[j]) or is_num(tokens[j]))
               and (is_money(tokens[j+1]) or is_num(tokens[j+1]))
               and (is_num(tokens[j+2]) or as_date(tokens[j+2]) or tokens[j+2].lower()=="page")):
            j += 1

        if j >= N or not (is_money(tokens[j]) or is_num(tokens[j])): i += 1; continue
        retracted_amt = parse_money(tokens[j]); j += 1

        # ghost money AFTER retracted
        while j < N and is_money(tokens[j]):
            nxt = tokens[j+1] if j+1 < N else ""
            if is_num(nxt) or as_date(nxt) or nxt.lower() == "page":
                break
            j += 1

        # find Sales Tax Factor by scanning to boundary
        k = j
        boundary = N
        while k < N:
            if as_date(tokens[k]) or is_page_footer(tokens, k) \
               or re.match(ACCOUNT, tokens[k] or "") or tokens[k] in ("Bill","Customer","Monthly"):
                boundary = k
                break
            k += 1

        factor = ""
        for t in range(j, min(boundary, j+12)):
            if is_num(tokens[t]) and not is_money(tokens[t]):
                try: val = float(tokens[t].replace(",",""))
                except: val = 9999
                if val <= 100:
                    factor = parse_float(tokens[t], nd=2)
                    j = t + 1
                    break
        if factor == "":
            for t in range(boundary-1, max(j-1, boundary-12), -1):
                if is_num(tokens[t]) and not is_money(tokens[t]):
                    try: val = float(tokens[t].replace(",",""))
                    except: val = 9999
                    if val <= 100:
                        factor = parse_float(tokens[t], nd=2)
                        j = t + 1
                        break

        while j < N:
            if as_date(tokens[j]): j += 1; continue
            if is_page_footer(tokens, j): j += 4; continue
            break

        rows.append({
            "Bill Account": bill_account or "",
            "Customer":     customer or "",
            "Bill Date":    d1,
            "Read Date":    d2,
            "Days Used":    days,
            "Billed Kwh":   kwh,
            "Billed Demand":demand,
            "Load Factor":  load_factor,
            "Billed Rkva":  rkva,
            "Bill Amount":  bill_amt,
            "Sales Tax Amt":sales_tax_amt,
            "Bill Amount w/Sales Tax": bill_with_tax,
            "Retracted Amt":retracted_amt,
            "Sales Tax Factor": factor,
        })
        i = j

    # output
    DEST_COLS = [
        "Bill Account","Customer","Bill Date","Read Date","Days Used","Billed Kwh",
        "Billed Demand","Load Factor","Billed Rkva","Bill Amount","Sales Tax Amt",
        "Bill Amount w/Sales Tax","Retracted Amt","Sales Tax Factor",
    ]

    df_out = pd.DataFrame(rows)
    for c in DEST_COLS:
        if c not in df_out.columns: 
            df_out[c] = ""
    df_out = df_out[DEST_COLS]

    # save to /data/processed
    save_csv(df_out, "processed", "bill_extraction_summary_formatted.csv")
    logger.info(f"Parsed {len(df_out)} rows → {OUTPUT_CSV_PATH}")
    return df_out



# Main

if __name__ == "__main__":
    # Import your chain objects from wherever you define them
    # Make sure these imports are valid in your project
    from src.agents.llm_chain import SchemaMain, llm, create_extraction_chain  # example path

    # Input PDF (standardized under /data/raw)
    pdf_path = get_file_path("raw", "National Grid Usage Statement-With Overcharge.pdf")

    # 1) Parse with LLM page-by-page
    outputs = parse_pdf(pdf_path, llm, SchemaMain, create_extraction_chain)

    # 2) Normalize into Summary + list sheets
    objs, sheets = normalize_outputs(outputs)

    # 3) Save NDJSON, CSV, Excel
    save_outputs(objs, sheets)

    # 4) Build Monthly Electric History table (robust logic)
    format_monthly_history()

    logger.info("Utility bill processing pipeline completed successfully.")

