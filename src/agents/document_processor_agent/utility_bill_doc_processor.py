# ------------------------------------------------------------------
# Unified Data Ingestion and Cleaning Toolkit
# Handles Excel loading, PDF table extraction, and text/table cleaning.
# ------------------------------------------------------------------

import os
import re
import unicodedata
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
import sys

from src.agents.billing_anomaly_detector_agent.anomaly_detector_llm_call import validate_account_with_llm

# Ensure project root is on sys.path so `from src...` imports work when running
# this file directly (python src/agents/document_processor/temp.py)
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pdfplumber
import pandas as pd

# Project helpers / paths / logger
from src.utils.helpers import ensure_file_exists, clean_column_names
from src.utils.data_paths import get_file_path
from src.utils.logger import get_logger
from src.database.db_utils import insert_user_bill

logger = get_logger(__name__)

# ==== Utility bill "Monthly Electric History" -> CSV ====
# Strong cleanup + diagnostics so we can see why rows were zero.

# Define your file name and directory
PDF_FILENAME = "validation_3.pdf"
INPUT_PDF = get_file_path("raw", PDF_FILENAME)

DEST_COLS = [
    "Bill Account", "Customer", "Bill Date", "Read Date", "Days Used", "Billed Kwh",
    "Billed Demand", "Load Factor", "Billed Rkva", "Bill Amount", "Sales Tax Amt",
    "Bill Amount w/Sales Tax", "Retracted Amt", "Sales Tax Factor",
]

# ---------------- helpers ----------------
def normspace(s: str) -> str:
    if s is None: return ""
    s = unicodedata.normalize("NFKC", str(s)).replace("\u00A0", " ")
    return re.sub(r"\s+", " ", s).strip()

DATE = r"\d{1,2}/\d{1,2}/\d{2,4}"
ACCOUNT = r"\b\d{7,12}\b"
DATE_RE = re.compile(rf"^{DATE}$")

def as_date(tok: str):
    tok = tok.strip()
    if not DATE_RE.match(tok): return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            dt = datetime.strptime(tok, fmt)
            return f"{dt.month}/{dt.day}/{dt.year}"
        except: pass
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
        v = float(s); out = f"{v:.{nd}f}".rstrip("0").rstrip("."); return out
    except: return tok

def is_money(tok: str):    return re.fullmatch(r"^\$?-?[\d,]*\.?\d+$", tok) is not None
def parse_money(tok: str):
    s = tok.replace("$","").replace(",","")
    if s in ("", "-", ".", "-."): return ""
    try: return f"${float(s):,.2f}"
    except: return tok

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

# ---------------- robust customer inference ----------------
def _norm_caps(s: str) -> str:
    s = re.sub(r"\s+", " ", s or "").strip()
    return re.sub(r"[^A-Z0-9 &'.,\-\/]", "", s.upper())

def _sim(a, b): return SequenceMatcher(None, a, b).ratio()

def _extract_header_customer(text: str) -> str:
    m = re.search(r"Customer[:\s]+(.+?)(?=\s+(?:Post Office:|Service Address:|Bill Account:|Monthly|Page\s+\d+|$))",
                  text, flags=re.IGNORECASE|re.DOTALL)
    return _norm_caps(m.group(1)) if m else ""

def _extract_row_customers(text: str):
    cands = []
    for m in re.finditer(rf"{ACCOUNT}\s*([A-Z0-9 &'.,\-\/]{{3,}}?)\s+(?={DATE})", text):
        cands.append(_norm_caps(m.group(1)))
    return cands

def _merge_customer(header_cand: str, row_cands):
    if not row_cands:  # fallback
        return header_cand
    freq = {}
    for c in row_cands:
        if c: freq[c] = freq.get(c, 0) + 1
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

# ---------------- main extraction logic ----------------
def extract_bill_data(pdf_path: Path):
    full_text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            full_text += page.extract_text() + "\n"
    full_text = normspace(full_text)

    # Apply the same normalization as in your CSV script
    full_text = re.sub(r"A/1R", "/1", full_text)
    full_text = re.sub(r"A0R/", "0/", full_text)
    full_text = re.sub(r"(?<=\d)\s*[A-Z]{1,3}\s*/\s*(?=\d{1,2}/\d{2,4})", "/", full_text)
    full_text = re.sub(r"(?<=\d)\s*A/?(\d)R(?=\d)", r"\1", full_text)
    full_text = re.sub(rf"({ACCOUNT})(?=[A-Z])", r"\1 ", full_text)
    full_text = re.sub(rf"([A-Z])(?={DATE})", r"\1 ", full_text)
    full_text = re.sub(rf"({DATE})(?={DATE})", r"\1 ", full_text)
    full_text = re.sub(rf"\s*(?={ACCOUNT})", "\n", full_text)

    # Extract customer and account
    bill_account = pull(full_text, [rf"Bill Account[:\s]+({ACCOUNT})"])
    customer = infer_customer_name(full_text)

    # Diagnostics
    acc_hits = len(re.findall(ACCOUNT, full_text))
    date_pairs = list(re.finditer(rf"{DATE}\s+{DATE}", full_text))
    print(f"accounts found: {acc_hits}")
    print(f"date-pairs found: {len(date_pairs)}")
    print("first 300 chars after cleanup:\n", full_text[:300], "\n")

    for m in date_pairs[:5]:
        a, b = m.span()
        s = max(0, a-50); e = b+50
        print("…", full_text[s:e], "…")

    # Tokenization and parsing
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

        # find factor by scanning to boundary
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

    return rows

# ---------------- output and database insertion ----------------
def process_bill(pdf_path: Path):
    rows = extract_bill_data(pdf_path)
    df_out = pd.DataFrame(rows)
    for c in DEST_COLS:
        if c not in df_out.columns: df_out[c] = ""
    df_out = df_out[DEST_COLS]

    # keep a human-readable timestamp column in the DataFrame if needed
    df_out["Inserted At"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Normalize and convert types before inserting into DB
    def _to_optional_float(val):
        if val is None: return None
        s = str(val).replace("$", "").replace(",", "").strip()
        if s == "": return None
        try:
            return float(s)
        except Exception:
            return None

    def _to_optional_int(val):
        if val is None: return None
        s = str(val).replace(",", "").strip()
        if s == "": return None
        try:
            return int(float(s))
        except Exception:
            return None

    def _to_date(val):
        """Return ISO date string (YYYY-MM-DD) or None."""
        if val is None: return None
        s = str(val).strip()
        if s == "": return None
        # Try known formats
        for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%Y/%m/%d"):
            try:
                dt = datetime.strptime(s, fmt)
                return dt.date()
            except Exception:
                continue
        # Last resort: try pandas parsing
        try:
            return pd.to_datetime(s, errors='coerce').date()
        except Exception:
            return None

    inserted = 0
    total_anomalies_overall = 0
    validated_accounts = set()  # Track which accounts we've already validated
    
    for _, row in df_out.iterrows():
        record = {
            "bill_account": row.get("Bill Account") or None,
            "customer": row.get("Customer") or None,
            "bill_date": _to_date(row.get("Bill Date")),
            "read_date": _to_date(row.get("Read Date")),
            "days_used": _to_optional_int(row.get("Days Used")),
            "billed_kwh": _to_optional_float(row.get("Billed Kwh")),
            "billed_demand": _to_optional_float(row.get("Billed Demand")),
            "load_factor": _to_optional_float(row.get("Load Factor")),
            "billed_rkva": _to_optional_float(row.get("Billed Rkva")),
            "bill_amount": _to_optional_float(row.get("Bill Amount")),
            "sales_tax_amt": _to_optional_float(row.get("Sales Tax Amt")),
            "bill_amount_with_sales_tax": _to_optional_float(row.get("Bill Amount w/Sales Tax")),
            "retracted_amt": _to_optional_float(row.get("Retracted Amt")),
            "sales_tax_factor": _to_optional_float(row.get("Sales Tax Factor")),
            # Match the ORM field name `created_at` in `UserBills` model
            "created_at": datetime.utcnow(),
        }
        try:
            bill_account = insert_user_bill(record)
            if bill_account:
                validated_accounts.add(bill_account)  # Track unique accounts for validation
            inserted += 1
        except Exception as e:
            logger.error(f"Failed to insert user bill for account {record.get('bill_account')}: {e}")

    # Validate each unique account once after all bills are inserted
    for account in validated_accounts:
        try:
            anomalies = validate_account_with_llm(account)
            # Derive counts from LLM response
            summary = anomalies.get("summary", {}) if isinstance(anomalies, dict) else {}
            bills_with_anomalies = summary.get("bills_with_anomalies", 0)
            total_bills = summary.get("total_bills", 0)
            # Count total anomaly items across all bills
            bill_anomalies = anomalies.get("bill_anomalies", []) if isinstance(anomalies, dict) else []
            total_anomalies = 0
            for ba in bill_anomalies:
                try:
                    total_anomalies += len(ba.get("anomalies", []) or [])
                except Exception:
                    pass
            total_anomalies_overall += total_anomalies
            logger.info(
                f"Validation for account {account}: total_bills={total_bills}, bills_with_anomalies={bills_with_anomalies}, total_anomalies={total_anomalies}"
            )
        except Exception as e:
            logger.error(f"Failed to validate account {account}: {e}")

    logger.info(f"Parsed {len(df_out)} rows -> inserted {inserted} rows into DB (UserBills); total anomalies detected={total_anomalies_overall}")
    return df_out, total_anomalies_overall

# Example usage
if __name__ == "__main__":
    process_bill(INPUT_PDF)
    
    



