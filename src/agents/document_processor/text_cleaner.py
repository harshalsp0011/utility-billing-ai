#Cleans and formats extracted text from documents to prepare for analysis.
# ==== Utility bill "Monthly Electric History" -> CSV ====
# Strong cleanup + diagnostics so we can see why rows were zero.

import pandas as pd, re, unicodedata
from datetime import datetime
from pathlib import Path
from difflib import SequenceMatcher

INPUT_TEXT_CSV = Path("bill_extraction_summary.csv")              # <- ensure this is the CSV with the raw text
OUTPUT_CSV     = Path("bill_extraction_summary_formatted.csv")

DEST_COLS = [
    "Bill Account","Customer","Bill Date","Read Date","Days Used","Billed Kwh",
    "Billed Demand","Load Factor","Billed Rkva","Bill Amount","Sales Tax Amt",
    "Bill Amount w/Sales Tax","Retracted Amt","Sales Tax Factor",
]

# ---------------- helpers ----------------
def normspace(s: str) -> str:
    if s is None: return ""
    s = unicodedata.normalize("NFKC", str(s)).replace("\u00A0"," ")
    return re.sub(r"\s+", " ", s).strip()

DATE      = r"\d{1,2}/\d{1,2}/\d{2,4}"
ACCOUNT   = r"\b\d{7,12}\b"
DATE_RE   = re.compile(rf"^{DATE}$")

def as_date(tok: str):
    tok = tok.strip()
    if not DATE_RE.match(tok): return None
    for fmt in ("%m/%d/%Y","%m/%d/%y","%Y-%m-%d","%Y/%m/%d"):
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

# ---------------- load text ----------------
df_raw = pd.read_csv(INPUT_TEXT_CSV, encoding="utf-8-sig", engine="python", dtype=str, keep_default_na=False)
full_text = "\n".join(df_raw.apply(lambda r: " ".join(r.astype(str)), axis=1))
full_text = normspace(full_text)

# ---- Strong normalization ----
# 0) fix weird header shards that invade dates (Buffalo-style)
full_text = re.sub(r"A/1R", "/1", full_text)   # 4A/1R6/2021 -> 4/16/2021
full_text = re.sub(r"A0R/", "0/", full_text)   # 1A0R/14/2021 -> 10/14/2021
# generic variants
full_text = re.sub(r"(?<=\d)\s*[A-Z]{1,3}\s*/\s*(?=\d{1,2}/\d{2,4})", "/", full_text)
full_text = re.sub(r"(?<=\d)\s*A/?(\d)R(?=\d)", r"\1", full_text)

# 1) ensure space between ANY account and the start of an ALL-CAPS name
full_text = re.sub(rf"({ACCOUNT})(?=[A-Z])", r"\1 ", full_text)

# 2) ensure space between ANY letters and a date that follows immediately (e.g., 'DEP4/16/2021' -> 'DEP 4/16/2021')
full_text = re.sub(rf"([A-Z])(?={DATE})", r"\1 ", full_text)

# 3) split glued dates (…/YY)(MM/…) -> add a space
full_text = re.sub(rf"({DATE})(?={DATE})", r"\1 ", full_text)

# 4) start new potential row at each account number
full_text = re.sub(rf"\s*(?={ACCOUNT})", "\n", full_text)

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

bill_account = pull(full_text, [rf"Bill Account[:\s]+({ACCOUNT})"])
customer     = infer_customer_name(full_text)

# ---------------- diagnostics (so we see why rows might be zero) ----------------
acc_hits = len(re.findall(ACCOUNT, full_text))
date_pairs = list(re.finditer(rf"{DATE}\s+{DATE}", full_text))
print(f"accounts found: {acc_hits}")
print(f"date-pairs found: {len(date_pairs)}")
print("first 300 chars after cleanup:\n", full_text[:300], "\n")

for m in date_pairs[:5]:
    a, b = m.span()
    s = max(0, a-50); e = b+50
    print("…", full_text[s:e], "…")

# ---------------- tokenization + parsing ----------------
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

# ---------------- output ----------------
df_out = pd.DataFrame(rows)
for c in DEST_COLS:
    if c not in df_out.columns: df_out[c] = ""
df_out = df_out[DEST_COLS]
df_out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
print(f"\n✅ Parsed {len(df_out)} rows -> {OUTPUT_CSV.resolve()}")
print(df_out.head(30))
