#Extracts tables/text from PDF bills and processes them for further analysis.
# --- Optional: install in Colab ---
# !pip install pdfplumber pandas openpyxl pdfminer.six

# --- Optional: install (Colab) ---
#%pip install pdfplumber pandas openpyxl

#from tkinter import Tk, filedialog

#Tk().withdraw()  # hides the root window
#pdf_path = filedialog.askopenfilename(
#    title="Select a PDF file",
#    filetypes=[("PDF files", "*.pdf")]
#)
 
#print("Selected:", pdf_path)

import os, re, json
import pdfplumber
from pdfminer.layout import LAParams
import pandas as pd

# ========= SET YOUR PDF PATH HERE =========
pdf_path = r"D:\git\utility-billing-ai\data\raw\National Grid Usage Statement-With Overcharge.pdf"

#pdf_path = "National Grid Usage Statement-With Overcharge.pdf"
#pdf_path = "National Grid Usage Statement-Without Overcharge.pdf"
assert os.path.exists(pdf_path), f"PDF not found at: {pdf_path}"


# ========= YOUR CHAIN OBJECTS =========
# Assumes SchemaMain, llm, and create_extraction_chain(SchemaMain, llm) exist.
# If not, define/import them before running this cell.

# ---------------- helpers ----------------
DATE_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}$")

def rebuild_text_from_words(page, *, crop_to_table=True):
    """
    Rebuild text by (1) extracting words w/ positions, (2) optionally cropping to the
    table body using a simple heuristic, (3) grouping by line (y) and sorting by x.
    Returns (page_raw_text, table_text).
    """
    # Pull words with geometry; keep blanks and flow left-to-right
    words = page.extract_words(
        x_tolerance=1.5, y_tolerance=3,
        keep_blank_chars=True, use_text_flow=True,
        extra_attrs=["size"]
    )

    # Raw page text (best-effort reconstruction) — *never* altered later
    raw_lines = {}
    for w in words:
        y = round(w["top"], 1)
        raw_lines.setdefault(y, []).append(w)
    raw_text = "\n".join(
        " ".join(w["text"] for w in sorted(raw_lines[y], key=lambda z: z["x0"]))
        for y in sorted(raw_lines)
    )

    # Heuristic crop to the table body to avoid header shards leaking into rows:
    # - find the first line that contains a date token -> that's the top of the table body
    # - find the first line that contains 'Page' -> that's the footer (bottom bound)
    y_with_date = []
    for w in words:
        if DATE_RE.match(w["text"]):
            y_with_date.append(w["top"])
    table_top = min(y_with_date) - 2 if y_with_date else 0

    y_page_lines = [w["top"] for w in words if w["text"].lower() == "page"]
    table_bottom = min(y_page_lines) - 2 if y_page_lines else page.height

    tbl_words = [
        w for w in words
        if (not crop_to_table) or (table_top <= w["top"] <= table_bottom)
    ]

    # Rebuild table-only text
    tbl_lines = {}
    for w in tbl_words:
        y = round(w["top"], 1)
        tbl_lines.setdefault(y, []).append(w)
    table_text = "\n".join(
        " ".join(w["text"] for w in sorted(tbl_lines[y], key=lambda z: z["x0"]))
        for y in sorted(tbl_lines)
    )

    return raw_text, table_text

# ---------------- 1) Run your chain over each page ----------------
final = []
laparams = {"char_margin": 2.0, "word_margin": 0.1, "line_margin": 0.3}  # tighter word joins
with pdfplumber.open(pdf_path, laparams=laparams) as pdf:
    for pi, page in enumerate(pdf.pages, start=1):
        print(f"on page {pi}")
        # Rebuild robust text from absolute word positions
        page_raw, table_text = rebuild_text_from_words(page, crop_to_table=True)

        # Keep both: truly raw (best effort) & table-only (cleaner for LLM)
        try:
            inp = table_text.strip() or page_raw  # prefer the table body
            chain = create_extraction_chain(SchemaMain, llm)
            output = chain.run(inp)
            # also store the raw page text for auditability
            final.append({"_page_index": pi-1, "_raw_text": page_raw, **(output if isinstance(output, dict) else {"_raw_response": output})})
        except Exception as e:
            print(f"Error processing page {pi}: {e}")
            final.append({"_error": str(e), "_page_index": pi-1, "_raw_text": page_raw})

# ---------------- 2) Normalize chain outputs ----------------
def extract_json(obj):
    if isinstance(obj, (dict, list)):
        return obj
    if isinstance(obj, str):
        s = obj.strip()
        try:
            return json.loads(s)
        except Exception:
            start_obj = s.find("{"); end_obj = s.rfind("}")
            start_arr = s.find("["); end_arr = s.rfind("]")
            cand = None
            if start_obj != -1 and end_obj > start_obj: cand = s[start_obj:end_obj+1]
            elif start_arr != -1 and end_arr > start_arr: cand = s[start_arr:end_arr+1]
            if cand:
                try: return json.loads(cand)
                except Exception: return {"_raw_response": s}
            return {"_raw_response": s}
    return {"_raw_response": str(obj)}

objs = [extract_json(x) for x in final]

# Save raw NDJSON (includes _raw_text) so nothing is ever lost/overwritten
with open("llm_page_outputs.ndjson", "w", encoding="utf-8") as f:
    for o in objs:
        f.write(json.dumps(o, ensure_ascii=False) + "\n")

# ---------------- 3) Build the Summary sheet (top-level only) ----------------
summary_df = pd.json_normalize(objs, max_level=1)

# Drop nested lists/dicts from the summary
nested_cols = [c for c in summary_df.columns
               if summary_df[c].apply(lambda v: isinstance(v, (list, dict))).any()]
summary_df = summary_df.drop(columns=nested_cols, errors="ignore")

# Light parsing for dates/numbers (keeps original values in NDJSON)
def to_num(x):
    if pd.isna(x): return pd.NA
    s = str(x).replace("$","").replace(",","").strip()
    return pd.to_numeric(s, errors="coerce")

date_like = [c for c in summary_df.columns if re.search(r"(date|period|start|end|due)", c, re.I)]
for c in date_like:
    try: summary_df[c] = pd.to_datetime(summary_df[c], errors="coerce")
    except Exception: pass

num_like = [c for c in summary_df.columns if re.search(r"(kwh|amount|charge|tax|rate|demand|rkva|usage|total|balance|price|qty)", c, re.I)]
for c in num_like:
    summary_df[c] = summary_df[c].apply(to_num)

# ---------------- 4) Split repeated list keys to extra sheets ----------------
list_keys = set()
for o in objs:
    if isinstance(o, dict):
        for k, v in o.items():
            if isinstance(v, list):
                list_keys.add(k)

sheets = {"Summary": summary_df}
for key in list_keys:
    frames = []
    for idx, o in enumerate(objs):
        if not isinstance(o, dict) or key not in o or not isinstance(o[key], list):
            continue
        meta = {mk: mv for mk, mv in o.items() if not isinstance(mv, (list, dict))}
        df_li = pd.json_normalize(o, record_path=[key])
        df_li["SourcePageIndex"] = idx
        for mk, mv in meta.items():
            df_li[mk] = mv
        frames.append(df_li)
    if frames:
        li_df = pd.concat(frames, ignore_index=True)
        for c in li_df.columns:
            if re.search(r"(kwh|qty|amount|charge|tax|rate|demand|rkva|usage|total|price|unit)", str(c), re.I):
                li_df[c] = li_df[c].apply(to_num)
        sheets[key[:31] or "Items"] = li_df

# ---------------- 5) Save files ----------------
summary_df.to_csv("bill_extraction_summary.csv", index=False)

with pd.ExcelWriter("bill_extraction.xlsx", engine="openpyxl") as writer:
    for name, df in sheets.items():
        safe = re.sub(r"[:\\/?*\[\]]", "_", name)[:31] or "Sheet"
        df.to_excel(writer, sheet_name=safe, index=False)

print("Wrote:")
print(" - bill_extraction_summary.csv")
print(" - bill_extraction.xlsx (Summary + one sheet per nested list)")
print(" - llm_page_outputs.ndjson (raw per-page with _raw_text)")
