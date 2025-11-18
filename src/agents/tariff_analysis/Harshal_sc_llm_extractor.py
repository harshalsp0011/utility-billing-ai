# sc_llm_extractor_v3.py
"""
Dynamic LLM-based Tariff JSON extractor (v3)

Features:
- Detects schema type for each service classification from extracted text
- Asks LLM to output the matching, rule-engine-ready JSON
- Validates essential keys for each schema type
- Saves raw LLM responses and final parsed JSON

Usage:
    python sc_llm_extractor_v3.py
"""

import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, Any, List, Optional

from dotenv import load_dotenv
load_dotenv()

from openai import OpenAI

# -----------------------
# Config (edit paths as required)
# -----------------------
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SC_TEXT_JSON = ROOT / "data" / "processed" / "harshal_sc.json"
OUT_JSON = ROOT / "data" / "processed" / "Hp_sc_final_tariffs_v3.json"
RAW_RESP_JSON = ROOT / "data" / "processed" / "Hp_sc_llm_raw_responses_v3.json"

MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
MAX_RETRIES = 2
SLEEP_BETWEEN_RETRIES = 1.5

# -----------------------
# OpenAI client init
# -----------------------
try:
    client = OpenAI()
except Exception as e:
    print(f"❌ Error initializing OpenAI client: {e}")
    print("Make sure OPENAI_API_KEY is set in the environment.")
    sys.exit(1)


# -----------------------
# Utility: concat all sections into big text snippet
# -----------------------
def concat_all_sections(block: Dict[str, Any]) -> str:
    parts: List[str] = []
    src = block.get("pages")
    if src:
        parts.append("SOURCE_PAGES: " + ",".join([str(x) for x in src]))
    for k, v in block.items():
        if k == "pages" or v is None:
            continue
        header = f"=== SECTION: {k} ==="
        if isinstance(v, list):
            if v:
                parts.append(header)
                parts.extend([ln for ln in v if isinstance(ln, str)])
        elif isinstance(v, str):
            if v.strip():
                parts.append(header)
                parts.append(v.strip())
        else:
            parts.append(header)
            parts.append(json.dumps(v))
    return "\n".join(parts)


# -----------------------
# Schema detection heuristics
# -----------------------
def detect_schema_type(text: str) -> str:
    """
    Return one of:
      - simple_residential
      - simple_commercial
      - demand_metered
      - voltage_tiered_demand_metered
    """
    txt = text.lower()

    # voltage indicators & multi-tier + per kW/per kwh => SC3 family
    has_voltage = bool(re.search(r"\b0-2\.2|2\.2-15|22-50|over\s*60|over-60", txt))
    has_tier = bool(re.search(r"\btier\s*\d|\btier1|\b tier 1\b", txt))
    has_per_kw = bool(re.search(r"per\s*kW|per-kW|distribution\s*\(per kW\)", txt))
    has_per_kwh = bool(re.search(r"per\s*kwh|per-kwh|on peak|off peak|super peak", txt))
    has_first_40 = "first 40" in txt or "first forty" in txt
    has_rkva = "r kva" in txt.lower() or "rkva" in txt.lower() or "reactive demand" in txt.lower()
    has_demand_rules = bool(re.search(r"15[- ]minute|highest kW measured|billing demand|ratchet|preceding 11 months|minimum demand", txt))

    # Heuristics
    if has_voltage and (has_tier or has_first_40 or has_per_kw):
        return "voltage_tiered_demand_metered"
    if has_demand_rules or has_per_kw or has_rkva or "contract demand" in txt:
        return "demand_metered"
    # tiered energy but no voltage tiers -> simple_commercial
    if has_tier or (has_per_kwh and "tier" in txt):
        return "simple_commercial"
    # default fallback
    return "simple_residential"


# -----------------------
# Schema-specific instruction fragments
# -----------------------
BASE_INSTRUCTION = r"""
You are a utility tariff extraction engine. Use ONLY the supplied text below (do not hallucinate).
Return EXACTLY valid JSON (no markdown). Values that are not present should be omitted (not null).
Numeric values should be plain numbers (no dollar signs).
"""

SCHEMA_TEMPLATES: Dict[str, str] = {
    "simple_residential": r"""
Schema: simple_residential
Return JSON keys:
{ "service_class","version?","effective_date?","customer_charge": {"default": number}, "energy_rates": {"flat_kwh":number}, "minimum_bill?":number, "rules?": {...}, "special_provisions?": {...}, "surcharges?": {"list": [...]}, "formulas?": {"bill": "..."}, "notes?": {...} }
Notes: residential classes have flat kWh or simple TOU. If On/Off/Super Peak rates are present capture as energy_rates.on_peak_kwh / off_peak_kwh / super_peak_kwh.
""",
    "simple_commercial": r"""
Schema: simple_commercial
Return JSON keys:
{ "service_class","version?","effective_date?","customer_charge": {"default": number}, "energy_rates": { "tier1": {...}, "tier2"?: {...}, ... }, "demand_charges?": {...}, "rules?": {...}, "special_provisions?": {...}, "surcharges?": {"list":[...]}, "formulas?": {...}, "notes?": {...} }
Notes: capture tiered energy rates (tier1..tier4) and attach on_peak/off_peak/super_peak keys under each tier if present.
""",
    "demand_metered": r"""
Schema: demand_metered
Return JSON keys:
{ "service_class","version?","effective_date?","customer_charge": {"default": number}, "demand_charges": {"contract_kw_rate"?:number, "on_peak_kw_rate"?:number, "super_peak_kw_rate"?:number, "additional_kw"?:number }, "reactive_charge?": {"rate_per_rkva":number,"formula":string}, "energy_rates?": {...}, "rules": {"demand_determination": string, "classification_shift": [...]}, "special_provisions?": {...}, "surcharges?": {"list":[...]}, "formulas?": {...}, "notes?": {...} }
Notes: demand formulas and ratchets are essential for billing.
""",
    "voltage_tiered_demand_metered": r"""
Schema: voltage_tiered_demand_metered
Return JSON keys:
{ "service_class","version?","effective_date?","voltage_levels": [..], "customer_charge": {"<voltage>":number,...}, "demand_charges": {"first_40_kw": {"<voltage>":number,...}, "additional_kw": {"<voltage>":number,...} }, "tiered_energy_rates": { "<voltage>": { "tier1": {"distribution_kw":number, "on_peak_kwh":number, "off_peak_kwh":number, "super_peak_kwh":number}, "tier2": {...}, ... }, ... }, "reactive_charge?": {"rate_per_rkva":number,"formula":string}, "rules": {"demand_determination":string, "classification_shift":[...]}, "special_provisions?": {...}, "surcharges?": {"list":[...]}, "formulas?": {...}, "notes?": {...} }
Notes: capture full per-voltage tier tables; if table has Tier1..Tier4 capture all tiers.
"""
}


# -----------------------
# Cleaning LLM output to pure JSON
# -----------------------
def clean_llm_json_text(raw: str) -> str:
    s = raw.strip()
    if s.startswith("```"):
        pieces = s.split("```")
        if len(pieces) >= 3:
            s = pieces[1]
        else:
            s = s.strip("`")
    if s.lower().startswith("json\n"):
        s = s[len("json\n"):]
    first = s.find("{")
    last = s.rfind("}")
    if first != -1 and last != -1:
        s = s[first:last+1]
    return s.strip()


# -----------------------
# LLM call wrapper
# -----------------------
def call_llm(prompt: str, model: str = MODEL) -> str:
    for attempt in range(1, MAX_RETRIES + 2):
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=6000
            )
            return resp.choices[0].message.content
        except Exception as e:
            print(f"⚠️ LLM error (attempt {attempt}): {e}")
            if attempt <= MAX_RETRIES:
                time.sleep(SLEEP_BETWEEN_RETRIES * attempt)
                continue
            raise


# -----------------------
# Schema basic validators
# -----------------------
def minimal_validation(parsed: Dict[str, Any], schema_type: str) -> List[str]:
    """
    Return a list of warning/error strings (empty means pass minimal checks).
    """
    issues = []
    # require service_class
    if "service_class" not in parsed:
        issues.append("missing service_class")
    if schema_type == "simple_residential":
        if "customer_charge" not in parsed and "energy_rates" not in parsed:
            issues.append("simple_residential should have customer_charge or energy_rates")
    if schema_type == "simple_commercial":
        if "energy_rates" not in parsed:
            issues.append("simple_commercial missing energy_rates (tiered rates)")
    if schema_type == "demand_metered":
        if "demand_charges" not in parsed and "reactive_charge" not in parsed:
            issues.append("demand_metered should include demand_charges or reactive_charge")
    if schema_type == "voltage_tiered_demand_metered":
        if "voltage_levels" not in parsed:
            issues.append("voltage_tiered_demand_metered missing voltage_levels")
        if "tiered_energy_rates" not in parsed and "demand_charges" not in parsed:
            issues.append("voltage_tiered_demand_metered requires tiered_energy_rates and demand_charges")
    return issues


# -----------------------
# Build prompt for a scheme
# -----------------------
def build_prompt_for_scheme(scheme: str, full_text: str, schema_type: str) -> str:
    prompt_parts = [
        BASE_INSTRUCTION,
        f"SCHEME: {scheme}",
        f"DETECTED_SCHEMA: {schema_type}",
        "SCHEMA_INSTRUCTIONS:",
        SCHEMA_TEMPLATES[schema_type],
        "",
        "INPUT TEXT (use ONLY this text):",
        "-----",
        full_text,
        "-----",
        "",
        "Output only JSON and nothing else."
    ]
    return "\n".join(prompt_parts)


# -----------------------
# Main run
# -----------------------
def run():
    if not SC_TEXT_JSON.exists():
        print(f"❌ Input file not found: {SC_TEXT_JSON}")
        sys.exit(1)

    with open(SC_TEXT_JSON, "r", encoding="utf-8") as f:
        sc_text = json.load(f)

    final_parsed: Dict[str, Any] = {}
    raw_responses: Dict[str, Any] = {}

    for scheme, block in sc_text.items():
        print(f"\n🔎 Processing: {scheme}")
        full_text = concat_all_sections(block)
        snippet = (full_text[:600] + "...") if len(full_text) > 600 else full_text

        # 1) detect schema
        schema_type = detect_schema_type(full_text)
        print(f" -> detected schema_type: {schema_type}")

        # 2) build prompt
        prompt = build_prompt_for_scheme(scheme, full_text, schema_type)

        # 3) call LLM
        try:
            raw = call_llm(prompt)
        except Exception as e:
            print(f"❌ LLM failed for {scheme}: {e}")
            raw_responses[scheme] = {"error": str(e)}
            continue

        raw_responses[scheme] = {"raw_response": raw}

        # 4) clean & parse
        cleaned = clean_llm_json_text(raw)
        raw_responses[scheme]["cleaned_text"] = cleaned[:4000]
        if not cleaned:
            print(f"❌ Empty cleaned response for {scheme}")
            continue

        try:
            parsed = json.loads(cleaned)
        except Exception as e:
            print(f"❌ JSON parse error for {scheme}: {e}")
            print("---- RAW RESPONSE (truncated) ----")
            print(raw[:2000])
            raw_responses[scheme]["parse_error"] = str(e)
            continue

        # 5) set service_class if missing
        if "service_class" not in parsed:
            parsed["service_class"] = scheme

        # 6) add trace snippet notes
        parsed.setdefault("notes", {})
        parsed["notes"]["raw_extraction_version"] = parsed["notes"].get("raw_extraction_version", "v3-auto")
        parsed["notes"]["raw_text_snippet"] = parsed["notes"].get("raw_text_snippet", snippet[:300])

        # 7) minimal validation
        issues = minimal_validation(parsed, schema_type)
        if issues:
            print(f"⚠️ Validation warnings for {scheme}: {issues}")
            parsed.setdefault("_validation_warnings", []).extend(issues)

        final_parsed[scheme] = parsed
        raw_responses[scheme]["parsed_json"] = parsed

        print(f"✔ Done {scheme} — keys: {list(parsed.keys())}")

    # Save results
    try:
        with open(RAW_RESP_JSON, "w", encoding="utf-8") as f:
            json.dump(raw_responses, f, indent=2)
        with open(OUT_JSON, "w", encoding="utf-8") as f:
            json.dump(final_parsed, f, indent=2)
    except Exception as e:
        print(f"❌ Error saving outputs: {e}")
        sys.exit(1)

    print(f"\n✅ Finished. Final JSON: {OUT_JSON}")
    print(f"✅ Raw responses: {RAW_RESP_JSON}")


if __name__ == "__main__":
    run()
