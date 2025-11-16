"""
scheme_extractor.py
---------------------------------
This module:
- Takes extracted SC text (from your PDF text extractor)
- Sends it to GPT-4o-mini via your LLMClient
- Produces dynamic tariff JSON for each SC
- Adds audit-friendly eligibility templates
- Saves JSON output using the same naming style as json_processor.py
"""

import json
import os
from datetime import datetime
from src.utils.llm_client import llm     # your GPT wrapper


# ================================================================
# 1. DYNAMIC JSON PROMPT
# ================================================================
DYNAMIC_JSON_PROMPT = """
You are an electric utility tariff extraction engine.

Your goals:
1. Extract tariff values from the given text.
2. Output a STRICT JSON object.
3. Include ONLY keys whose values appear in the text.
4. NEVER include null, empty strings, placeholders, or guesses.
5. NEVER invent a field unless present in the text.
6. Output MUST ALWAYS be valid JSON.

===========================
JSON STRUCTURE
===========================

{
  "tariff_entries": [...],
  "effective_dates": [...],
  "latest_effective_date": "...",
  "eligibility_short": {...}
}

===========================
RULES
===========================
- If a field is not present → do NOT include the key.
- For multiple rate tiers → include all.
- Extract ALL dates (Issued/Effective/Revised).
- Identify MOST RECENT effective date.
- No explanations outside JSON.

TEXT:
<<<BEGIN_TEXT>>>
{insert text}
<<<END_TEXT>>>
"""


# ================================================================
# 2. ELIGIBILITY SHORT RULE TEMPLATES
# ================================================================
ELIGIBILITY_TEMPLATES = {
    "SC1": {
        "applies": [
            "Residential customer",
            "Secondary voltage",
            "No demand meter",
            "Household use only"
        ],
        "excludes": [
            "Commercial activity",
            "Demand meter present",
            "Three-phase service"
        ]
    },

    "SC1C": {
        "applies": [
            "Residential heating customer",
            "Electric space heating",
            "Secondary voltage"
        ],
        "excludes": [
            "Non-heating accounts",
            "Commercial premises"
        ]
    },

    "SC2": {
        "applies": [
            "General business customer",
            "Secondary voltage",
            "No demand meter"
        ],
        "excludes": [
            "Demand-metered customers",
            "Primary voltage unless allowed"
        ]
    },

    "SC2D": {
        "applies": [
            "Demand-metered general business customer",
            "Demand billing applies",
            "Secondary or primary voltage"
        ],
        "excludes": [
            "No demand meter installed",
            "Residential customers"
        ]
    },

    "SC3": {
        "applies": [
            "Large general service",
            "Commercial or industrial load"
        ],
        "excludes": [
            "Residential premises"
        ]
    },

    "SC3A": {
        "applies": [
            "Large power service",
            "Primary voltage"
        ],
        "excludes": [
            "Secondary voltage customers",
            "Residential locations"
        ]
    }
}


def get_eligibility_rules(scheme):
    return {
        "applies_if_all_true": ELIGIBILITY_TEMPLATES.get(scheme, {}).get("applies", []),
        "does_not_apply_if_true": ELIGIBILITY_TEMPLATES.get(scheme, {}).get("excludes", [])
    }


# ================================================================
# 3. GPT EXTRACTION
# ================================================================
def run_llm_extraction(scheme_name, text):
    prompt = (
        DYNAMIC_JSON_PROMPT.replace("{insert text}", text)
        + f"\n\nSCHEME: {scheme_name}\n"
    )

    response = llm.ask(prompt, temperature=0.0)

    # Strip markdown code fences if present
    response = response.strip()
    if response.startswith("```json"):
        response = response[7:]  # Remove ```json
    elif response.startswith("```"):
        response = response[3:]  # Remove ```
    if response.endswith("```"):
        response = response[:-3]  # Remove trailing ```
    response = response.strip()

    try:
        data = json.loads(response)
    except json.JSONDecodeError:
        print("❌ INVALID JSON from model:")
        print(response[:500])
        raise

    # Add eligibility (LLM version may sometimes skip)
    data["eligibility_short"] = get_eligibility_rules(scheme_name)

    # Always normalize:
    dates = data.get("effective_dates", [])
    if isinstance(dates, str):
        dates = [dates]

    data["effective_dates"] = dates

    # compute latest effective date
    latest = data.get("latest_effective_date")

    if not latest and dates:
        # try auto-detect
        try:
            parsed = []
            for d in dates:
                parsed.append(datetime.strptime(d, "%m/%d/%Y"))
            data["latest_effective_date"] = max(parsed).strftime("%m-%d-%Y")
        except:
            data["latest_effective_date"] = "unknown"

    return data


# ================================================================
# 4. SAVE JSON USING SAME NAMING STYLE AS json_processor.py
# ================================================================
def save_output_json(scheme_name, data, output_dir="data/processed"):
    os.makedirs(output_dir, exist_ok=True)

    latest = data.get("latest_effective_date", "unknown")
    safe_date = latest.replace("/", "-").replace(" ", "_")

    filename = f"{scheme_name}_{safe_date}.json"
    path = os.path.join(output_dir, filename)

    with open(path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"✅ Saved JSON → {path}")
    return path


# ================================================================
# 5. MAIN PUBLIC FUNCTION
# ================================================================
def extract_scheme(scheme_name, text, output_dir="data/processed"):
    """
    Call this after you extract text from PDF.

    Example:
        extract_scheme("SC2D", extracted_text)
    """
    data = run_llm_extraction(scheme_name, text)
    save_path = save_output_json(scheme_name, data, output_dir)
    return data, save_path


# ================================================================
# 6. MANUAL TEST MODE
# ================================================================
if __name__ == "__main__":
    print("🔧 Manual test mode. Call extract_scheme('SC1', 'some text')")
