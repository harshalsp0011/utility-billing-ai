"""
sc_llm_extractor.py (FINAL — MATCHES NEW SCHEME TEXT FORMAT)
---------------------------------------------------------
Loads sc_text_blocks.json:
{
   "SC1": {
        "applicable_text": [...],
        "rate_lines": [...]
   }
}

Builds full_text dynamically → sends to LLM → saves structured JSON.
"""

import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

from openai import OpenAI

# Ensure project root is on sys.path
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

# Initialize OpenAI client
try:
    client = OpenAI()
except Exception as e:
    print(f"⚠️ Error initializing OpenAI client: {e}")
    print("Ensure OPENAI_API_KEY is set correctly.")
    sys.exit(1)

# Input/Output paths
SC_TEXT_JSON = Path(r"D:\utility-billing-ai\data\processed\sc_text_blocks.json")
OUT_JSON = Path(r"D:\utility-billing-ai\data\processed\sc_llm_output.json")


# ================================================================
# SMART LLM PROMPT (unchanged — classification rules + dates)
# ================================================================
LLM_PROMPT = """
You are a highly specialized Electric Utility Tariff Extraction Engine trained to interpret 
regulatory tariffs, supply/delivery charges, rate tables, classification rules, and 
eligibility conditions with extreme accuracy.

The input text is cleaned and filtered for a single service classification (SC1, SC1C, SC2, 
SC2D, SC3, SC3A).  
Your goal is to convert it into a STRICT, VALID JSON object without hallucination.

=============================================================
CORE RESPONSIBILITIES
=============================================================

You MUST analyze the text with full domain understanding and produce:

1. **tariff_entries**
   Extract ONLY the rates that actually appear in the text.

   VALID examples include:
   - Basic Service Charge ($ per month)
   - Customer Charges ($ per month)
   - Distribution Delivery Charges (per kWh, per kW)
   - Voltage-tier rates (0–2.2 kV, 2.2–15 kV, 22–50 kV, Over 60 kV)
   - Demand charges (per kW)
   - Additional demand charges (per kW)
   - TOU Rates (On-Peak, Off-Peak, Super-Peak)
   - Special Provision O/P/L/N charges
   - Any table row that contains a valid $ rate

   **Do NOT invent missing rates.  
   Do NOT output supply rates unless explicitly in the text.**

   Each tariff entry should be structured as:
   {
      "label": "...",
      "value": "$X.XX",
      "unit": "per kWh" | "per kW" | "monthly" | "per kW (additional)" | etc.,
      "voltage_level": "Up to 2.2 kV" | "2.2–15 kV" | ... (IF APPLICABLE)
   }


2. **effective_dates**
   Extract ALL dates mentioned in the text:
   - “Effective”
   - “Issued”
   - “Revised”
   - “Filed”
   - “Postponed”

   Output them as a list of raw date strings.

3. **latest_effective_date**
   The most recent of the effective dates.
   If you cannot identify any effective date, omit this key entirely.

4. **eligibility_short**
   MUST summarize the applicability text accurately.

   Extract and categorize:
   - **applies_if_all_true** → 
     Clean bullet points describing when a customer QUALIFIES for this SC.
   - **does_not_apply_if_true** → 
     Conditions that EXCLUDE a customer.

   Examples:
   - “Customer served at secondary voltage”
   - “Electric heating required”
   - “Residential use only”
   - “Customer cannot be demand metered”

   **This must be factual and traceable to the text.**

5. **classification_shift_rules**
   Extract all rules that describe WHEN a customer moves from one SC to another.

   Examples:
   - “Customer becomes SC2D if a demand meter is installed.”
   - “SC3 customer class shifts to SC3A when served at primary voltage.”
   - “SC1 shifts to SC1C for electric space heating.”

6. **change_of_classification_conditions**
   Extract numerical or threshold-based rules.

   Examples:
   - “SC2 → SC2D if billing demand exceeds 10 kW.”
   - “SC3 → SC3A if voltage > 2.2 kV.”
   - “Remain SC3 unless demand < 100 kW for 12 consecutive months.”

=============================================================
STRICT JSON REQUIREMENTS
=============================================================

You MUST output ONLY this JSON structure:

{
  "tariff_entries": [...],                   // omit if empty
  "effective_dates": [...],                  // omit if none
  "latest_effective_date": "...",            // omit if unknown
  "eligibility_short": {
      "applies_if_all_true": [...],
      "does_not_apply_if_true": [...]
  },
  "classification_shift_rules": [...],       // omit if none
  "change_of_classification_conditions": [...] // omit if none
}

RULES:
- ABSOLUTELY NO hallucinated numbers.
- DO NOT guess missing rates.
- DO NOT include keys with null, empty, or placeholder values.
- OMIT keys when there is no information.
- Output MUST be pure JSON. No markdown, no commentary.

=============================================================
INPUT TEXT BELOW
=============================================================

<<<BEGIN>>>
{TEXT}
<<<END>>>

"""


# ================================================================
# MAIN EXTRACTION
# ================================================================
def run_llm_extraction():

    # Load cleaned text blocks
    with open(SC_TEXT_JSON, "r", encoding="utf-8") as f:
        sc_text = json.load(f)

    final_output = {}

    for scheme, block in sc_text.items():
        print(f"\n🚀 Processing {scheme}...")

        # -----------------------------------------------------
        # Build full_text dynamically (NEW KEYS!)
        # -----------------------------------------------------
      

        # your file uses "rates" not "rate_lines"
        rate_lines = block.get("rates", []) or block.get("rate_lines", [])

        # your file uses "applicable" not "applicable_text"
        applicable_lines = block.get("applicable", []) or block.get("applicable_text", [])

        full_text = "\n".join(rate_lines + [""] + applicable_lines)

        if not full_text.strip():
            print(f"⚠️ WARNING: {scheme} has empty text but WILL STILL BE SENT to LLM.")
            full_text = "(no rate lines found)\n" + "(no applicable text found)"


        # Prepare prompt
        prompt = LLM_PROMPT.replace("{TEXT}", full_text)

        # Send to model
        response = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )

        content = response.choices[0].message.content.strip()

        # Remove ```json wrappers if present
        if content.startswith("```"):
            content = content.split("```", 2)[1].strip()

        # Parse JSON output
        try:
            parsed = json.loads(content)
        except Exception:
            print(f"❌ INVALID JSON FROM MODEL FOR {scheme}:\n{content[:400]}")
            raise

        final_output[scheme] = parsed

    # Save final JSON
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(final_output, f, indent=2)

    print(f"\n✔ LLM tariff extraction saved to:\n{OUT_JSON}")


# ================================================================
# MAIN
# ================================================================
if __name__ == "__main__":
    run_llm_extraction()
