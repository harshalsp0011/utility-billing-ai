import json
import os
from openai import OpenAI
from dotenv import load_dotenv
import re

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")


# ==========================================
#           UTIL FUNCTIONS
# ==========================================

def extract_float(value):
    """Extract float from strings or mixed text (e.g., $0.11 per kWh)."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        match = re.search(r"[\d\.]+", value)
        if match:
            return float(match.group())
    return None


def extract_customer_charge(sc_block):
    """Extract default customer charge."""
    cc = sc_block.get("customer_charge", {})
    if isinstance(cc, dict):
        return extract_float(cc.get("default"))
    return extract_float(cc)


def extract_energy_rates(sc_block):
    """Generic extractor for energy rates: flat, TOU, tiered, voltage-based."""
    rates = {}
    er = sc_block.get("energy_rates", {})

    for key, val in er.items():
        # Format 1: { "per_kwh": 0.088 }
        if isinstance(val, dict) and "per_kwh" in val:
            rates[key] = {"type": "flat", "rate": extract_float(val["per_kwh"])}

        # Format 2: TOU groups
        elif isinstance(val, dict):
            inner = {}
            for subk, subv in val.items():
                inner[subk] = extract_float(subv)
            rates[key] = {"type": "tou", "rates": inner}

        # Format 3: simple numeric
        elif isinstance(val, (int, float, str)):
            rates[key] = {"type": "flat", "rate": extract_float(val)}

    return rates


def extract_demand_charges(sc_block):
    """Extract all demand charge values."""
    d = sc_block.get("demand_charges", {})
    out = {}
    for k, v in d.items():
        out[k] = extract_float(v)
    return out


# ==========================================
#          LLM CONDITION PARSER
# ==========================================

class TariffLLMInterpreter:

    def __init__(self, model="gpt-4o-mini", temperature=0):
        self.model = model
        self.temperature = temperature

    def interpret_sc(self, sc_name, sc_block):
        """LLM-assisted extraction of regulatory conditions."""

        payload = json.dumps(sc_block, indent=2)

        prompt = f"""
Convert this Service Classification into STRICT MACHINE-READABLE conditions.

DO NOT include pricing. Extract only rules:
- classification_conditions
- shifting_conditions
- metering_conditions
- operational_limits
- billing_logic_notes
- energy_tiers (if detected)
- voltage_tiers (if detected)

Be generic because future tariff files may differ.

SC NAME: {sc_name}
RAW DATA:
{payload}

OUTPUT STRICT JSON ONLY:
{{
  "classification_conditions": [],
  "shifting_conditions": [],
  "metering_conditions": [],
  "operational_limits": [],
  "billing_logic_notes": [],
  "energy_tiers": [],
  "voltage_tiers": [],
  "extra_metadata": {{
     "sc_name": "",
     "inferred_type": ""
  }}
}}
"""

        response = client.chat.completions.create(
            model=self.model,
            temperature=self.temperature,
            messages=[
                {"role": "system", "content": "You are an expert regulatory tariff interpreter."},
                {"role": "user", "content": prompt}
            ]
        )

        raw = response.choices[0].message.content

        # Try to parse strict JSON
        try:
            return json.loads(raw)
        except:
            try:
                json_blob = re.search(r"\{.*\}", raw, re.DOTALL).group()
                return json.loads(json_blob)
            except:
                return {
                    "classification_conditions": [],
                    "shifting_conditions": [],
                    "metering_conditions": [],
                    "operational_limits": [],
                    "billing_logic_notes": [],
                    "energy_tiers": [],
                    "voltage_tiers": [],
                    "extra_metadata": {"sc_name": sc_name, "inferred_type": "unknown"},
                    "_warning": "LLM JSON parse failed"
                }


# ==========================================
#        FORMULA TREE BUILDER
# ==========================================

def build_formula_tree(sc_name, sc_block, sc2_metered=None):

    customer_charge = extract_customer_charge(sc_block)
    energy_rates = extract_energy_rates(sc_block)
    demand_rates = extract_demand_charges(sc_block)

    # IMPORTANT RULE:
    # SC2D MUST inherit SC2 metered demand rate!
    if sc_name == "SC2D" and sc2_metered is not None:
        demand_rates["sc2_metered_demand_per_kw"] = sc2_metered

    tree = {
        "service_class": sc_name,
        "customer_charge": customer_charge,
        "energy_rates": energy_rates,
        "demand_charges": demand_rates,

        "formula_tree": {
            "base": "bill_amount = customer_charge",
            "energy_formula": "energy_cost = sum(rate * usage for all TOU/tiers/voltage groups)",
            "demand_formula": "demand_cost = sum(demand_rate * measured_kw or contract_kw)",
            "final": "bill_amount = customer_charge + energy_cost + demand_cost"
        }
    }

    return tree


# ==========================================
#          MAIN TARIF PROCESSOR
# ==========================================

def process_tariff(file_path):

    interpreter = TariffLLMInterpreter()

    with open(file_path, "r") as f:
        tariff = json.load(f)

    # Extract SC2 metered demand for SC2D inheritance
    sc2_metered = None
    if "SC2" in tariff:
        sc2_metered = extract_float(
            tariff["SC2"]["demand_charges"].get("per_kw")
        )

    final = {}

    for sc_name, sc_block in tariff.items():

        conditions = interpreter.interpret_sc(sc_name, sc_block)
        formula_tree = build_formula_tree(sc_name, sc_block, sc2_metered)

        final[sc_name] = {
            "conditions": conditions,
            "formula_tree": formula_tree
        }

    return final


# ==========================================
#              RUN IT
# ==========================================

file_path = "D:\\utility-billing-ai\\data\\processed\\Hp_sc_final_tariffs_v4.json"
output = process_tariff(file_path)

with open("final_tariff_output.json", "w") as f:
    json.dump(output, f, indent=2)

print("Formula Tree + LLM Conditions Applied Successfully!")