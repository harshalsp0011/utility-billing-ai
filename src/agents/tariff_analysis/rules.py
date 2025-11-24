import json
import re

# -----------------------------------------------------------
# Normalization helper
# -----------------------------------------------------------
def normalize_keys(obj):
    if isinstance(obj, dict):
        new = {}
        for k, v in obj.items():
            nk = re.sub(r"[^a-zA-Z0-9]", "_", k).lower()
            new[nk] = normalize_keys(v)
        return new
    elif isinstance(obj, list):
        return [normalize_keys(i) for i in obj]
    return obj

# -----------------------------------------------------------
# SC Type Detection
# -----------------------------------------------------------
def detect_sc_type(sc):
    has_tou = "time_of_use" in sc or "tou" in sc
    has_demand = "demand_charges" in sc
    is_voltage = isinstance(sc.get("customer_charge"), dict)
    
    if is_voltage:
        return "voltage_large_power"
    if has_tou and has_demand:
        return "tou_demand"
    if has_demand:
        return "demand_only"
    return "flat"

# -----------------------------------------------------------
# Add formula with value + label
# -----------------------------------------------------------
def add_formula(rules, key, label_path, value, bill_var):
    rules[key] = {
        "label": label_path,
        "value": value,
        "formula": f"$${bill_var} * {label_path}"
    }
    return rules

# -----------------------------------------------------------
# Build a formula tree for one SC
# -----------------------------------------------------------
def build_formula_tree(sc_name, sc_data, all_sc):
    rules = {}

    # SC2D inherits SC2 demand logic
    if sc_name == "sc2d" and "sc2" in all_sc:
        sc_data["demand_charges"] = all_sc["sc2"].get("demand_charges")

    # CUSTOMER CHARGE
    if "customer_charge" in sc_data:
        cc = sc_data["customer_charge"]
        if isinstance(cc, dict):  # voltage-level
            rules["customer_charge"] = {
                "label": "customer_charge[$voltage_level]",
                "value": "dynamic",
                "formula": "rates.customer_charge[$voltage_level]"
            }
        else:
            rules["customer_charge"] = {
                "label": "customer_charge",
                "value": cc,
                "formula": f"{cc}"
            }

    # ENERGY RATES
    if "energy_rates" in sc_data:
        er = sc_data["energy_rates"]

        # flat rate
        if isinstance(er, dict) and "default" in er and isinstance(er["default"], dict) and "per_kwh" in er["default"]:
            rate = er["default"]["per_kwh"]
            rules = add_formula(
                rules,
                "energy_charge",
                "energy_rates.default.per_kwh",
                rate,
                "billed_kwh"
            )
        elif isinstance(er, dict) and "per_kwh" in er:
            # Direct per_kwh (SC1C style)
            rate = er["per_kwh"]
            rules = add_formula(
                rules,
                "energy_charge",
                "energy_rates.per_kwh",
                rate,
                "billed_kwh"
            )

        # TOU rate
        if "time_of_use" in er and isinstance(er["time_of_use"], dict):
            tou = er["time_of_use"]
            tou_values = {}
            tou_formula_parts = []
            
            if "super_peak_on_peak" in tou:
                tou_values["super_peak_on_peak"] = tou["super_peak_on_peak"]
                tou_formula_parts.append("$$super_peak_kwh * energy_rates.time_of_use.super_peak_on_peak")
            if "off_peak" in tou:
                tou_values["off_peak"] = tou["off_peak"]
                tou_formula_parts.append("$$off_peak_kwh * energy_rates.time_of_use.off_peak")
            if "on_peak" in tou:
                tou_values["on_peak"] = tou["on_peak"]
                tou_formula_parts.append("$$on_peak_kwh * energy_rates.time_of_use.on_peak")
            
            if tou_values:
                rules["energy_charge"] = {
                    "label": "energy_rates.time_of_use",
                    "value": tou_values,
                    "formula": " + ".join(tou_formula_parts)
                }

    # DEMAND CHARGES
    if "demand_charges" in sc_data:
        d = sc_data["demand_charges"]

        # Check for various demand charge key patterns
        demand_keys = [k for k in d.keys() if "per_kw" in k or "demand" in k]
        
        if demand_keys:
            # Use the first demand charge found
            first_key = demand_keys[0]
            rules = add_formula(
                rules,
                "demand_charge",
                f"demand_charges.{first_key}",
                d[first_key],
                "billed_demand"
            )

        if "minimum" in d:
            rules["demand_charge"]["minimum_formula"] = \
                f"max({d[first_key]} * $$billed_demand, {d['minimum']})"

    # REACTIVE
    if "demand_charges" in sc_data and "reactive_per_rkva" in sc_data["demand_charges"]:
        rk = sc_data["demand_charges"]["reactive_per_rkva"]
        rules = add_formula(
            rules,
            "reactive_charge",
            "demand_charges.reactive_per_rkva",
            rk,
            "billed_rkva"
        )

    # FINAL BILL FORMULA
    components = [
        name for name in
        ["customer_charge", "energy_charge", "demand_charge", "reactive_charge"]
        if name in rules
    ]
    
    rules["final_bill"] = {
        "formula": " + ".join(components)
    }

    return rules

# -----------------------------------------------------------
# MAIN BUILDER — creates rules.json
# -----------------------------------------------------------
def build_rules_json(input_json_path, output_json_path):

    raw = json.load(open(input_json_path, "r"))

    # normalize SC data
    normalized = {}
    for sc, block in raw.items():
        # Check if this is already normalized tariff data (not LLM response)
        if isinstance(block, dict) and "parsed_json" not in block and "raw_response" not in block:
            # Already normalized tariff data, use as-is
            normalized[sc.lower()] = normalize_keys(block)
        else:
            # LLM response format: has parsed_json
            normalized[sc.lower()] = normalize_keys(block.get("parsed_json", block))

    # apply SC2→SC2D inheritance
    if "sc2" in normalized and "sc2d" in normalized:
        normalized["sc2d"]["demand_charges"] = normalized["sc2"].get("demand_charges")

    rules = {
        "billing_tree": {
            "root": {
                "condition": "service_class",
                "branches": {}
            }
        }
    }

    for sc_name, sc_data in normalized.items():
        rules["billing_tree"]["root"]["branches"][sc_name] = \
            build_formula_tree(sc_name, sc_data, normalized)

    with open(output_json_path, "w") as f:
        json.dump(rules, f, indent=4)

    print(f"Created rules.json at: {output_json_path}")

# -----------------------------------------------------------
# MAIN EXECUTION BLOCK
# -----------------------------------------------------------
if __name__ == "__main__":
    input_path = "D:\\utility-billing-ai\\data\\processed\\Hp_sc_final_tariffs_v4.json"
    output_path = "D:\\utility-billing-ai\\data\\processed\\rules.json"
    build_rules_json(input_path, output_path)
