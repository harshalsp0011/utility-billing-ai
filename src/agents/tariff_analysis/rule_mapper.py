"""
rule_mapper.py
---------------
🧮 Maps extracted tariff rate data into computational logic.

Purpose:
--------
Converts raw rate data (energy_rate, demand_rate, min_charge)
into structured JSON "formula templates" that can later be used
by the Bill Comparison agent to calculate expected charges.

Workflow:
---------
1️⃣ Receive structured rate DataFrame (from tariff_extractor).
2️⃣ Map each row into a rate formula dictionary.
3️⃣ Save output as JSON file in /data/processed/.
4️⃣ Optionally insert into database (tariff_rules table).

Inputs:
-------
- DataFrame with rate_code, energy_rate, demand_rate, min_charge

Outputs:
--------
- JSON file: /data/processed/tariff_rules.json

Depends On:
-----------
- pandas
- src.utils.helpers
- src.utils.logger
"""

import pandas as pd
from src.utils.helpers import save_json
from src.utils.logger import get_logger

logger = get_logger(__name__)

def map_rules_to_json(df: pd.DataFrame, output_name="tariff_rules.json"):
    """
    Converts tariff DataFrame into JSON formulas.
    """
    try:
        rules = []
        for _, row in df.iterrows():
            rule = {
                "rate_code": row["rate_code"],
                "formula": f"(usage_kwh * {row['energy_rate']})",
                "energy_rate": row["energy_rate"],
                "demand_rate": row.get("demand_rate", 0),
                "min_charge": row.get("minimum_charge", 0)
            }
            rules.append(rule)

        save_json({"tariff_rules": rules}, "processed", output_name)
        logger.info(f"💾 Saved {len(rules)} mapped tariff rules → data/processed/{output_name}")
        return rules

    except Exception as e:
        logger.error(f"❌ Failed to map tariff rules: {e}")
        return []
