"""
tariff_extractor.py
-------------------
💡 Extracts rate schedules, usage thresholds, and pricing tiers from parsed tariff text.

Purpose:
--------
This module processes the text output from `tariff_parser.py` and identifies
specific rate classes (e.g., VE-100, VE-200) along with their parameters.

Workflow:
---------
1️⃣ Receive full text (as a string) from tariff_parser.
2️⃣ Use regex or keyword search to locate rate schedule sections.
3️⃣ Extract key data like:
    - Energy rate ($/kWh)
    - Demand rate ($/kW)
    - Minimum charge
    - Thresholds
4️⃣ Build structured DataFrame or dict for each rate schedule.

Inputs:
-------
- Raw text (from tariff_parser)

Outputs:
--------
- Structured DataFrame or dict of rate schedules

Depends On:
-----------
- re (regex)
- pandas
- src.utils.logger
- src.utils.helpers
"""

import re
import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)

def extract_tariff_rules(tariff_text: str):
    """
    Finds and structures rate rules from tariff text.
    """
    try:
        # Example pattern: Rate Schedule VE-100, Energy Charge $0.065 per kWh
        pattern = r"Rate Schedule\s+(VE-\d+).*?Energy\s+Charge\s+\$?([\d.]+).*?kWh"
        matches = re.findall(pattern, tariff_text, flags=re.DOTALL)

        rule_list = []
        for rate_code, energy_rate in matches:
            rule_list.append({
                "rate_code": rate_code,
                "energy_rate": float(energy_rate),
                "demand_rate": None,
                "minimum_charge": None
            })

        df = pd.DataFrame(rule_list)
        logger.info(f"✅ Extracted {len(df)} tariff rules.")
        return df

    except Exception as e:
        logger.error(f"❌ Error extracting tariff rules: {e}")
        return pd.DataFrame()
