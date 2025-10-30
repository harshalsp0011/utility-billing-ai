"""
calculation_engine.py
---------------------
🧮 Performs the tariff-based billing calculation logic.

Purpose:
--------
Given usage (kWh), demand (kW), and the tariff rule formula,
this module computes the "expected" amount a customer
should have been charged according to tariff.

Workflow:
---------
1️⃣ Receives one record (row) from merged DataFrame.
2️⃣ Applies energy_rate × usage_kwh (+ demand_rate × demand_kw).
3️⃣ Enforces minimum charge if applicable.
4️⃣ Returns computed expected charge.

Inputs:
-------
- Row (with usage_kwh, demand_kw, energy_rate, demand_rate, min_charge)

Outputs:
--------
- Float (expected charge)

Depends On:
-----------
- pandas
- src.utils.logger
"""

from src.utils.logger import get_logger

logger = get_logger(__name__)

def compute_expected_charge(row):
    """
    Compute the expected charge for a given account/month.
    """
    try:
        usage = float(row.get("usage_kwh", 0))
        demand = float(row.get("demand_kw", 0))
        energy_rate = float(row.get("energy_rate", 0))
        demand_rate = float(row.get("demand_rate", 0))
        min_charge = float(row.get("min_charge", 0))

        expected = (usage * energy_rate) + (demand * demand_rate)
        expected = max(expected, min_charge)

        return round(expected, 2)

    except Exception as e:
        logger.error(f"❌ Error computing expected charge: {e}")
        return 0.0
