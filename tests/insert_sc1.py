import sys
from pathlib import Path

# ensure project root is on sys.path (tests/ -> project root)
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.database.db_utils import insert_service_classification, insert_sc1_rate_detail

# Step 1: Insert SC1 into the classification table
sc_data = {
    "code": "SC1",
    "description": "SC1 rate classification"
}
sc1_id = insert_service_classification(sc_data)
print(f"Inserted ServiceClassification, got id {sc1_id}")

# Step 2: Insert rate details for SC1
rate_data = {
    "service_classification_id": sc1_id,
    "effective_date": "2025-09-01",
    "basic_service_charge": 19,
    "monthly_minimum_charge": 19,
    "energy_rate_all_hours": None,
    "on_peak": 0.11494,
    "off_peak": 0.0097,
    "super_peak": 0.11494,
    "distribution_delivery": "0.11494/0.00970"
}
insert_sc1_rate_detail(rate_data)
print("Inserted SC1 rate detail.")
