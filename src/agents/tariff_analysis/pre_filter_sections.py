# src/agents/tariff_analysis/pre_filter_sections.py
import json
from pathlib import Path
import sys
import glob

# Resolve repo root and use the canonical filename produced by `tag_sections.py`
PROJECT_ROOT = Path(__file__).resolve().parents[3]
INPUT_PATH = PROJECT_ROOT / "data/processed/sections_tagged.json"
OUTPUT_PATH = PROJECT_ROOT / "data/processed/filtered_tarif_sections.json"

# Optional keywords if tags are missing
KEY_SECTIONS = [
    "service classification",
    "rate",
    "energy charge",
    "demand charge",
    "supply service",
    "customer charge",
    "special provision",
    "rider",
    "surcharge",
    "adjustment"
]

SKIP_SECTIONS = [
    "territory",
    "load zone",
    "service area",
    "form",
    "table of contents",
    "definition",
    "general information"
]


def filter_sections():
    # Load the tagged sections
    if not INPUT_PATH.exists():
        print(f"❌ Expected tagged sections at: {INPUT_PATH}")
        candidates = sorted(glob.glob(str(PROJECT_ROOT / "data" / "processed" / "*.json")))
        if candidates:
            print("Found these files in data/processed/:")
            for p in candidates:
                print(" -", p)
        else:
            print("data/processed/ is empty. Run earlier steps (extract -> tag_sections) to produce the file.")
        sys.exit(1)

    with open(INPUT_PATH, "r") as f:
        data = json.load(f)
    
    sections = data.get("sections", [])
    filtered_sections = []

    for s in sections:
        heading = s.get("heading", "").lower()
        body = s.get("body", "").lower()
        tags = s.get("tags", {})

        # Skip if it's clearly non-rate / administrative
        if any(skip in heading for skip in SKIP_SECTIONS):
            continue
        if tags.get("category", "") in ["territory_section", "misc"]:
            continue

        # Keep if tag says it has rates or heading matches known tariff keywords
        if tags.get("contains_rates") or any(k in heading for k in KEY_SECTIONS):
            filtered_sections.append(s)

    print(f"✅ Filtered down from {len(sections)} to {len(filtered_sections)} relevant sections.")
    with open(OUTPUT_PATH, "w") as f:
        json.dump({"sections": filtered_sections}, f, indent=2)
    print(f"💾 Saved filtered tariff sections to: {OUTPUT_PATH}")


if __name__ == "__main__":
    filter_sections()
