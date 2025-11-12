"""
Step 1.4 – Dynamic Section Segmentation
Groups raw pages into logical tariff sections (service classes, rules, rates).
"""

import re
import json
import yaml
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_JSON = PROJECT_ROOT / "data/processed/raw_extracted_tarif.json"
CONFIG_YAML = PROJECT_ROOT / "src/config/tariff_patterns.yaml"
OUTPUT_JSON = PROJECT_ROOT / "data/processed/sections.json"

def load_raw_pdf_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)["pages"]

def load_patterns(yaml_path):
    with open(yaml_path, "r") as f:
        return yaml.safe_load(f)

def find_section_starts(text, patterns):
    # Combine all heading regexes
    combined = []
    for group in patterns["headings"].values():
        combined.extend(group)
    regex = "(" + "|".join(combined) + ")"
    return list(re.finditer(regex, text, flags=re.IGNORECASE | re.MULTILINE))

def segment_pages(pages, patterns):
    sections = []
    current = None

    for page in pages:
        text = page["text"]
        matches = find_section_starts(text, patterns)

        if matches:
            for m in matches:
                # Start a new section
                if current:
                    sections.append(current)
                current = {
                    "heading": m.group().strip(),
                    "body": text[m.end():].strip(),
                    "pages": [page["page_number"]],
                    "tables": page["tables"]
                }
        else:
            # Append text to current section (if any)
            if current:
                current["body"] += "\n" + text
                current["pages"].append(page["page_number"])
                current["tables"].extend(page["tables"])
            else:
                # preamble or cover pages
                current = {
                    "heading": "DOCUMENT INTRO",
                    "body": text,
                    "pages": [page["page_number"]],
                    "tables": page["tables"]
                }
    if current:
        sections.append(current)
    return sections

def save_sections(sections, output_path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({"sections": sections}, f, indent=2)
    print(f"✅ Segmented sections saved to {output_path}")

if __name__ == "__main__":
    print("🔍 Loading extracted PDF JSON...")
    pages = load_raw_pdf_json(RAW_JSON)

    print("⚙️ Loading regex patterns...")
    patterns = load_patterns(CONFIG_YAML)

    print("📄 Segmenting pages into logical sections...")
    sections = segment_pages(pages, patterns)

    print(f"💾 Saving {len(sections)} sections...")
    save_sections(sections, OUTPUT_JSON)

    print("✅ Done. Proceed to Step 1.5 – Keyword + Semantic Tagging.")
