"""
Step 1.5 – Keyword & Semantic Tagging
Uses config keywords to auto-label each tariff section before LLM parsing.
"""

import re
import json
import yaml
from rapidfuzz import fuzz, process
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
INPUT_JSON = PROJECT_ROOT / "data/processed/sections.json"
CONFIG_YAML = PROJECT_ROOT / "src/config/tariff_patterns.yaml"
OUTPUT_JSON = PROJECT_ROOT / "data/processed/sections_tagged.json"

def load_sections(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)["sections"]

def load_patterns(yaml_path):
    with open(yaml_path, "r") as f:
        return yaml.safe_load(f)

def detect_keywords(text, keywords, threshold):
    found = set()
    for label, variants in keywords.items():
        for v in variants:
            if re.search(v, text, re.IGNORECASE):
                found.add(label)
                break
            # Fuzzy fallback
            for token in text.split():
                if fuzz.ratio(token.lower(), v.lower()) > threshold:
                    found.add(label)
                    break
    return list(found)

def categorize_section(heading, patterns):
    """Determine high-level category based on which heading regex matched."""
    h = heading.lower()
    for cat, regex_list in patterns["headings"].items():
        for r in regex_list:
            if re.search(r, h, re.IGNORECASE):
                return cat
    return "misc"

def tag_sections(sections, patterns):
    tagged = []
    kw_threshold = patterns.get("fuzzy_thresholds", {}).get("keyword_match", 70)

    for s in sections:
        text = (s["heading"] + "\n" + s["body"]).lower()
        keywords_found = detect_keywords(text, patterns["keywords"], kw_threshold)
        category = categorize_section(s["heading"], patterns)
        contains_rates = any(k in ["on_peak","off_peak","demand","energy","distribution"]
                             for k in keywords_found)
        s["tags"] = {
            "category": category,
            "contains_rates": contains_rates,
            "keywords_found": keywords_found
        }
        tagged.append(s)
    return tagged

def save_output(sections, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"sections": sections}, f, indent=2)
    print(f"✅ Tagged sections saved to {path}")

if __name__ == "__main__":
    print("🔍 Loading segmented sections...")
    sections = load_sections(INPUT_JSON)

    print("⚙️ Loading pattern config...")
    patterns = load_patterns(CONFIG_YAML)

    print("🏷️ Tagging sections using config keywords...")
    tagged = tag_sections(sections, patterns)

    print(f"💾 Saving {len(tagged)} tagged sections...")
    save_output(tagged, OUTPUT_JSON)

    print("✅ Done. Proceed to Step 1.6 – LLM Reasoning & Extraction.")
