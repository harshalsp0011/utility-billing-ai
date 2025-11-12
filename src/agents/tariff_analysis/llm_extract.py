"""
Step 1.6 – LLM Reasoning & Extraction
Uses a local or remote LLM to extract structured tariff rules from tagged sections.
"""

import json
import subprocess
from pathlib import Path
import yaml
import time
import shutil

#from tests.test_ollama_ping import OLLAMA_PATH
OLLAMA_PATH = "/usr/local/bin/ollama" 

# ---------- CONFIG ----------
PROJECT_ROOT = Path(__file__).resolve().parents[3]
#INPUT_JSON = PROJECT_ROOT / "data/processed/sections_tagged.json"
INPUT_JSON = PROJECT_ROOT / "data/processed/filtered_tarif_sections.json"
OUTPUT_JSON = PROJECT_ROOT / "data/processed/tariff_rules_extracted.json"
CONFIG_PATH = PROJECT_ROOT / "src/config/llm_config.yaml"

# ---------- CONFIGURATION ----------
def load_config():
    """Load model configuration (supports Ollama or API in future)."""
    defaults = {
        "model_backend": "ollama",
        "ollama_model": "llama3:8b",
        "temperature": 0.1,
        "max_retries": 2,
    }

    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, "r") as f:
                cfg = yaml.safe_load(f) or {}
            if not isinstance(cfg, dict):
                print(f"⚠️ Warning: {CONFIG_PATH} did not contain a mapping; using defaults.")
                return defaults
            # Merge defaults with provided config (provided values override defaults)
            merged = {**defaults, **cfg}
            return merged
        except Exception as e:
            print(f"⚠️ Failed to load config {CONFIG_PATH}: {e}. Using defaults.")
            return defaults
    else:
        # default if config missing
        return defaults

# ---------- CORE FUNCTIONS ----------
def load_tagged_sections(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)["sections"]

def make_prompt(section_text: str) -> str:
    """
    Strict JSON extraction prompt.
    Forces the LLM to return *only* one valid JSON object following the schema below.
    """
    return f"""
You are a domain expert in electricity tariffs.

Read the given text and extract key rate and condition information.
Return a *single valid JSON object* following this exact schema and nothing else.

### SCHEMA (MUST MATCH EXACTLY):
{{
  "service_class": "string or null",
  "rate_blocks": [
    {{
      "type": "on_peak|off_peak|demand|distribution|energy|customer_charge|other",
      "rate": float,
      "unit": "kWh|kW|month|USD|other"
    }}
  ],
  "conditions": ["list of strings describing eligibility, voltage, time, etc."],
  "notes": "optional free text or null"
}}

### OUTPUT RULES:
- Output must begin with '{{' and end with '}}'
- No commentary, explanation, markdown, or natural language
- If you cannot find any rate information, return: {{}}
- Do NOT include comments, notes, or explanations inside JSON (no // or #)
- All null values must be plain null, not text

### TEXT TO ANALYZE:
<<<
{section_text}
>>>
"""


import re

def clean_json_text(text: str) -> str:
    """
    Remove common JSON formatting issues that models introduce.
    """
    # Remove inline comments
    text = re.sub(r'//.*', '', text)
    text = re.sub(r'#.*', '', text)
    # Replace trailing commas
    text = re.sub(r',\s*}', '}', text)
    text = re.sub(r',\s*]', ']', text)
    # Strip weird quotes
    return text.strip()


def call_ollama(model, prompt):
    """
    Call Ollama locally via subprocess, capture text output,
    and extract JSON from it. Handles both plain and streamed output.
    """
    try:
        # Run Ollama model locally
        result = subprocess.run(
            [OLLAMA_PATH, "run", model],
            input=prompt.encode("utf-8"),
            capture_output=True,
            timeout=120
        )
        out = result.stdout.decode("utf-8", errors="ignore").strip()

        if not out:
            print("⚠️ No output from Ollama (check model name or running status).")
            return None

        # Extract first valid JSON block
        json_start = out.find("{")
        json_end = out.rfind("}")
        if json_start == -1 or json_end == -1:
            # Print first few characters for debugging
            print("⚠️ Ollama returned non-JSON output:")
            print(out[:300], "...")
            return None

        json_str = out[json_start:json_end + 1]
        try:
            json_str = clean_json_text(json_str)
            return json.loads(json_str)
        except json.JSONDecodeError:
            print("⚠️ JSON parse failed, output snippet:")
            print(json_str[:300], "...")
            return None

    except FileNotFoundError:
        print("❌ Ollama command not found. Make sure Ollama is installed and in PATH.")
        return None
    except subprocess.TimeoutExpired:
        print("⚠️ Ollama call timed out (try smaller sections or model).")
        return None


def call_openai_api(prompt, model="gpt-4o-mini"):
    """Example placeholder for later API upgrade."""
    from openai import OpenAI
    client = OpenAI()
    resp = client.chat.completions.create(
        model=model,
        response_format={"type": "json_object"},
        messages=[{"role": "user", "content": prompt}]
    )
    return json.loads(resp.choices[0].message["content"])

def extract_rules(sections, config):
    results = []
    territory_info = [] 
    model_type = config.get("model_backend", "ollama")

    for s in sections:
        # Skip sections that don't contain rate info
        if not s.get("tags", {}).get("contains_rates"):
            continue  

        # Save "territory" or "load zone" sections separately
        heading_lower = s.get("heading", "").lower()
        if any(k in heading_lower for k in ["territory", "load zone", "sub-zone", "service area", "contained"]):
            print(f"📍 Saving service area section separately: {s['heading']}")
            territory_info.append({
                "heading": s["heading"],
                "pages": s["pages"],
                "text": s["body"]
            })
            continue

        # Skip unrelated sections
        if not any(k in heading_lower for k in ["service classification", "rate", "supply", "demand", "energy charge"]):
            print(f"⏭️  Skipping non-tariff section: {s['heading']}")
            continue

        print(f"🧠 Processing section: {s['heading']}")
        print(f"   📄 Pages: {s.get('pages', [])}")

        # --- NEW: Chunking logic for large sections ---
        body_text = s.get("body", "")
        max_len = config.get("max_section_length", 10000)  # Max characters per chunk
        overlap = 200  # Keep overlap for context continuity

        if len(body_text) > max_len:
            print(f"⚠️  Splitting large section: {s['heading']} (len={len(body_text)})")
            chunks = [body_text[i:i + max_len] for i in range(0, len(body_text), max_len - overlap)]
        else:
            chunks = [body_text]

        all_chunks_data = []

        for idx, chunk in enumerate(chunks):
            print(f"   🧩 Chunk {idx+1}/{len(chunks)} (len={len(chunk)})")
            prompt = make_prompt(chunk)

            data_chunk = None
            for attempt in range(config.get("max_retries", 2)):
                if model_type == "ollama":
                    data_chunk = call_ollama(config["ollama_model"], prompt)
                elif model_type == "openai":
                    data_chunk = call_openai_api(prompt, model=config["api_model"])
                else:
                    raise ValueError(f"Unknown model_backend: {model_type}")

                if data_chunk:
                    break
                print(f"      Retrying chunk ({attempt+1})...")
                time.sleep(2)

            if not data_chunk:
                data_chunk = {"error": "model_failed", "chunk": idx+1}

            all_chunks_data.append(data_chunk)

        # --- Merge all chunk results into one unified JSON ---
        merged_blocks = []
        for d in all_chunks_data:
            if isinstance(d, dict) and "rate_blocks" in d:
                merged_blocks.extend(d["rate_blocks"])

        merged_data = {"rate_blocks": merged_blocks} if merged_blocks else {"chunks": all_chunks_data}

        record = {
            "section_heading": s["heading"],
            "pages": s.get("pages", []),
            "rules": merged_data
        }
        results.append(record)

    # Save extracted service areas separately
    with open("data/processed/territory_sections.json", "w") as f:
        json.dump({"territories": territory_info}, f, indent=2)

    return results


def save_output(data, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"tariff_rules": data}, f, indent=2)
    print(f"✅ Saved structured tariff rules to {path}")

# ---------- MAIN ----------
if __name__ == "__main__":
    print("⚙️ Loading configuration...")
    config = load_config()

    print("📄 Loading tagged sections...")
    sections = load_tagged_sections(INPUT_JSON)

    page_start = config.get("page_start")
    page_end = config.get("page_end")

    if page_start and page_end:
        original_count = len(sections)
        sections = [
        s for s in sections
        if any(page_start <= p <= page_end for p in s.get("pages", []))
        ]
        
    print(f"📄 Filtering sections to pages {page_start}-{page_end} "
          f"({len(sections)}/{original_count} sections retained)")

    print("🤖 Extracting tariff rules using model:", config["model_backend"])
    extracted = extract_rules(sections, config)

    print("💾 Saving output...")
    save_output(extracted, OUTPUT_JSON)

    print("✅ Done. Proceed to Step 1.7 – Schema Validation.")
