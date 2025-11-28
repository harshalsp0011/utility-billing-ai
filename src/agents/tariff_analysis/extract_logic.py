import os
import json
import time
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI
import tariff_prompts

load_dotenv()

API_KEY = os.getenv("OPENAI_API_KEY")
MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

client = OpenAI(api_key=API_KEY)

def clean_json_response(response_text):
    if "```json" in response_text:
        return response_text.split("```json")[1].split("```")[0].strip()
    elif "```" in response_text:
        return response_text.split("```")[1].split("```")[0].strip()
    return response_text.strip()

def extract_tariff_logic(input_file, output_file):
    print(f"--- Starting Phase 2: Logic Extraction ---")
    
    with open(input_file, 'r') as f:
        grouped_data = json.load(f)

    final_definitions = []

    for sc_code, data in grouped_data.items():
        print(f"Processing {sc_code}...")
        raw_text = data.get('full_text', "")
        
        # --- CRITICAL UPDATE: CAPTURE METADATA ---
        # We grab the date found by Phase 1 so we can pass it to the DB later
        effective_date = data.get('effective_date')

        if not raw_text:
            continue

        try:
            completion = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": tariff_prompts.SYSTEM_ROLE},
                    {"role": "user", "content": tariff_prompts.LOGIC_EXTRACTION_PROMPT + f"\n\n--- TEXT TO ANALYZE ---\n{raw_text}"}
                ],
                temperature=0.0
            )

            response_content = completion.choices[0].message.content
            cleaned_json = clean_json_response(response_content)
            parsed_data = json.loads(cleaned_json)

            # Handle List vs Dict wrapper
            extracted_items = []
            if "tariffs" in parsed_data:
                extracted_items = parsed_data["tariffs"]
            elif isinstance(parsed_data, list):
                extracted_items = parsed_data
            else:
                extracted_items = [parsed_data]

            # --- STAMP METADATA ONTO LOGIC ---
            for item in extracted_items:
                item['metadata'] = {
                    "effective_date": effective_date,
                    "utility": "National Grid NY",
                    "source_doc": "PSC 220"
                }
                final_definitions.append(item)
                
            print(f"   [+] Extracted {len(extracted_items)} blocks")

        except Exception as e:
            print(f"   [!] Error: {e}")
        
        time.sleep(1)

    with open(output_file, 'w') as f:
        json.dump(final_definitions, f, indent=2)
    
    print(f"Saved {len(final_definitions)} definitions to {output_file}")

if __name__ == "__main__":
    # Resolve paths (assuming running from src/processing or root)
    # Adjust as needed for your specific folder structure
    base_path = Path("data/processed") 
    # If running directly from src/processing, go up levels
    if not base_path.exists():
        base_path = Path("../../data/processed")

    input_path = base_path / "grouped_tariffs.json"
    output_path = base_path / "tariff_definitions.json"
    
    extract_tariff_logic(str(input_path), str(output_path))