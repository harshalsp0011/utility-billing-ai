# extract_logic.py
import os
import json
import time
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI
import tariff_prompts  # Importing the prompt file we created above

# Load environment variables from .env file
load_dotenv()

# Configuration
API_KEY = os.getenv("OPENAI_API_KEY")
MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

if not API_KEY:
    raise ValueError(
        "OPENAI_API_KEY environment variable is not set.\n"
        "Set it with: export OPENAI_API_KEY=sk-your-key-here\n"
        "Or add it to your .env file in the project root."
    )

client = OpenAI(api_key=API_KEY)

def clean_json_response(response_text):
    """
    Helper to strip markdown code blocks if the LLM adds them.
    e.g., ```json ... ``` -> ...
    """
    if "```json" in response_text:
        response_text = response_text.split("```json")[1].split("```")[0]
    elif "```" in response_text:
        response_text = response_text.split("```")[1].split("```")[0]
    return response_text.strip()

def extract_tariff_logic(input_file, output_file):
    print(f"--- Starting Phase 2: Logic Extraction using {MODEL} ---")
    
    # 1. Load the Grouped Data (Output from Phase 1)
    try:
        with open(input_file, 'r') as f:
            grouped_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: {input_file} not found. Please run Phase 1 script first.")
        return

    final_definitions = []

    # 2. Iterate through each SC found in Phase 1
    for sc_code, data in grouped_data.items():
        print(f"\nProcessing {sc_code}...")
        raw_text = data.get('full_text', "")

        if not raw_text:
            print(f"   [!] Skipping {sc_code} (No text content)")
            continue

        try:
            # 3. Call OpenAI API
            completion = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": tariff_prompts.SYSTEM_ROLE},
                    {"role": "user", "content": tariff_prompts.LOGIC_EXTRACTION_PROMPT + f"\n\n--- TEXT TO ANALYZE ---\n{raw_text}"}
                ],
                temperature=0.0  # Keep it deterministic for code generation
            )

            response_content = completion.choices[0].message.content
            cleaned_json_str = clean_json_response(response_content)

            # 4. Parse JSON
            parsed_data = json.loads(cleaned_json_str)

            # Check if LLM returned the expected wrapper
            if "tariffs" in parsed_data:
                extracted_tariffs = parsed_data["tariffs"]
                print(f"   [+] Success: Extracted {len(extracted_tariffs)} definition(s) from {sc_code}")
                final_definitions.extend(extracted_tariffs)
            else:
                # Fallback if LLM forgot the wrapper
                print(f"   [~] Warning: LLM missed 'tariffs' key, attempting direct append.")
                if isinstance(parsed_data, list):
                    final_definitions.extend(parsed_data)
                else:
                    final_definitions.append(parsed_data)

        except json.JSONDecodeError:
            print(f"   [!] Error: Failed to parse valid JSON from LLM response for {sc_code}")
            # Optional: Save the failed raw text to a log file for debugging
        except Exception as e:
            print(f"   [!] API Error: {str(e)}")

        # Sleep briefly to avoid rate limits if processing many items
        time.sleep(1)

    # 5. Save the Master Logic File
    with open(output_file, 'w') as f:
        json.dump(final_definitions, f, indent=2)
    
    print(f"\n--- Extraction Complete ---")
    print(f"Saved {len(final_definitions)} tariff definitions to {output_file}")


def _get_default_paths():
    """Resolve default input/output paths relative to repo root."""
    # extract_logic.py is in: <root>/src/agents/tariff_analysis/
    # root is 3 levels up
    root = Path(__file__).resolve().parents[3]
    input_path = root / "data" / "processed" / "grouped_tariffs.json"
    output_path = root / "data" / "processed" / "tariff_definitions.json"
    return input_path, output_path


if __name__ == "__main__":
    input_file, output_file = _get_default_paths()
    
    if not input_file.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_file}\n"
            "Please ensure grouped_tariffs.json exists in data/processed/ (run group_tariffs.py first)"
        )
    
    extract_tariff_logic(str(input_file), str(output_file))