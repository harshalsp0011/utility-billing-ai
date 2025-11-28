import os
import sys
import json
import time
import logging
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI
from sqlalchemy.exc import SQLAlchemyError

# --- PATH SETUP ---
# Ensure we can import from src regardless of where script is run
# This file is in /src/agents/tariff_analysis_agent/
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

# Import after path setup
from src.agents.tariff_analysis_agent import prompts_to_extract_logic as tariff_prompts
from src.database.db_utils import (
    get_or_create_tariff_version,
    save_tariff_logic_to_db,
    get_engine
)

# Load environment variables
load_dotenv()

# --- CONFIGURATION ---
API_KEY = os.getenv("OPENAI_API_KEY")
MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
DATABASE_URL = os.getenv("DB_URL")

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Validation
if not API_KEY:
    raise ValueError("OPENAI_API_KEY environment variable is not set.")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set.")

client = OpenAI(api_key=API_KEY)

def clean_json_response(response_text):
    """Helper to strip markdown code blocks if the LLM adds them."""
    if "```json" in response_text:
        return response_text.split("```json")[1].split("```")[0].strip()
    elif "```" in response_text:
        return response_text.split("```")[1].split("```")[0].strip()
    return response_text.strip()

def extract_tariff_logic_hybrid(input_file, output_file):
    """
    Extracts logic from text, saves to JSON file AND Database.
    """
    logger.info(f"--- Starting Phase 2: Logic Extraction using {MODEL} ---")
    
    if not os.path.exists(input_file):
        logger.error(f"Input file not found: {input_file}")
        return

    with open(input_file, 'r') as f:
        grouped_data = json.load(f)

    # Context Metadata (In future, extract this dynamically)
    UTILITY_NAME = "National Grid NY"
    DOCUMENT_ID = "PSC 220"

    # List to hold all logic for the JSON file output
    all_definitions_for_file = []

    # Initialize DB Engine
    db_engine = get_engine()

    try:
        # Start ONE transaction for the whole batch
        with db_engine.begin() as conn:
            
            # Iterate through each SC found in Phase 1
            for sc_code, data in grouped_data.items():
                logger.info(f"\nProcessing {sc_code}...")
                raw_text = data.get('full_text', "")
                
                # --- 1. GET METADATA ---
                # Use the date extracted in Phase 1 (fallback to today if missing)
                effective_date = data.get('effective_date')
                
                # --- 2. PREPARE DB VERSION ---
                # Get correct version ID for this specific text block's date
                version_id = get_or_create_tariff_version(conn, UTILITY_NAME, DOCUMENT_ID, effective_date)

                if not raw_text:
                    logger.warning(f"   [!] Skipping {sc_code} (No text content)")
                    continue

                try:
                    # --- 3. CALL LLM ---
                    completion = client.chat.completions.create(
                        model=MODEL,
                        messages=[
                            {"role": "system", "content": tariff_prompts.SYSTEM_ROLE},
                            {"role": "user", "content": tariff_prompts.LOGIC_EXTRACTION_PROMPT + f"\n\n--- TEXT TO ANALYZE ---\n{raw_text[:25000]}"}
                        ],
                        temperature=0.0
                    )

                    response_content = completion.choices[0].message.content
                    cleaned_json_str = clean_json_response(response_content)
                    parsed_data = json.loads(cleaned_json_str)

                    # Normalize Output to List
                    extracted_tariffs = []
                    if "tariffs" in parsed_data:
                        extracted_tariffs = parsed_data["tariffs"]
                    elif isinstance(parsed_data, list):
                        extracted_tariffs = parsed_data
                    else:
                        extracted_tariffs = [parsed_data]

                    # --- 4. SAVE TO MEMORY (For JSON File) ---
                    # We verify strict context here to prevent pollution
                    valid_items = []
                    for item in extracted_tariffs:
                        # Optional: Add metadata to the JSON file version for debugging
                        item['metadata'] = {
                            "effective_date": effective_date,
                            "version_id": version_id
                        }
                        valid_items.append(item)
                    
                    all_definitions_for_file.extend(valid_items)

                    # --- 5. SAVE TO DB (For Production) ---
                    # Use your centralized util function
                    saved_count = save_tariff_logic_to_db(conn, version_id, valid_items)
                    
                    logger.info(f"   [+] Extracted & Saved {saved_count} blocks for {sc_code} (Date: {effective_date})")

                except json.JSONDecodeError:
                    logger.error(f"   [!] Error: Failed to parse valid JSON from LLM response for {sc_code}")
                except Exception as e:
                    logger.error(f"   [!] API/Processing Error: {str(e)}")

                time.sleep(1) # Rate limiting

            logger.info("--- Database Transaction Committed ---")

    except SQLAlchemyError as e:
        logger.error(f"Database Transaction Failed: {e}")
        return # Stop if DB fails

    # --- 6. WRITE JSON FILE ---
    # This ensures you still have the file for your Streamlit app
    try:
        with open(output_file, 'w') as f:
            json.dump(all_definitions_for_file, f, indent=2)
        logger.info(f"✅ Backup JSON saved to {output_file}")
    except Exception as e:
        logger.error(f"Failed to write JSON file: {e}")

def _get_default_paths():
    """Resolve default input/output paths relative to repo root."""
    # Input: grouped_tariffs.json (from Phase 1)
    # Output: tariff_definitions.json (for Streamlit/Debugging)
    root = Path(__file__).resolve().parents[3]
    input_path = root / "data" / "processed" / "grouped_tariffs.json"
    output_path = root / "data" / "processed" / "tariff_definitions.json"
    return input_path, output_path

if __name__ == "__main__":
    input_file, output_file = _get_default_paths()
    
    if not input_file.exists():
        # Fallback for local testing
        if Path("grouped_tariffs.json").exists():
            input_file = Path("grouped_tariffs.json")
            output_file = Path("tariff_definitions.json")
        else:
            raise FileNotFoundError(
                f"Input file not found: {input_file}\n"
                "Please ensure grouped_tariffs.json exists in data/processed/ (run group_tariffs.py first)"
            )
    
    # Ensure output dir exists
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    extract_tariff_logic_hybrid(str(input_file), str(output_file))