import os
import sys
import json
import time
import logging
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI
from sqlalchemy.exc import SQLAlchemyError

# Add project root to path (file is 3 levels deep: src/agents/tariff_analysis_agent/)
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from src.agents.tariff_analysis_agent import prompts_to_extract_logic as tariff_prompts


# Use centralized DB utilities (ORM)
from src.database.db_utils import (
    register_tariff_document,
    save_tariff_logic_version,
    get_engine,
)

load_dotenv()

API_KEY = os.getenv("OPENAI_API_KEY")
MODEL = os.getenv("OPENAI_MODEL")
DATABASE_URL = os.getenv("DB_URL")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if not API_KEY or not DATABASE_URL:
    raise ValueError("Missing API_KEY or DATABASE_URL.")

client = OpenAI(api_key=API_KEY)

def clean_json_response(response_text):
    if "```json" in response_text:
        return response_text.split("```json")[1].split("```")[0].strip()
    elif "```" in response_text:
        return response_text.split("```")[1].split("```")[0].strip()
    return response_text.strip()

def extract_tariff_logic_hybrid(input_file, output_file):
    logger.info(f"--- Starting Phase 2: Logic Extraction ---")
    
    if not os.path.exists(input_file):
        logger.error(f"Input file not found: {input_file}")
        return

    with open(input_file, 'r') as f:
        grouped_data = json.load(f)

    # Hardcoded Filename for now (In prod, pass this as an argument)
    CURRENT_PDF_FILENAME = "NationalGrid_Tariff-NewYork.pdf"
    UTILITY_NAME = "National Grid NY"
    
    all_definitions_for_file = []
    db_engine = get_engine()

    try:
        # 1. Register Source Document (no conn needed - uses session internally)
        doc_id = register_tariff_document(
            filename=CURRENT_PDF_FILENAME,
            utility_name=UTILITY_NAME
        )
        
        for sc_code, data in grouped_data.items():
            logger.info(f"\nProcessing {sc_code}...")
            raw_text = data.get('full_text', "")
            effective_date = data.get('effective_date')

            if not raw_text:
                continue

            try:
                # 2. LLM Extraction
                completion = client.chat.completions.create(
                    model=MODEL,
                    messages=[
                        {"role": "system", "content": tariff_prompts.SYSTEM_ROLE},
                        {"role": "user", "content": tariff_prompts.LOGIC_EXTRACTION_PROMPT + f"\n\n--- TEXT FOR CLASS: {sc_code} ---\n{raw_text[:25000]}"}
                    ],
                    temperature=0.0
                )

                resp = completion.choices[0].message.content
                parsed_data = json.loads(clean_json_response(resp))

                extracted_tariffs = []
                if "tariffs" in parsed_data:
                    extracted_tariffs = parsed_data["tariffs"]
                elif isinstance(parsed_data, list):
                    extracted_tariffs = parsed_data
                else:
                    extracted_tariffs = [parsed_data]

                # 3. Save to DB & File
                for item in extracted_tariffs:
                    # Add Metadata
                    item['metadata'] = {
                        "effective_date": effective_date,
                        "filename": CURRENT_PDF_FILENAME
                    }
                    
                    # Validate Context (Prevent SC3 inside SC7)
                    extracted_code = item.get('sc_code', 'UNKNOWN')
                    if sc_code not in extracted_code and extracted_code not in sc_code:
                         logger.warning(f"   [⚠️] Cross-Contamination Warning: {sc_code} vs {extracted_code}")

                    # Add to file list
                    all_definitions_for_file.append(item)
                    
                    # Save to DB (Table: tariff_logic_versions)
                    save_tariff_logic_version(doc_id, item)
                
                logger.info(f"   [+] Saved {len(extracted_tariffs)} blocks for {sc_code}")

            except Exception as e:
                logger.error(f"   [!] Processing Error: {str(e)}")
            
            time.sleep(1)

    except SQLAlchemyError as e:
        logger.error(f"DB Error: {e}")

    # 4. Save Backup JSON
    try:
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w') as f:
            json.dump(all_definitions_for_file, f, indent=2)
        logger.info(f"✅ Backup JSON saved to {output_file}")
    except Exception as e:
        logger.error(f"Failed to write JSON: {e}")

def _get_default_paths():
    root = Path(__file__).resolve().parents[3]
    input_path = root / "data" / "processed" / "grouped_tariffs.json"
    output_path = root / "data" / "processed" / "tariff_definitions.json"
    return input_path, output_path

if __name__ == "__main__":
    in_file, out_file = _get_default_paths()
    if not in_file.exists():
        # Fallback check
        if Path("grouped_tariffs.json").exists():
            in_file = Path("grouped_tariffs.json")
            out_file = Path("tariff_definitions.json")
    
    if in_file.exists():
        extract_tariff_logic_hybrid(str(in_file), str(out_file))
    else:
        logger.error(f"Input file not found: {in_file}")