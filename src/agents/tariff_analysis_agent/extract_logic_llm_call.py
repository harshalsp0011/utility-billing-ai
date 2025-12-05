import os
import sys
import json
import time
import logging
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI
from sqlalchemy.exc import SQLAlchemyError

# Import S3 utilities
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from src.utils.aws_app import download_json_from_s3, upload_json_to_s3, get_s3_key

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

def get_env(key: str, default=None):
    """
    Get environment variable from Streamlit secrets (if available)
    or from os.environ/.env (for local development).
    """
    try:
        import streamlit as st
        if hasattr(st, 'secrets') and key in st.secrets:
            return st.secrets[key]
    except (ImportError, AttributeError, KeyError, FileNotFoundError):
        pass
    return os.getenv(key, default)

API_KEY = get_env("OPENAI_API_KEY")
MODEL = get_env("OPENAI_MODEL")
DATABASE_URL = get_env("DB_URL")

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
    
    # Load from S3 only
    s3_key_input = get_s3_key("processed", Path(input_file).name)
    grouped_data = download_json_from_s3(s3_key_input)
    
    if not grouped_data:
        logger.error(f"Input file not found in S3: {s3_key_input}")
        raise Exception(f"Failed to load from S3: {s3_key_input}")

    # Accept filename from command line argument, otherwise use default
    if len(sys.argv) > 1:
        pdf_filename = sys.argv[1]
        CURRENT_PDF_FILENAME = Path(pdf_filename).name  # Extract just the filename
    else:
        print("No PDF filename provided as argument; using default.")
        logger.info(" No PDF filename provided as argument.")
    UTILITY_NAME = "National Grid NY"
    
    all_definitions_for_file = []
    #db_engine = get_engine()

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

    # 4. Save directly to S3 (no local storage)
    try:
        s3_key_output = get_s3_key("processed", Path(output_file).name)
        if upload_json_to_s3(all_definitions_for_file, s3_key_output):
            logger.info(f"✅ Uploaded to S3: {s3_key_output}")
        else:
            raise Exception(f"Failed to upload to S3: {s3_key_output}")
    except Exception as e:
        logger.error(f"Failed to upload to S3: {e}")
        raise

def _get_default_paths():
    root = Path(__file__).resolve().parents[3]
    input_path = root / "data" / "processed" / "grouped_tariffs.json"
    output_path = root / "data" / "processed" / "final_logic_output.json"
    return input_path, output_path

if __name__ == "__main__":
    in_file, out_file = _get_default_paths()
    # Run directly - will fetch from S3
    extract_tariff_logic_hybrid(str(in_file), str(out_file))