import os
import json
import time
import logging
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import prompts_to_extract_logic  # Importing the prompt file we created above

# Load environment variables
load_dotenv()

# Configuration
API_KEY = os.getenv("OPENAI_API_KEY")
MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")
DATABASE_URL = os.getenv("DATABASE_URL")  # e.g., postgresql://user:pass@localhost/dbname

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Validation
if not API_KEY:
    raise ValueError("OPENAI_API_KEY environment variable is not set.")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set.")

client = OpenAI(api_key=API_KEY)
db_engine = create_engine(DATABASE_URL)

def clean_json_response(response_text):
    """Helper to strip markdown code blocks if the LLM adds them."""
    if "```json" in response_text:
        return response_text.split("```json")[1].split("```")[0].strip()
    elif "```" in response_text:
        return response_text.split("```")[1].split("```")[0].strip()
    return response_text.strip()

def get_or_create_tariff_version(conn, utility_name, document_id, effective_date):
    """
    Checks if a tariff version exists for the utility and date.
    If not, creates a new entry in 'tariff_versions'.
    Returns the version_id.
    """
    # 1. Check if version exists
    check_query = text("""
        SELECT id FROM tariff_versions 
        WHERE utility_name = :uname 
          AND tariff_document_id = :doc_id 
          AND effective_date = :eff_date
    """)
    
    result = conn.execute(check_query, {
        "uname": utility_name, 
        "doc_id": document_id, 
        "eff_date": effective_date
    }).fetchone()

    if result:
        return result[0]

    # 2. Create new version if not found
    insert_query = text("""
        INSERT INTO tariff_versions (utility_name, tariff_document_id, effective_date)
        VALUES (:uname, :doc_id, :eff_date)
        RETURNING id
    """)
    
    result = conn.execute(insert_query, {
        "uname": utility_name, 
        "doc_id": document_id, 
        "eff_date": effective_date
    }).fetchone()
    
    logger.info(f"Created NEW Tariff Version ID: {result[0]} for Date: {effective_date}")
    return result[0]

def save_logic_to_db(conn, version_id, definitions):
    """
    Inserts the extracted logic JSON objects into the 'tariff_logic' table.
    """
    insert_query = text("""
        INSERT INTO tariff_logic (version_id, sc_code, logic_json)
        VALUES (:vid, :code, :logic)
    """)
    
    count = 0
    for item in definitions:
        sc_code = item.get('sc_code', 'UNKNOWN')
        
        # Serialize the logic object to JSON string for storage
        logic_json_str = json.dumps(item)
        
        conn.execute(insert_query, {
            "vid": version_id,
            "code": sc_code,
            "logic": logic_json_str
        })
        count += 1
    
    return count

def extract_tariff_logic_to_db(input_file):
    logger.info(f"--- Starting Phase 2: Logic Extraction to Database using {MODEL} ---")
    
    if not os.path.exists(input_file):
        logger.error(f"Input file not found: {input_file}")
        return

    with open(input_file, 'r') as f:
        grouped_data = json.load(f)

    # Metadata Context (Could be extracted from filename or args in future)
    UTILITY_NAME = "National Grid NY"
    DOCUMENT_ID = "PSC 220"

    try:
        # Start a DB Transaction
        with db_engine.begin() as conn:
            
            total_saved = 0

            # Iterate and Process Each SC
            for sc_code, data in grouped_data.items():
                logger.info(f"\nProcessing {sc_code}...")
                raw_text = data.get('full_text', "")
                
                # --- DYNAMIC DATE ---
                # Use the date extracted in Phase 1 (group_tariffs.py)
                # Fallback to today's date or a default if missing
                effective_date = data.get('effective_date', '2025-09-01')
                
                # Get correct version ID for this specific text block's date
                version_id = get_or_create_tariff_version(conn, UTILITY_NAME, DOCUMENT_ID, effective_date)

                if not raw_text:
                    logger.warning(f"   [!] Skipping {sc_code} (No text content)")
                    continue

                try:
                    # API Call
                    completion = client.chat.completions.create(
                        model=MODEL,
                        messages=[
                            {"role": "system", "content": tariff_prompts.SYSTEM_ROLE},
                            {"role": "user", "content": tariff_prompts.LOGIC_EXTRACTION_PROMPT + f"\n\n--- TEXT TO ANALYZE ---\n{raw_text}"}
                        ],
                        temperature=0.0
                    )

                    response_content = completion.choices[0].message.content
                    cleaned_json_str = clean_json_response(response_content)
                    parsed_data = json.loads(cleaned_json_str)

                    # Normalize Output
                    extracted_tariffs = []
                    if "tariffs" in parsed_data:
                        extracted_tariffs = parsed_data["tariffs"]
                    elif isinstance(parsed_data, list):
                        extracted_tariffs = parsed_data
                    else:
                        extracted_tariffs = [parsed_data]

                    # Save to DB
                    saved_count = save_logic_to_db(conn, version_id, extracted_tariffs)
                    total_saved += saved_count
                    logger.info(f"   [+] Saved {saved_count} logic blocks for {sc_code} (Date: {effective_date})")

                except json.JSONDecodeError:
                    logger.error(f"   [!] Error: Failed to parse valid JSON from LLM response for {sc_code}")
                except Exception as e:
                    logger.error(f"   [!] API/Processing Error: {str(e)}")

                time.sleep(1) # Rate limiting

            logger.info(f"\n--- Extraction Complete ---")
            logger.info(f"Successfully stored {total_saved} tariff logic entries in database.")

    except SQLAlchemyError as e:
        logger.error(f"Database Transaction Failed: {e}")

def _get_default_input_path():
    """Resolve default input path relative to repo root."""
    root = Path(__file__).resolve().parents[3]
    return root / "data" / "processed" / "grouped_tariffs.json"

if __name__ == "__main__":
    input_file = _get_default_input_path()
    
    if not input_file.exists():
        # Fallback for local testing
        if Path("grouped_tariffs.json").exists():
            input_file = Path("grouped_tariffs.json")
        else:
            raise FileNotFoundError(
                f"Input file not found: {input_file}\n"
                "Please ensure grouped_tariffs.json exists in data/processed/ (run group_tariffs.py first)"
            )
    
    extract_tariff_logic_to_db(str(input_file))