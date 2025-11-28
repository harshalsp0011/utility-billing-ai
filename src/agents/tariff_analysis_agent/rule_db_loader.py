import json
import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

load_dotenv()

# Setup
DATABASE_URL = os.getenv("DATABASE_URL")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not set in .env")

engine = create_engine(DATABASE_URL)

def get_or_create_version(conn, meta):
    """Finds or creates the Tariff Version ID based on metadata."""
    query_check = text("""
        SELECT id FROM tariff_versions 
        WHERE utility_name = :u AND tariff_document_id = :d AND effective_date = :e
    """)
    res = conn.execute(query_check, {
        "u": meta['utility'],
        "d": meta['source_doc'],
        "e": meta['effective_date']
    }).fetchone()

    if res:
        return res[0]

    query_insert = text("""
        INSERT INTO tariff_versions (utility_name, tariff_document_id, effective_date)
        VALUES (:u, :d, :e)
        RETURNING id
    """)
    res = conn.execute(query_insert, {
        "u": meta['utility'],
        "d": meta['source_doc'],
        "e": meta['effective_date']
    }).fetchone()
    
    logger.info(f"🆕 Created Version ID {res[0]} for {meta['effective_date']}")
    return res[0]

def load_tariffs_to_db(json_file_path):
    logger.info(f"🚀 Loading Tariffs from {json_file_path}")
    
    with open(json_file_path, 'r') as f:
        data = json.load(f)

    inserted_count = 0

    try:
        with engine.begin() as conn:
            for entry in data:
                # 1. Extract Metadata attached in Phase 2
                meta = entry.get('metadata', {
                    "effective_date": "2025-09-01", 
                    "utility": "Unknown", 
                    "source_doc": "Unknown"
                })
                
                # 2. Get Version ID
                version_id = get_or_create_version(conn, meta)
                
                # 3. Clean entry (remove metadata from the logic JSON itself to keep it clean)
                logic_payload = {k: v for k, v in entry.items() if k != 'metadata'}
                
                # 4. Insert Logic
                conn.execute(text("""
                    INSERT INTO tariff_logic (version_id, sc_code, logic_json)
                    VALUES (:vid, :code, :json)
                """), {
                    "vid": version_id,
                    "code": entry.get('sc_code', 'UNKNOWN'),
                    "json": json.dumps(logic_payload)
                })
                inserted_count += 1

        logger.info(f"✅ Successfully loaded {inserted_count} tariff logic entries.")

    except SQLAlchemyError as e:
        logger.error(f"❌ Database Error: {e}")

if __name__ == "__main__":
    # Adjust path to where extract_logic.py saved the file
    path = "../../data/processed/tariff_definitions.json" 
    load_tariffs_to_db(path)