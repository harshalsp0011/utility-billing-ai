import json
import os
import logging
from pathlib import Path
from dotenv import load_dotenv

# Use centralized DB utilities (ORM)
from src.database.db_utils import (
    register_tariff_document,
    save_tariff_logic_version,
    get_engine,
)

load_dotenv()

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_tariffs_to_db(json_file_path, filename="NationalGrid_Tariff-NewYork.pdf"):
    """
    Standalone loader for existing JSON files.
    """
    if not os.path.exists(json_file_path):
        logger.error(f"File not found: {json_file_path}")
        return

    with open(json_file_path, 'r') as f:
        data = json.load(f)

    inserted_count = 0
    try:
        # 1. Register Document (ORM)
        doc_id = register_tariff_document(filename=filename, utility_name="National Grid NY")

        # 2. Insert Logic Versions (ORM)
        for entry in data:
            save_tariff_logic_version(doc_id, entry)
            inserted_count += 1

        logger.info(f"✅ Successfully loaded {inserted_count} historical logic entries.")
    except Exception as e:
        logger.error(f"❌ Loader failed: {e}")

if __name__ == "__main__":
    # Adjust path to match your project
    root = Path(__file__).resolve().parents[3]
    path = root / "data" / "processed" / "tariff_definitions.json"
    load_tariffs_to_db(str(path))