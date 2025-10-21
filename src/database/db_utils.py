"""
db_utils.py
------------
🗄️ Common database utility functions for interacting with the Utility Billing AI database.

Purpose:
--------
Provides reusable CRUD (Create, Read, Update, Delete) operations for agents.
Helps store extracted data, processed results, and error detections.

Dependencies:
-------------
- SQLAlchemy ORM
- pandas (for bulk insert/export)
- src.utils.config (for DB_URL)
- src.database.models (ORM classes)
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd

from src.utils.config import DB_URL
from src.utils.logger import get_logger
from src.database.models import RawDocument, ProcessedData, ValidationResult

logger = get_logger(__name__)

# ----------------------------------------------------------------------
# 1️⃣ Setup Engine and Session Factory
# ----------------------------------------------------------------------
engine = create_engine(DB_URL)
SessionLocal = sessionmaker(bind=engine)

# ----------------------------------------------------------------------
# 2️⃣ Insert Functions
# ----------------------------------------------------------------------
def insert_raw_document(metadata: dict):
    """
    Inserts a new raw document record (e.g., uploaded file metadata).

    Parameters
    ----------
    metadata : dict
        Should contain file_name, file_type, upload_date, source, status
    """
    session = SessionLocal()
    try:
        doc = RawDocument(**metadata)
        session.add(doc)
        session.commit()
        logger.info(f"📄 Inserted raw document: {metadata.get('file_name')}")
    except SQLAlchemyError as e:
        logger.error(f"❌ Failed to insert raw document: {e}")
        session.rollback()
    finally:
        session.close()

def insert_processed_data(df: pd.DataFrame):
    """
    Bulk insert processed data from a DataFrame into the ProcessedData table.
    """
    session = SessionLocal()
    try:
        # Align incoming DataFrame columns to the DB schema defined in models.ProcessedData
        # DB columns: account_id, rate_code, usage_kwh, demand_kw, charge_amount, billing_date, source_file
        db_cols = [
            "account_id",
            "rate_code",
            "usage_kwh",
            "demand_kw",
            "charge_amount",
            "billing_date",
            "source_file",
        ]

        # Common mappings from analytics outputs to DB schema
        col_map = {
            # Map analytics names to DB names
            "actual_charge": "charge_amount",
            # Add other mappings here if needed
        }

        df_aligned = df.rename(columns=col_map).copy()

        # Ensure all DB columns exist; fill missing with None/NaN
        for col in db_cols:
            if col not in df_aligned.columns:
                df_aligned[col] = None

        # Keep only DB columns in the expected order
        df_aligned = df_aligned[db_cols]

        # Best-effort type normalization
        if "billing_date" in df_aligned.columns:
            try:
                df_aligned["billing_date"] = pd.to_datetime(
                    df_aligned["billing_date"], errors="coerce"
                )
            except Exception:
                pass

        # Insert
        df_aligned.to_sql("processed_data", engine, if_exists="append", index=False, method="multi")
        logger.info(
            f"💾 Inserted {len(df_aligned)} rows into ProcessedData table. Incoming cols: {list(df.columns)} -> stored cols: {db_cols}"
        )
    except Exception as e:
        logger.error(
            f"❌ Failed to insert processed data: {e}\nIncoming columns: {list(df.columns)}"
        )
    finally:
        session.close()

def insert_validation_result(record: dict):
    """
    Inserts a single validation result (e.g., detected error or anomaly).
    """
    session = SessionLocal()
    try:
        val = ValidationResult(**record)
        session.add(val)
        session.commit()
        logger.info(f"✅ Validation result added for Account {record.get('account_id')}")
    except SQLAlchemyError as e:
        logger.error(f"❌ Failed to insert validation result: {e}")
        session.rollback()
    finally:
        session.close()

# ----------------------------------------------------------------------
# 3️⃣ Fetch Functions
# ----------------------------------------------------------------------
def fetch_all_raw_docs():
    """Returns a list of all raw documents."""
    session = SessionLocal()
    try:
        results = session.query(RawDocument).all()
        logger.info(f"📂 Retrieved {len(results)} raw documents.")
        return results
    except SQLAlchemyError as e:
        logger.error(f"❌ Failed to fetch raw docs: {e}")
        return []
    finally:
        session.close()

def fetch_processed_data(limit: int = 10):
    """Fetch limited processed data rows for review."""
    try:
        df = pd.read_sql(f"SELECT * FROM processed_data LIMIT {limit}", engine)
        logger.info(f"📊 Fetched {len(df)} processed rows.")
        return df
    except Exception as e:
        logger.error(f"❌ Failed to fetch processed data: {e}")
        return pd.DataFrame()

# ----------------------------------------------------------------------
# 4️⃣ Update Functions
# ----------------------------------------------------------------------
def update_document_status(file_name: str, new_status: str):
    """
    Updates the status of a document record (e.g., 'processed', 'error', etc.)
    """
    session = SessionLocal()
    try:
        doc = session.query(RawDocument).filter_by(file_name=file_name).first()
        if doc:
            doc.status = new_status
            session.commit()
            logger.info(f"🔄 Updated status for {file_name} → {new_status}")
        else:
            logger.warning(f"⚠️ Document {file_name} not found in DB.")
    except SQLAlchemyError as e:
        logger.error(f"❌ Failed to update status for {file_name}: {e}")
        session.rollback()
    finally:
        session.close()

# ----------------------------------------------------------------------
# 5️⃣ Self-Test
# ----------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("🧪 Running db_utils self-test...")

    # Example 1: Insert raw document
    insert_raw_document({
        "file_name": "Hampton_Sept2025.pdf",
        "file_type": "PDF",
        "upload_date": "2025-10-20",
        "source": "City of Hampton",
        "status": "uploaded"
    })

    # Example 2: Fetch raw documents
    docs = fetch_all_raw_docs()
    logger.info(f"Found {len(docs)} documents in DB.")

    # Example 3: Update a record
    update_document_status("Hampton_Sept2025.pdf", "processed")

    logger.info("✅ db_utils self-test completed.")
