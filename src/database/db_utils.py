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
from datetime import datetime
from src.utils.config import DB_URL
from src.utils.logger import get_logger
from src.database.models import RawDocument, ProcessedData, ValidationResult,PipelineRun, UserBills

logger = get_logger(__name__)

# ----------------------------------------------------------------------
# 1️⃣ Setup Engine and Session Factory (Lazy-loaded)
# ----------------------------------------------------------------------
_engine = None
_SessionLocal = None

def get_engine():
    """Lazily create and return the SQLAlchemy engine."""
    global _engine
    if _engine is None:
        _engine = create_engine(DB_URL)
    return _engine

def get_session():
    """Lazily create and return a new database session."""
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(bind=get_engine())
    return _SessionLocal()

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
    logger.info("start of insert_raw_document")
    session = get_session()
    try:
        doc = RawDocument(**metadata)
        session.add(doc)
        session.commit()
        logger.info(f"📄 Inserted raw document: {metadata.get('file_name')}")
    except SQLAlchemyError as e:
        logger.error(f"❌ Failed to insert raw document: {e}")
        session.rollback()
    finally:
        logger.info("end of insert_raw_document")
        session.close()

def insert_processed_data(df: pd.DataFrame):
    """
    Bulk insert processed data from a DataFrame into the ProcessedData table.
    """
    logger.info("start of insert_processed_data")
    session = get_session()
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
        df_aligned.to_sql("processed_data", get_engine(), if_exists="append", index=False, method="multi")
        logger.info(
            f"💾 Inserted {len(df_aligned)} rows into ProcessedData table. Incoming cols: {list(df.columns)} -> stored cols: {db_cols}"
        )
    except Exception as e:
        logger.error(
            f"❌ Failed to insert processed data: {e}\nIncoming columns: {list(df.columns)}"
        )
    finally:
        logger.info("end of insert_processed_data")
        session.close()

def insert_validation_result(record: dict):
    """
    Inserts a single validation result (e.g., detected error or anomaly).
    """
    logger.info("start of insert_validation_result")
    session = get_session()
    try:
        val = ValidationResult(**record)
        session.add(val)
        session.commit()
        logger.info(f"✅ Validation result added for Account {record.get('account_id')}")
    except SQLAlchemyError as e:
        logger.error(f"❌ Failed to insert validation result: {e}")
        session.rollback()
    finally:
        logger.info("end of insert_validation_result")
        session.close()

# ----------------------------------------------------------------------
# 3️⃣ Fetch Functions
# ----------------------------------------------------------------------
def fetch_all_raw_docs():
    """Returns a list of all raw documents."""
    logger.info("start of fetch_all_raw_docs")
    session = get_session()
    try:
        results = session.query(RawDocument).all()
        logger.info(f"📂 Retrieved {len(results)} raw documents.")
        return results
    except SQLAlchemyError as e:
        logger.error(f"❌ Failed to fetch raw docs: {e}")
        return []
    finally:
        logger.info("end of fetch_all_raw_docs")
        session.close()

def fetch_processed_data(limit: int = 10):
    """Fetch limited processed data rows for review."""
    engine = get_engine()
    try:
        with engine.connect() as connection:
            df = pd.read_sql(f"SELECT * FROM processed_data LIMIT {limit}", connection)
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
    session = get_session()
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


# ----------------------------------------------------------------------
# 6️⃣ Pipeline Run Management
# ----------------------------------------------------------------------

def start_pipeline_run(dag_id: str):
    """
    Creates a new pipeline run entry and returns its run_id.
    """
    logger.info("start of start_pipeline_run")
    session = get_session()
    try:
        run = PipelineRun(dag_id=dag_id, status="running")
        session.add(run)
        session.commit()
        logger.info(f"🟢 Started pipeline run {run.id} for {dag_id}")
        return run.id
    except SQLAlchemyError as e:
        logger.error(f"❌ Failed to start pipeline run: {e}")
        session.rollback()
        return None
    finally:
        logger.info("end of start_pipeline_run")
        session.close()


def update_pipeline_run(run_id: int, status: str, error_msg: str = None):
    """
    Updates pipeline run end_time, total_runtime, and final status.
    """
    logger.info("start of update_pipeline_run")
    session = get_session()
    try:
        run = session.query(PipelineRun).filter_by(id=run_id).first()
        if run:
            run.end_time = datetime.utcnow()
            run.status = status
            if run.start_time:
                run.total_runtime = int((run.end_time - run.start_time).total_seconds())
            run.error_msg = error_msg
            session.commit()
            logger.info(f"🔄 Updated pipeline run {run_id} → {status}")
        else:
            logger.warning(f"⚠️ Pipeline run {run_id} not found.")
    except SQLAlchemyError as e:
        logger.error(f"❌ Failed to update pipeline run {run_id}: {e}")
        session.rollback()
    finally:
        logger.info("end of update_pipeline_run")
        session.close()


def insert_user_bill(record: dict):
    """
    Inserts a single UserBills record.
    """
    logger.info("start of insert_user_bill")
    session = get_session()
    try:
        bill = UserBills(**record)
        session.add(bill)
        session.commit()
        logger.info(f"📄 Inserted UserBills record for Account {record.get('bill_account')}")
    except SQLAlchemyError as e:
        logger.error(f"❌ Failed to insert UserBills record: {e}")
        session.rollback()
    finally:
        logger.info("end of insert_user_bill")
        session.close()


def insert_user_bills_bulk(df: pd.DataFrame):
    """
    Bulk insert UserBills records from a DataFrame.
    """
    logger.info("start of insert_user_bills_bulk")
    session = get_session()
    try:
        db_cols = [
            "bill_account",
            "customer",
            "bill_date",
            "read_date",
            "days_used",
            "billed_kwh",
            "billed_demand",
            "load_factor",
            "billed_rkva",
            "bill_amount",
            "sales_tax_amt",
            "bill_amount_with_sales_tax",
            "retracted_amt",
            "sales_tax_factor",
        ]
        for col in db_cols:
            if col not in df.columns:
                df[col] = None
        df = df[db_cols]
        if "bill_date" in df.columns:
            try:
                df["bill_date"] = pd.to_datetime(df["bill_date"], errors="coerce")
            except Exception:
                pass
        if "read_date" in df.columns:
            try:
                df["read_date"] = pd.to_datetime(df["read_date"], errors="coerce")
            except Exception:
                pass
        df.to_sql("user_bills", get_engine(), if_exists="append", index=False, method="multi")
        logger.info(f"💾 Inserted {len(df)} rows into UserBills table.")
    except Exception as e:
        logger.error(f"❌ Failed to insert UserBills bulk: {e}")
    finally:
        logger.info("end of insert_user_bills_bulk")
        session.close()


def fetch_user_bills(limit: int = 10):
    """Fetch limited UserBills rows for review."""
    engine = get_engine()
    try:
        with engine.connect() as connection:
            df = pd.read_sql(f"SELECT * FROM user_bills LIMIT {limit}", connection)
        logger.info(f"📊 Fetched {len(df)} UserBills rows.")
        return df
    except Exception as e:
        logger.error(f"❌ Failed to fetch UserBills: {e}")
        return pd.DataFrame()
    logger.info("end of fetch_user_bills")