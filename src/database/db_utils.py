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
from dateutil import parser as dateparser
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from datetime import datetime
from sqlalchemy import text
from typing import Union, Optional
from src.utils.config import DB_URL
from src.utils.logger import get_logger
from src.database.models import BillValidationResult, RawBillDocument,PipelineRun, UserBills
from src.database.models import TariffDocument, TariffLogicVersion


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
def insert_raw_bill_document(metadata: dict):
    """
    Inserts a new raw document record (e.g., uploaded file metadata).

    Parameters
    ----------
    metadata : dict
        Should contain file_name, file_type, upload_date, source, status
    """
    logger.info("start of insert_raw_bill_document")
    session = get_session()
    try:
        doc = RawBillDocument(**metadata)
        session.add(doc)
        session.commit()
        logger.info(f"📄 Inserted raw document: {metadata.get('file_name')}")
    except SQLAlchemyError as e:
        logger.error(f"❌ Failed to insert raw document: {e}")
        session.rollback()
    finally:
        logger.info("end of insert_raw_bill_document")
        session.close()


# ----------------------------------------------------------------------
# 3️⃣ Fetch Functions
# ----------------------------------------------------------------------
def fetch_all_raw_bill_docs():
    """Returns a list of all raw documents."""
    logger.info("start of fetch_all_raw_bill_docs")
    session = get_session()
    try:
        results = session.query(RawBillDocument).all()
        logger.info(f"📂 Retrieved {len(results)} raw documents.")
        return results
    except SQLAlchemyError as e:
        logger.error(f"❌ Failed to fetch raw docs: {e}")
        return []
    finally:
        logger.info("end of fetch_all_raw_bill_docs")
        session.close()


# ----------------------------------------------------------------------
# 4️⃣ Update Functions
# ----------------------------------------------------------------------
def update_document_status(file_name: str, new_status: str):
    """
    Updates the status of a document record (e.g., 'processed', 'error', etc.)
    """
    session = get_session()
    try:
        doc = session.query(RawBillDocument).filter_by(file_name=file_name).first()
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




def fetch_user_bills(account_id: Optional[str] = None):
    """
    Fetch all user bills.
    Optionally filter by bill_account.
    """
    logger.info("start of fetch_user_bills")
    engine = get_engine()

    try:
        with engine.connect() as connection:
            if account_id:
                stmt = text("""
                    SELECT *
                    FROM user_bills
                    WHERE TRIM(bill_account)::text = TRIM(:acct)::text
                """)
                result = connection.execute(stmt, {"acct": account_id})
            else:
                result = connection.execute(text("SELECT * FROM user_bills"))

            rows = result.mappings().all()
            df = pd.DataFrame(rows)

        logger.info(f"📊 Fetched {len(df)} UserBills rows.")
        return df

    except Exception as e:
        logger.error(f"❌ Failed to fetch UserBills: {e}")
        return pd.DataFrame()

    finally:
        logger.info("end of fetch_user_bills")



def fetch_all_account_numbers():
    """Return a list of all distinct bill_account values from user_bills."""
    logger.info("start of fetch_all_account_numbers")
    engine = get_engine()

    try:
        with engine.connect() as connection:
            stmt = text("SELECT DISTINCT bill_account FROM user_bills")
            result = connection.execute(stmt)

            # Extract the column into a Python list
            accounts = [row[0] for row in result.fetchall()]

        logger.info(f"📌 Found {len(accounts)} distinct account numbers.")
        return accounts

    except Exception as e:
        logger.error(f"❌ Failed to fetch account numbers: {e}")
        return []

    finally:
        logger.info("end of fetch_all_account_numbers")





# ----------------------------------------------------------------------



def insert_bill_validation_result(record: dict):
    """
    Inserts a single BillValidationResult record (error, anomaly, or validation finding).
    """
    logger.info("start of insert_bill_validation_result")
    session = get_session()
    try:
        val = BillValidationResult(**record)
        session.add(val)
        session.commit()
        logger.info(
            f"[OK] Bill validation result added "
            f"Account={record.get('account_id')} | Issue={record.get('issue_type')}"
        )
    except SQLAlchemyError as e:
        logger.error(f"[ERROR] Failed to insert bill validation result: {e}")
        session.rollback()
    finally:
        logger.info("end of insert_bill_validation_result")
        session.close()


def fetch_bill_validation_results(
    account_id: str = None,
    user_bill_id: int = None,
    status: str = None,
    limit: int = 100
):
    """
    Fetch BillValidationResult rows optionally filtered by:
    - account_id
    - user_bill_id
    - status ('open', 'resolved', etc.)
    """
    logger.info("start of fetch_bill_validation_results")
    session = get_session()
    try:
        query = session.query(BillValidationResult)

        if account_id:
            query = query.filter_by(account_id=account_id)

        if user_bill_id:
            query = query.filter_by(user_bill_id=user_bill_id)

        if status:
            query = query.filter_by(status=status)

        results = query.order_by(BillValidationResult.detected_on.desc()).limit(limit).all()

        logger.info(f"📊 Retrieved {len(results)} bill validation results.")
        return results

    except SQLAlchemyError as e:
        logger.error(f"❌ Failed to fetch bill validation results: {e}")
        return []
    finally:
        logger.info("end of fetch_bill_validation_results")
        session.close()


def update_bill_validation_result(result_id: int, updates: dict):
    """
    Updates fields of a BillValidationResult row.
    Example: update_bill_validation_result(1, {"status": "resolved"})
    """
    logger.info("start of update_bill_validation_result")
    session = get_session()
    try:
        result = session.query(BillValidationResult).filter_by(id=result_id).first()

        if result:
            for key, value in updates.items():
                setattr(result, key, value)

            session.commit()
            logger.info(f"🔄 Updated BillValidationResult id={result_id}")
        else:
            logger.warning(f"⚠️ BillValidationResult id={result_id} not found.")

    except SQLAlchemyError as e:
        logger.error(f"❌ Failed to update BillValidationResult {result_id}: {e}")
        session.rollback()
    finally:
        logger.info("end of update_bill_validation_result")
        session.close()


def fetch_user_bills_with_issues(account_id: str, issue_type: Optional[str] = None):
    """
    Fetch ONLY the user bills that have validation issues
    for the given account_id.
    """
    logger.info("start of fetch_user_bills_with_issues")
    engine = get_engine()

    try:
        with engine.connect() as connection:
            base_sql = """
SELECT
    ub.id AS bill_id,
    bvr.user_bill_id AS fk_user_bill_id,
    bvr.id AS issue_id,
    ub.bill_account,
    ub.customer,
    ub.bill_date,
    ub.read_date,
    ub.days_used,
    ub.billed_kwh,
    ub.billed_demand,
    ub.load_factor,
    ub.billed_rkva,
    ub.bill_amount,
    ub.sales_tax_amt,
    ub.bill_amount_with_sales_tax,
    ub.retracted_amt,
    ub.sales_tax_factor,
    ub.created_at AS bill_created_at,

    bvr.issue_type,
    bvr.description,
    bvr.status,
    bvr.detected_on
FROM user_bills AS ub
JOIN bill_validation_results AS bvr
    ON ub.id = bvr.user_bill_id
WHERE TRIM(ub.bill_account)::text = TRIM(:acct)::text

"""


            params = {"acct": account_id}

            if issue_type:
                base_sql += " AND bvr.issue_type = :issue"
                params["issue"] = issue_type

            result = connection.execute(text(base_sql), params)
            rows = result.mappings().all()

            df = pd.DataFrame(rows)

        logger.info(f"⚠️ Found {len(df)} bills WITH issues for account {account_id}.")
        return df

    except Exception as e:
        logger.error(f"❌ Failed to fetch bills with issues: {e}")
        return pd.DataFrame()

    finally:
        logger.info("end of fetch_user_bills_with_issues")


# ----------------------------------------------------------------------
# 7️⃣ Tariff Version & Logic Management (ORM, session.query)
# ----------------------------------------------------------------------

def register_tariff_document(filename: str, utility_name: str, document_version: Optional[str] = None, description: Optional[str] = None) -> int:
    """
    Register or update a tariff document and return its id.
    - Uses ORM with session.query (no raw SQL / conn).
    - If document exists (by unique filename), updates metadata and refreshes upload_date.
    - Otherwise inserts a new row.
    """
    logger.info("start of register_tariff_document")
    session = get_session()
    try:
        doc = session.query(TariffDocument).filter_by(filename=filename).first()
        if doc:
            doc.utility_name = utility_name
            doc.document_version = document_version
            doc.description = description
            doc.upload_date = datetime.utcnow()
        else:
            doc = TariffDocument(
                filename=filename,
                utility_name=utility_name,
                document_version=document_version,
                description=description,
            )
            session.add(doc)
        session.commit()
        logger.info(f"📄 Registered tariff document id={doc.id} filename={filename}")
        return doc.id
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"❌ Failed to register tariff document: {e}")
        raise
    finally:
        logger.info("end of register_tariff_document")
        session.close()

def save_tariff_logic_version(doc_id: int, logic_item: dict) -> bool:
    """
    Upsert a specific version of logic for a given SC code.
    - Expects logic_item with keys: sc_code, metadata.effective_date, and JSON logic fields.
    - Uses ORM with session.query (no raw SQL / conn).
    - Removes 'metadata' wrapper before persisting to keep logic_json clean.
    """
    import json
    from dateutil import parser as dateparser

    logger.info("start of save_tariff_logic_version")
    session = get_session()
    try:
        sc_code = logic_item.get("sc_code")
        meta = logic_item.get("metadata", {})
        effective_date_str = meta.get("effective_date")

        # Normalize date to YYYY-MM-DD
        effective_date = dateparser.parse(effective_date_str).date()

        # Remove metadata before persisting
        clean_logic = {k: v for k, v in logic_item.items() if k != "metadata"}

        # Find existing version
        existing = (
            session.query(TariffLogicVersion)
            .filter_by(sc_code=sc_code, effective_date=effective_date)
            .first()
        )

        if existing:
            existing.logic_json = clean_logic
            existing.tariff_document_id = doc_id
            logger.info(f"Found existing tariff logic version for sc={sc_code} eff={effective_date}, updating...")
        else:
            new_ver = TariffLogicVersion(
                tariff_document_id=doc_id,
                sc_code=sc_code,
                effective_date=effective_date,
                logic_json=clean_logic,
            )
            session.add(new_ver)

        session.commit()
        logger.info(f"💾 Saved tariff logic version sc={sc_code} eff={effective_date}")
        return True
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Failed to save version: {e}")
        return False
    finally:
        logger.info("end of save_tariff_logic_version")
        session.close()

def fetch_logic_for_audit(sc_code: str, bill_date: Union[str, datetime.date]) -> Optional[dict]:
    """
    Time Machine lookup: find the logic active on bill_date for sc_code.
    - Accepts bill_date as string or date; normalizes to date.
    - Uses ORM with session.query ordering by effective_date DESC.
    - Returns the stored JSON object (dict) or None if not found.
    """


    logger.info("start of fetch_logic_for_audit")
    session = get_session()
    try:
        # Normalize bill_date
        bill_date_parsed = (
            dateparser.parse(bill_date).date() if isinstance(bill_date, str) else bill_date
        )

        result = (
            session.query(TariffLogicVersion)
            .filter(TariffLogicVersion.sc_code == sc_code)
            .filter(TariffLogicVersion.effective_date <= bill_date_parsed)
            .order_by(TariffLogicVersion.effective_date.desc())
            .first()
        )

        return result.logic_json if result else None
    except SQLAlchemyError as e:
        logger.error(f"❌ Failed to fetch logic for audit: {e}")
        return None
    finally:
        logger.info("end of fetch_logic_for_audit")
        session.close()