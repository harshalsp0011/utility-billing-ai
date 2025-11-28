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
from src.database.models import BillValidationResult, RawDocument, ProcessedData, ValidationResult,PipelineRun, UserBills
from src.database.models import ServiceClassification, SC1RateDetails


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


from sqlalchemy import text

def fetch_user_bills(account_id: str | None = None):
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





def insert_service_classification(data: dict):
    """
    Inserts a new Service Classification record.
    data: dict with keys 'code' and optional 'description'
    """
    logger.info("start of insert_service_classification")
    session = get_session()
    try:
        sc = ServiceClassification(**data)
        session.add(sc)
        session.commit()
        logger.info(f"✅ Inserted Service Classification: {data.get('code')}")
        return sc.id
    except SQLAlchemyError as e:
        logger.error(f"❌ Failed to insert Service Classification: {e}")
        session.rollback()
        return None
    finally:
        session.close()


def insert_sc1_rate_detail(data: dict):
    """
    Inserts a new SC1 Rate Detail record.
    data: dict with effective_date, etc. Must include service_classification_id.
    """
    logger.info("start of insert_sc1_rate_detail")
    session = get_session()
    try:
        detail = SC1RateDetails(**data)
        session.add(detail)
        session.commit()
        logger.info(f"✅ Inserted SC1 Rate Detail for Classification ID {data.get('service_classification_id')}")
        return detail.id
    except SQLAlchemyError as e:
        logger.error(f"❌ Failed to insert SC1 Rate Detail: {e}")
        session.rollback()
        return None
    finally:
        session.close()


def fetch_all_service_classifications():
    """Fetch all service classifications."""
    session = get_session()
    try:
        results = session.query(ServiceClassification).all()
        return results
    except SQLAlchemyError as e:
        logger.error(f"❌ Failed to fetch service classifications: {e}")
        return []
    finally:
        session.close()


def fetch_sc1_rates_by_classification(sc_id: int):
    """Fetch all SC1 Rate Details for a specific classification id."""
    session = get_session()
    try:
        results = session.query(SC1RateDetails).filter_by(service_classification_id=sc_id).all()
        return results
    except SQLAlchemyError as e:
        logger.error(f"❌ Failed to fetch SC1 rate details for ID {sc_id}: {e}")
        return []
    finally:
        session.close()




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
            f"✅ Bill validation result added "
            f"Account={record.get('account_id')} | Issue={record.get('issue_type')}"
        )
    except SQLAlchemyError as e:
        logger.error(f"❌ Failed to insert bill validation result: {e}")
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


def fetch_user_bills_with_issues(account_id: str, issue_type: str | None = None):
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
# 7️⃣ Tariff Version & Logic Management
# ----------------------------------------------------------------------

def get_or_create_tariff_version(conn, utility_name: str, document_id: str, effective_date):
    """
    Checks if a tariff version exists for the utility and date.
    If not, creates a new entry in 'tariff_versions'.
    Returns the version_id.
    
    NOTE: This function expects an active connection (conn) parameter
    for use within transaction contexts (e.g., extract_logic.py).
    
    Parameters
    ----------
    conn : SQLAlchemy connection
        Active database connection (typically within a transaction)
    utility_name : str
        Name of utility (e.g., 'National Grid NY')
    document_id : str
        Tariff document identifier (e.g., 'PSC 220')
    effective_date : str or datetime
        Effective date of tariff version (accepts MM/DD/YYYY or YYYY-MM-DD)
    
    Returns
    -------
    int
        The version_id (primary key)
    """
    from dateutil import parser as date_parser
    
    logger.info("start of get_or_create_tariff_version")
    logger.info(f"Checking tariff version: {utility_name} | {document_id} | {effective_date}")
    
    try:
        # Normalize date format to YYYY-MM-DD for PostgreSQL
        if isinstance(effective_date, str):
            try:
                parsed_date = date_parser.parse(effective_date)
                effective_date_normalized = parsed_date.strftime('%Y-%m-%d')
            except Exception as e:
                logger.warning(f"Failed to parse date '{effective_date}', using as-is: {e}")
                effective_date_normalized = effective_date
        else:
            effective_date_normalized = effective_date
        # Check if version exists
        check_query = text("""
            SELECT id FROM tariff_versions 
            WHERE utility_name = :uname 
              AND tariff_document_id = :doc_id 
              AND effective_date = :eff_date
        """)
        
        result = conn.execute(check_query, {
            "uname": utility_name, 
            "doc_id": document_id, 
            "eff_date": effective_date_normalized
        }).fetchone()

        if result:
            logger.info(f"✓ Found existing tariff version ID: {result[0]}")
            return result[0]

        # Create new version if not found
        insert_query = text("""
            INSERT INTO tariff_versions (utility_name, tariff_document_id, effective_date)
            VALUES (:uname, :doc_id, :eff_date)
            RETURNING id
        """)
        
        result = conn.execute(insert_query, {
            "uname": utility_name, 
            "doc_id": document_id, 
            "eff_date": effective_date_normalized
        }).fetchone()
        
        logger.info(f"✅ Created NEW Tariff Version ID: {result[0]} for Date: {effective_date_normalized}")
        return result[0]
    
    except Exception as e:
        logger.error(f"❌ Failed in get_or_create_tariff_version: {e}")
        raise
    finally:
        logger.info("end of get_or_create_tariff_version")


def save_tariff_logic_to_db(conn, version_id: int, definitions: list):
    """
    Inserts the extracted logic JSON objects into the 'tariff_logic' table.
    
    NOTE: This function expects an active connection (conn) parameter
    for use within transaction contexts (e.g., extract_logic.py).
    
    Parameters
    ----------
    conn : SQLAlchemy connection
        Active database connection (typically within a transaction)
    version_id : int
        Foreign key to tariff_versions.id
    definitions : list of dict
        List of tariff logic definitions extracted from LLM
    
    Returns
    -------
    int
        Count of inserted records
    """
    import json
    logger.info("start of save_tariff_logic_to_db")
    
    try:
        insert_query = text("""
            INSERT INTO tariff_logic (version_id, sc_code, logic_json)
            VALUES (:vid, :code, :logic)
        """)
        
        count = 0
        for item in definitions:
            sc_code = item.get('sc_code', 'UNKNOWN')
            logic_json_str = json.dumps(item)
            
            conn.execute(insert_query, {
                "vid": version_id,
                "code": sc_code,
                "logic": logic_json_str
            })
            count += 1
        
        logger.info(f"💾 Inserted {count} tariff logic entries for version_id={version_id}")
        return count
    
    except Exception as e:
        logger.error(f"❌ Failed to save tariff logic: {e}")
        raise
    finally:
        logger.info("end of save_tariff_logic_to_db")


def fetch_tariff_logic_by_version(version_id: int):
    """
    Fetch all tariff logic entries for a specific tariff version.
    
    Parameters
    ----------
    version_id : int
        The tariff version ID
    
    Returns
    -------
    pd.DataFrame
        DataFrame with columns: id, version_id, sc_code, logic_json, etc.
    """
    logger.info("start of fetch_tariff_logic_by_version")
    logger.info(f"Fetching tariff logic for version_id={version_id}")
    engine = get_engine()
    
    try:
        with engine.connect() as connection:
            stmt = text("""
                SELECT id, version_id, sc_code, section_name, charge_type, 
                       logic_json, condition_text
                FROM tariff_logic
                WHERE version_id = :vid
                ORDER BY sc_code, id
            """)
            result = connection.execute(stmt, {"vid": version_id})
            rows = result.mappings().all()
            df = pd.DataFrame(rows)
        
        logger.info(f"📊 Fetched {len(df)} tariff logic entries for version {version_id}")
        return df
    
    except Exception as e:
        logger.error(f"❌ Failed to fetch tariff logic: {e}")
        return pd.DataFrame()
    finally:
        logger.info("end of fetch_tariff_logic_by_version")


def fetch_tariff_logic_by_sc_code(sc_code: str, effective_date=None):
    """
    Fetch tariff logic for a specific service classification code.
    Optionally filter by effective_date to get the correct version.
    
    Parameters
    ----------
    sc_code : str
        Service classification code (e.g., 'SC1', 'SC3A')
    effective_date : str or datetime, optional
        If provided, returns logic for this effective date
    
    Returns
    -------
    pd.DataFrame
        DataFrame with tariff logic entries
    """
    logger.info("start of fetch_tariff_logic_by_sc_code")
    logger.info(f"Fetching tariff logic for SC: {sc_code}")
    engine = get_engine()
    
    try:
        with engine.connect() as connection:
            if effective_date:
                stmt = text("""
                    SELECT tl.id, tl.version_id, tl.sc_code, tl.section_name, 
                           tl.charge_type, tl.logic_json, tl.condition_text,
                           tv.utility_name, tv.tariff_document_id, tv.effective_date
                    FROM tariff_logic tl
                    JOIN tariff_versions tv ON tl.version_id = tv.id
                    WHERE tl.sc_code = :sc 
                      AND tv.effective_date = :eff_date
                    ORDER BY tl.id
                """)
                result = connection.execute(stmt, {"sc": sc_code, "eff_date": effective_date})
            else:
                stmt = text("""
                    SELECT tl.id, tl.version_id, tl.sc_code, tl.section_name, 
                           tl.charge_type, tl.logic_json, tl.condition_text,
                           tv.utility_name, tv.tariff_document_id, tv.effective_date
                    FROM tariff_logic tl
                    JOIN tariff_versions tv ON tl.version_id = tv.id
                    WHERE tl.sc_code = :sc
                    ORDER BY tv.effective_date DESC, tl.id
                """)
                result = connection.execute(stmt, {"sc": sc_code})
            
            rows = result.mappings().all()
            df = pd.DataFrame(rows)
        
        logger.info(f"📊 Fetched {len(df)} tariff logic entries for {sc_code}")
        return df
    
    except Exception as e:
        logger.error(f"❌ Failed to fetch tariff logic for {sc_code}: {e}")
        return pd.DataFrame()
    finally:
        logger.info("end of fetch_tariff_logic_by_sc_code")


