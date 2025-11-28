# src/database/models.py


from sqlalchemy import Column, Integer, String, Float, Date, DateTime, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from sqlalchemy import ForeignKey

Base = declarative_base()

class RawDocument(Base):
    """
    Stores raw extracted metadata from PDFs or Excel files.
    """
    __tablename__ = "raw_documents"

    id = Column(Integer, primary_key=True, autoincrement=True)
    file_name = Column(String(255), nullable=False)
    file_type = Column(String(50))
    upload_date = Column(DateTime, default=datetime.utcnow)
    source = Column(String(100))
    status = Column(String(50), default="pending")

class ProcessedData(Base):
    """
    Stores cleaned and structured billing or usage data.
    """
    __tablename__ = "processed_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(String(100))
    rate_code = Column(String(100))
    usage_kwh = Column(Float)
    demand_kw = Column(Float)
    charge_amount = Column(Float)
    billing_date = Column(Date)
    source_file = Column(String(255))

class ValidationResult(Base):
    """
    Stores error detection, validation, and anomaly findings.
    """
    __tablename__ = "validation_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(String(100))
    issue_type = Column(String(255))
    description = Column(Text)
    detected_on = Column(DateTime, default=datetime.utcnow)
    status = Column(String(50), default="open")





class PipelineRun(Base):
    """
    Tracks every workflow/DAG execution event.
    """
    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    dag_id = Column(String(255), nullable=False)
    start_time = Column(DateTime, default=datetime.utcnow)
    end_time = Column(DateTime)
    status = Column(String(20), default="running")
    total_runtime = Column(Integer)
    error_msg = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)


class UserBills(Base):
    """
    Stores detailed billing information for each user.
    """
    __tablename__ = "user_bills"

    id = Column(Integer, primary_key=True, autoincrement=True)
    bill_account = Column(String(100))
    customer = Column(String(255))
    bill_date = Column(Date)
    read_date = Column(Date)
    days_used = Column(Integer)
    billed_kwh = Column(Float)
    billed_demand = Column(Float)
    load_factor = Column(Float)
    billed_rkva = Column(Float)
    bill_amount = Column(Float)
    sales_tax_amt = Column(Float)
    bill_amount_with_sales_tax = Column(Float)
    retracted_amt = Column(Float)
    sales_tax_factor = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)



class BillValidationResult(Base):
    """
    Stores error detection, validation, and anomaly findings.
    """
    __tablename__ = "bill_validation_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(String(100))
    user_bill_id = Column(Integer, ForeignKey("user_bills.id"))
    issue_type = Column(String(255))
    description = Column(Text)
    detected_on = Column(DateTime, default=datetime.utcnow)
    status = Column(String(50), default="open")
    

class ServiceClassification(Base):
    __tablename__ = "service_classification"
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, unique=True)
    description = Column(Text)

class SC1RateDetails(Base):
    __tablename__ = "sc1_rate_details"
    id = Column(Integer, primary_key=True, autoincrement=True)
    service_classification_id = Column(Integer, ForeignKey('service_classification.id'))
    effective_date = Column(Date)
    basic_service_charge = Column(Float)
    monthly_minimum_charge = Column(Float)
    energy_rate_all_hours = Column(Float)
    on_peak = Column(Float)
    off_peak = Column(Float)
    super_peak = Column(Float)
    distribution_delivery = Column(String(50))


class TariffVersion(Base):
    """Tariff document version metadata (maps to tariff_versions)."""
    __tablename__ = "tariff_versions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    utility_name = Column(String(100), nullable=False)  # e.g., 'National Grid NY'
    tariff_document_id = Column(String(50))             # e.g., 'PSC 220'
    effective_date = Column(Date, nullable=False)       # e.g., 2023-09-01
    end_date = Column(Date)                             # null if current
    created_at = Column(DateTime, default=datetime.utcnow)


class TariffLogic(Base):
    """Calculation logic rules for a given tariff version (maps to tariff_logic)."""
    __tablename__ = "tariff_logic"

    id = Column(Integer, primary_key=True, autoincrement=True)
    version_id = Column(Integer, ForeignKey("tariff_versions.id"), nullable=False)
    sc_code = Column(String(100), nullable=False)        # e.g., 'SC1', 'SC3A', longer composite codes
    section_name = Column(String(100))                  # e.g., 'Customer Charge'
    charge_type = Column(String(50))                    # e.g., 'fixed_fee', 'formula'
    logic_json = Column(JSONB, nullable=False)          # actual math/rule payload
    condition_text = Column(Text)                       # human-readable condition

