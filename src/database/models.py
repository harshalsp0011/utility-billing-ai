# src/database/models.py

from sqlalchemy import Column, Integer, String, Float, Date, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

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
