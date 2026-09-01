"""
SQLAlchemy ORM Models for FHIR database
"""
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, Enum, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class Transaction(Base):
    __tablename__ = "transactions"
    
    id = Column(Integer, primary_key=True, index=True)
    transaction_id = Column(String(255), unique=True, nullable=False, index=True)
    patient_id = Column(String(255), nullable=False, index=True)
    patient_name = Column(String(255))
    status = Column(String(50), default="PENDING", index=True)  # PENDING, PROCESSING, SUCCESS, FAILED
    json_payload = Column(Text)
    fhir_payload = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class FHIRRequest(Base):
    __tablename__ = "fhir_requests"
    
    id = Column(Integer, primary_key=True, index=True)
    transaction_id = Column(String(255), ForeignKey("transactions.transaction_id"), index=True)
    request_id = Column(String(255), unique=True, nullable=False)
    fhir_resource_type = Column(String(100))
    fhir_payload = Column(Text)
    validation_status = Column(String(50), default="PENDING", index=True)  # PENDING, VALID, INVALID
    validation_errors = Column(Text)
    sent_to_hospital = Column(Boolean, default=False)
    sent_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class FHIRResponse(Base):
    __tablename__ = "fhir_responses"
    
    id = Column(Integer, primary_key=True, index=True)
    transaction_id = Column(String(255), ForeignKey("transactions.transaction_id"), index=True)
    response_id = Column(String(255), unique=True, nullable=False)
    fhir_payload = Column(Text)
    hospital_response_code = Column(Integer)
    hospital_response_message = Column(String(500))
    received_at = Column(DateTime)
    processed = Column(Boolean, default=False)
    processed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ResponseMapping(Base):
    __tablename__ = "response_mappings"
    
    id = Column(Integer, primary_key=True, index=True)
    transaction_id = Column(String(255), ForeignKey("transactions.transaction_id"), index=True)
    original_json = Column(Text)
    final_json = Column(Text)
    status = Column(String(50), default="PENDING")  # PENDING, COMPLETED, FAILED
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
