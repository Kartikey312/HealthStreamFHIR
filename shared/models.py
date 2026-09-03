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


class Workflow(Base):
    __tablename__ = "workflows"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(String(1000))
    definition = Column(Text, nullable=False)  # JSON: {"nodes": [...], "edges": [...]}
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class WorkflowRun(Base):
    __tablename__ = "workflow_runs"

    id = Column(Integer, primary_key=True, index=True)
    workflow_id = Column(Integer, ForeignKey("workflows.id"), nullable=False, index=True)
    status = Column(String(50), default="RUNNING", index=True)  # RUNNING, SUCCESS, FAILED
    trigger_input = Column(Text)
    error = Column(Text)
    started_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)


class WorkflowRunStep(Base):
    __tablename__ = "workflow_run_steps"

    id = Column(Integer, primary_key=True, index=True)
    run_id = Column(Integer, ForeignKey("workflow_runs.id"), nullable=False, index=True)
    node_id = Column(String(255), nullable=False)
    node_type = Column(String(100), nullable=False)
    status = Column(String(50), default="RUNNING", index=True)  # RUNNING, SUCCESS, FAILED, SKIPPED
    input = Column(Text)
    output = Column(Text)
    error = Column(Text)
    started_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)
