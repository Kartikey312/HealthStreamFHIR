"""
SQLAlchemy ORM Models for FHIR database
"""
from sqlalchemy import Column, Integer, BigInteger, String, Text, DateTime, Boolean, Enum, ForeignKey, Index, JSON
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


class PreAuthRequestLog(Base):
    """
    Audit log for the PreAuth request side (JSON_REQUEST, FHIR_REQUEST stages).
    Written only by preauth-log-service, a dedicated Kafka consumer - never written
    inline by integration-api, so a logging failure can't block/break that request.

    payload uses SQLAlchemy's JSON type (native MySQL JSON column, unlike every other
    table's Text+json.dumps pattern) - assign a dict directly here, do NOT json.dumps()
    it first, or it will double-encode as a JSON string instead of a JSON object.
    """
    __tablename__ = "preauth_request_log"

    id = Column(BigInteger, primary_key=True, index=True)
    claim_id = Column(String(200), nullable=False, index=True)
    correlation_id = Column(String(36), nullable=False, index=True)
    stage = Column(String(20), nullable=False)  # JSON_REQUEST, FHIR_REQUEST
    payload = Column(JSON, nullable=False)
    kafka_topic = Column(String(150), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)


class PreAuthResponseLog(Base):
    """
    Audit log for the PreAuth response side (FHIR_RESPONSE, JSON_RESPONSE stages).
    Written only by preauth-log-service. See PreAuthRequestLog for the payload/JSON
    column caveat - the same applies here.
    """
    __tablename__ = "preauth_response_log"

    id = Column(BigInteger, primary_key=True, index=True)
    claim_id = Column(String(200), nullable=False, index=True)
    correlation_id = Column(String(36), nullable=False, index=True)
    stage = Column(String(20), nullable=False)  # FHIR_RESPONSE, JSON_RESPONSE
    payload = Column(JSON, nullable=False)
    kafka_topic = Column(String(150), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)


class AuditLog(Base):
    """
    Generic audit trail per implementation.md section 5 - every request and
    state change across the rebuilt pipeline (integration-api,
    json-fhir-service, communication-service, fhir-json-service,
    processing-service), one row per Envelope a service handles.
    """
    __tablename__ = "audit_log"

    id = Column(BigInteger, primary_key=True, index=True)
    correlation_id = Column(String(36), nullable=False, index=True)
    service = Column(String(100), nullable=False)
    action = Column(String(100), nullable=False)
    client_id = Column(String(255), nullable=True)
    payload = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)


class MessageTracking(Base):
    """
    Message lifecycle tracking per implementation.md section 5 - one row per
    correlation_id, updated as it moves through the pipeline. Backs
    GET /api/v1/patients/status/{correlation_id} on integration-api.
    """
    __tablename__ = "message_tracking"

    correlation_id = Column(String(36), primary_key=True)
    patient_id = Column(String(255), nullable=False, index=True)
    status = Column(String(20), nullable=False, default="RECEIVED", index=True)
    # RECEIVED|TRANSFORMED|SENT|RESPONDED|COMPLETED|FAILED
    fhir_resource_id = Column(String(255), nullable=True)
    last_error = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ProcessedMessage(Base):
    """
    Idempotency for consumers per implementation.md section 5 - dedupe by
    message_id per service, checked before handling so redelivered messages
    (at-least-once delivery + manual commit) are harmless.
    """
    __tablename__ = "processed_messages"

    service = Column(String(100), primary_key=True)
    message_id = Column(String(36), primary_key=True)
    processed_at = Column(DateTime, default=datetime.utcnow)
