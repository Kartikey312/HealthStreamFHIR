"""
Shared utilities for JSON2FHIR services
"""
import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from .database import engine, SessionLocal, get_db
from .models import (
    Base, Transaction, FHIRRequest, FHIRResponse, ResponseMapping,
    Workflow, WorkflowRun, WorkflowRunStep,
    PreAuthRequestLog, PreAuthResponseLog,
    AuditLog, MessageTracking, ProcessedMessage
)
from .schemas import (
    PatientRequest, EligibilityResponseIn, FHIRPatientResource, TransactionResponse,
    KafkaMessagePayload, FHIRValidationResult, HospitalResponse, FinalJSONResponse
)
from .kafka_utils import (
    create_kafka_producer, create_kafka_consumer, send_kafka_message,
    consume_kafka_messages, TOPICS
)
from .fhir_utils import (
    json_to_fhir_patient, fhir_to_json_response, validate_fhir_patient,
    fhir_to_json_request, json_to_fhir_response, json_to_fhir_claim,
    fhir_to_json_claim_response, fhir_to_json_claim
)
from .envelope import Envelope
from .patient_mapper import to_fhir_patient, to_internal_response
from .eligibility_mapper import to_fhir_eligibility_bundle, to_eligibility_response_json
from .preauth_mapper import to_preauth_tables, to_fhir_preauth_bundle, claim_key, find_claim
from .preauth_tables import ensure_preauth_schema, store_preauth_tables
from .eligibility_tables import (
    ensure_eligibility_schema, store_eligibility_request, store_eligibility_response,
    to_eligibility_request_rows, to_eligibility_response_rows,
)

__all__ = [
    "engine", "SessionLocal", "get_db",
    "Base", "Transaction", "FHIRRequest", "FHIRResponse", "ResponseMapping",
    "Workflow", "WorkflowRun", "WorkflowRunStep",
    "PreAuthRequestLog", "PreAuthResponseLog",
    "AuditLog", "MessageTracking", "ProcessedMessage",
    "PatientRequest", "EligibilityResponseIn", "FHIRPatientResource", "TransactionResponse",
    "KafkaMessagePayload", "FHIRValidationResult", "HospitalResponse", "FinalJSONResponse",
    "create_kafka_producer", "create_kafka_consumer", "send_kafka_message",
    "consume_kafka_messages", "TOPICS",
    "json_to_fhir_patient", "fhir_to_json_response", "validate_fhir_patient",
    "fhir_to_json_request", "json_to_fhir_response", "json_to_fhir_claim",
    "fhir_to_json_claim_response", "fhir_to_json_claim",
    "Envelope", "to_fhir_patient", "to_internal_response", "to_fhir_eligibility_bundle",
    "to_eligibility_response_json", "to_preauth_tables", "to_fhir_preauth_bundle", "claim_key", "find_claim",
    "ensure_preauth_schema", "store_preauth_tables",
    "ensure_eligibility_schema", "store_eligibility_request", "store_eligibility_response",
    "to_eligibility_request_rows", "to_eligibility_response_rows"
]
