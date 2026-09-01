"""
Shared utilities for JSON2FHIR services
"""
from .database import engine, SessionLocal, get_db
from .models import Base, Transaction, FHIRRequest, FHIRResponse, ResponseMapping
from .schemas import (
    PatientRequest, FHIRPatientResource, TransactionResponse,
    KafkaMessagePayload, FHIRValidationResult, HospitalResponse, FinalJSONResponse
)
from .kafka_utils import (
    create_kafka_producer, create_kafka_consumer, send_kafka_message,
    consume_kafka_messages, TOPICS
)
from .fhir_utils import (
    json_to_fhir_patient, fhir_to_json_response, validate_fhir_patient
)

__all__ = [
    "engine", "SessionLocal", "get_db",
    "Base", "Transaction", "FHIRRequest", "FHIRResponse", "ResponseMapping",
    "PatientRequest", "FHIRPatientResource", "TransactionResponse",
    "KafkaMessagePayload", "FHIRValidationResult", "HospitalResponse", "FinalJSONResponse",
    "create_kafka_producer", "create_kafka_consumer", "send_kafka_message",
    "consume_kafka_messages", "TOPICS",
    "json_to_fhir_patient", "fhir_to_json_response", "validate_fhir_patient"
]
