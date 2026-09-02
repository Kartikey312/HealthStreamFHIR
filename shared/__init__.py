"""
Shared utilities for JSON2FHIR services
"""
import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from .database import engine, SessionLocal, get_db
from .models import Base, Transaction, FHIRRequest, FHIRResponse, ResponseMapping
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
    fhir_to_json_request, json_to_fhir_response
)

__all__ = [
    "engine", "SessionLocal", "get_db",
    "Base", "Transaction", "FHIRRequest", "FHIRResponse", "ResponseMapping",
    "PatientRequest", "EligibilityResponseIn", "FHIRPatientResource", "TransactionResponse",
    "KafkaMessagePayload", "FHIRValidationResult", "HospitalResponse", "FinalJSONResponse",
    "create_kafka_producer", "create_kafka_consumer", "send_kafka_message",
    "consume_kafka_messages", "TOPICS",
    "json_to_fhir_patient", "fhir_to_json_response", "validate_fhir_patient",
    "fhir_to_json_request", "json_to_fhir_response"
]
