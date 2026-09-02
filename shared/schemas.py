"""
Pydantic schemas for data validation
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


class MessageHeaderIn(BaseModel):
    """Nested messageHeader block of a flattened eligibility request"""
    id: Optional[str] = None
    eventCoding: Optional[str] = None
    destinationReceiverIdentifier: Optional[str] = None
    senderIdentifier: Optional[str] = None
    focus: Optional[str] = None


class PatientRequest(BaseModel):
    """Schema for an incoming flattened CoverageEligibilityRequest JSON"""
    id: Optional[str] = None
    resourceType: Optional[str] = "CoverageEligibilityRequest"
    identifier: Optional[str] = None
    identifierSystem: Optional[str] = None
    status: str = "active"
    priority: Optional[str] = "normal"
    purpose: List[str] = Field(default_factory=lambda: ["discovery"])
    patientIdentifier: str
    patientIdentifierType: Optional[str] = "NI"
    patientIdentifierSystem: Optional[str] = None
    servicedDate: Optional[str] = None
    created: Optional[str] = None
    insurerIdentifier: str
    insurerIdentifierType: Optional[str] = None
    insurerName: Optional[str] = None
    providerIdentifier: str
    providerIdentifierType: Optional[str] = None
    providerName: Optional[str] = None
    insurances: List[Dict[str, Any]] = Field(default_factory=list)
    messageHeader: MessageHeaderIn = Field(default_factory=MessageHeaderIn)


class FHIRPatientResource(BaseModel):
    """FHIR Patient Resource schema"""
    resourceType: str = "Patient"
    id: str
    name: List[Dict[str, str]]
    active: bool = True
    telecom: Optional[List[Dict[str, str]]] = None
    address: Optional[List[Dict[str, str]]] = None


class TransactionResponse(BaseModel):
    """API Response for transaction"""
    status: str
    message: str
    transaction_id: str
    data: Dict[str, Any]


class KafkaMessagePayload(BaseModel):
    """Standard Kafka message payload"""
    transaction_id: str
    patient_id: str
    patient_name: str
    payload: Dict[str, Any]
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class FHIRValidationResult(BaseModel):
    """Result of FHIR validation"""
    is_valid: bool
    errors: Optional[List[str]] = None
    warnings: Optional[List[str]] = None


class HospitalResponse(BaseModel):
    """CoverageEligibilityResponse Bundle callback from hospital/Dhamani system"""
    original_id: str
    fhir_response: Dict[str, Any]


class FinalJSONResponse(BaseModel):
    """Final JSON response sent back to client"""
    internal_patient_id: str
    external_reference_id: str
    sync_status: str  # SUCCESS, FAILED
    completed_at: datetime
