"""
Pydantic schemas for data validation
"""
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Dict, Any, Union
from datetime import datetime


def _as_list(value):
    """Accept purpose as either a string or a list of strings, normalize to a list"""
    if value is None:
        return value
    if isinstance(value, list):
        return value
    return [value]


def _as_scalar(value):
    """Accept purpose as either a string or a list of strings, normalize to a string"""
    if isinstance(value, list):
        return value[0] if value else None
    return value


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

    @field_validator("purpose", mode="before")
    @classmethod
    def _normalize_purpose(cls, v):
        return _as_list(v)

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


class MessageHeaderResponseIn(BaseModel):
    """Nested messageHeader block of a flattened eligibility response"""
    id: Optional[str] = None
    eventCoding: Optional[str] = None
    destinationReceiverIdentifier: Optional[str] = None
    senderIdentifier: Optional[str] = None
    focus: Optional[str] = None
    responseIdentifier: Optional[str] = None
    responseCode: Optional[str] = None


class ErrorIn(BaseModel):
    """Nested error entry of a flattened eligibility response"""
    errorExtensionExpression: Optional[str] = None
    errorCode: Optional[str] = None


class EligibilityResponseIn(BaseModel):
    """Schema for our outbound flattened CoverageEligibilityResponse JSON (our decision, sent to Dhamani)"""
    id: Optional[str] = None
    requestIdentifierSystem: Optional[str] = None
    resourceType: Optional[str] = "CoverageEligibilityResponse"
    extensionNotInForceReason: Optional[str] = None
    identifier: Optional[str] = None
    status: str = "active"
    purpose: Optional[str] = "discovery"

    @field_validator("purpose", mode="before")
    @classmethod
    def _normalize_purpose(cls, v):
        return _as_scalar(v)

    patientIdentifier: str
    patientIdentifierSystem: Optional[str] = None
    patientIdentifierType: Optional[str] = "NI"
    servicedDate: Optional[str] = None
    servicedPeriodStart: Optional[str] = None
    servicedPeriodEnd: Optional[str] = None
    created: Optional[str] = None
    requestIdentifier: Optional[str] = None
    outcome: str = "complete"
    disposition: Optional[str] = None
    insurerIdentifier: str
    insuranceExtensionNotInForceReason: Optional[str] = None
    coverageId: Optional[str] = None
    coverageIdentifier: Optional[str] = None
    coverageType: Optional[str] = None
    coverageSubscriberIdentifier: Optional[str] = None
    coverageSubscriberIdentifierType: Optional[str] = None
    coverageSubscriberIdentifierSystem: Optional[str] = None
    coverageSubscriberId: Optional[str] = None
    coverageBeneficiaryIdentifier: Optional[str] = None
    coverageBeneficiaryIdentifierType: Optional[str] = None
    coverageBeneficiaryIdentifierSystem: Optional[str] = None
    coveragePolicyHolderIdentifier: Optional[str] = None
    coveragePolicyHolderIdentifierType: Optional[str] = None
    coveragePolicyHolderIdentifierSystem: Optional[str] = None
    coveragePolicyHolderIdentifierTypeSystem: Optional[str] = None
    coverageDependent: Optional[str] = None
    coverageRelationship: Optional[str] = None
    coveragePeriodStart: Optional[str] = None
    coveragePeriodEnd: Optional[str] = None
    coveragePayorIdentifier: Optional[str] = None
    coveragePayorIdentifierType: Optional[str] = None
    coverageSubrogation: Optional[bool] = None
    coverageClassType: Optional[str] = None
    coverageClassValue: Optional[str] = None
    coverageClassName: Optional[str] = None
    coverageNetwork: Optional[str] = None
    insuranceInforce: Optional[bool] = None
    insuranceBenefitPeriodStart: Optional[str] = None
    insuranceBenefitPeriodEnd: Optional[str] = None
    insurerName: Optional[str] = None
    providerName: Optional[str] = None
    costToBeneficiaries: List[Dict[str, Any]] = Field(default_factory=list)
    item: List[Dict[str, Any]] = Field(default_factory=list)
    error: List[ErrorIn] = Field(default_factory=list)
    messageHeader: MessageHeaderResponseIn = Field(default_factory=MessageHeaderResponseIn)


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
