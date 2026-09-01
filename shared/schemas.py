"""
Pydantic schemas for data validation
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


class PatientBase(BaseModel):
    patient_id: str
    name: str
    status: str = "new_admission"


class PatientRequest(PatientBase):
    """Schema for incoming patient JSON"""
    pass


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
    """Response from hospital system"""
    original_id: str
    hospital_system_id: str
    status: str
    timestamp: datetime
    fhir_response: Dict[str, Any]


class FinalJSONResponse(BaseModel):
    """Final JSON response sent back to client"""
    internal_patient_id: str
    external_reference_id: str
    sync_status: str  # SUCCESS, FAILED
    completed_at: datetime
