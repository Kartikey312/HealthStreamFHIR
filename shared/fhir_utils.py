"""
FHIR transformation utilities
"""
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import uuid

logger = logging.getLogger(__name__)


def json_to_fhir_patient(patient_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform JSON patient data to FHIR Patient resource format
    
    Args:
        patient_data: Patient data in JSON format
        
    Returns:
        FHIR Patient resource
    """
    try:
        fhir_patient = {
            "resourceType": "Patient",
            "id": patient_data.get("patient_id") or patient_data.get("id") or str(uuid.uuid4()),
            "name": [
                {
                    "use": "official",
                    "text": patient_data.get("name", "Unknown Patient"),
                    "given": [patient_data.get("given_name", "")],
                    "family": patient_data.get("family_name", "")
                }
            ],
            "active": patient_data.get("status") == "new_admission",
            "telecom": [],
            "address": [],
            "meta": {
                "lastUpdated": datetime.utcnow().isoformat(),
                "source": "json2fhir"
            }
        }
        
        # Add optional telecom
        if phone := patient_data.get("phone"):
            fhir_patient["telecom"].append({
                "system": "phone",
                "value": phone
            })
        
        if email := patient_data.get("email"):
            fhir_patient["telecom"].append({
                "system": "email",
                "value": email
            })
        
        # Add optional address
        if address := patient_data.get("address"):
            fhir_patient["address"].append({
                "use": "home",
                "text": address,
                "line": [address]
            })
        
        logger.info(f"✅ Transformed JSON to FHIR: {fhir_patient['id']}")
        return fhir_patient
        
    except Exception as e:
        logger.error(f"❌ Error transforming JSON to FHIR: {e}")
        raise


def fhir_to_json_response(fhir_response: Dict[str, Any], original_patient_id: str) -> Dict[str, Any]:
    """
    Transform FHIR response back to JSON format
    
    Args:
        fhir_response: FHIR response from hospital
        original_patient_id: Original patient ID
        
    Returns:
        JSON formatted response
    """
    try:
        json_response = {
            "internal_patient_id": original_patient_id,
            "external_reference_id": fhir_response.get("id"),
            "sync_status": "SUCCESS" if fhir_response.get("status") == "201 Created" else "FAILED",
            "hospital_response_code": fhir_response.get("code", 200),
            "hospital_system_id": fhir_response.get("hospitalSystemId"),
            "completed_at": datetime.utcnow().isoformat(),
            "fhir_response": fhir_response
        }
        
        logger.info(f"✅ Transformed FHIR to JSON: {original_patient_id}")
        return json_response
        
    except Exception as e:
        logger.error(f"❌ Error transforming FHIR to JSON: {e}")
        raise


def validate_fhir_patient(fhir_data: Dict[str, Any]) -> tuple[bool, Optional[list]]:
    """
    Validate FHIR Patient resource
    
    Args:
        fhir_data: FHIR Patient resource
        
    Returns:
        Tuple of (is_valid, error_list)
    """
    errors = []
    
    # Check required fields
    if fhir_data.get("resourceType") != "Patient":
        errors.append("resourceType must be 'Patient'")
    
    if not fhir_data.get("id"):
        errors.append("Patient id is required")
    
    if not fhir_data.get("name"):
        errors.append("Patient name is required")
    
    # Validate name structure
    if fhir_data.get("name"):
        for name in fhir_data.get("name", []):
            if not isinstance(name, dict):
                errors.append("Name entries must be objects")
            if not name.get("text"):
                errors.append("Name text is required")
    
    # Validate telecom
    if fhir_data.get("telecom"):
        for telecom in fhir_data.get("telecom", []):
            valid_systems = ["phone", "fax", "email", "pager", "url", "sms", "other"]
            if telecom.get("system") not in valid_systems:
                errors.append(f"Invalid telecom system: {telecom.get('system')}")
    
    is_valid = len(errors) == 0
    
    if is_valid:
        logger.info(f"✅ FHIR Patient validated: {fhir_data.get('id')}")
    else:
        logger.warning(f"⚠️ FHIR validation errors for {fhir_data.get('id')}: {errors}")
    
    return is_valid, errors if errors else None
