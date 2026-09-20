"""
JSON <-> FHIR Patient mapping, per implementation.md sections 6.3 and 6.5.
This is the generic Patient resource the plan is built around - not the
CoverageEligibilityRequest/Claim domain the rest of this codebase's
shared/fhir_utils.py handles. Kept separate on purpose so the rebuilt
pipeline's mapping logic isn't tangled with the pre-existing one.
"""
from typing import Any, Dict

GENDER = {"Male": "male", "Female": "female", "Other": "other", "Unknown": "unknown"}


def to_fhir_patient(src: Dict[str, Any]) -> Dict[str, Any]:
    """JSON {patientId, name, gender, dob} -> FHIR R4 Patient resource"""
    first, *rest = src["name"].split()
    return {
        "resourceType": "Patient",
        "id": str(src["patientId"]),
        "identifier": [{"system": "urn:company:patient-id", "value": str(src["patientId"])}],
        "name": [{
            "use": "official",
            "family": " ".join(rest) or first,
            "given": [first] if rest else [],
        }],
        "gender": GENDER[src["gender"]],
        "birthDate": str(src["dob"]),
    }


def to_internal_response(result: Dict[str, Any], error: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Maps the FHIR communication result (http_status/body from fhir-comm, or
    an envelope error) to the internal JSON response format.
    """
    if error:
        return {"status": "FAILED", "reason": error.get("detail")}
    body = result.get("body") or {}
    return {
        "status": "SUCCESS",
        "fhirId": body.get("id"),
        "versionId": body.get("meta", {}).get("versionId"),
    }
