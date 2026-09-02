"""
FHIR transformation utilities for Dhamani CoverageEligibility messages
"""
import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import uuid

logger = logging.getLogger(__name__)


def _find_resource(bundle: Dict[str, Any], resource_type: str) -> Dict[str, Any]:
    """Find the first entry of a given resourceType inside a FHIR Bundle"""
    for entry in bundle.get("entry", []) or []:
        resource = entry.get("resource", {})
        if resource.get("resourceType") == resource_type:
            return resource
    return {}


def _first(items: Optional[List[Any]]) -> Dict[str, Any]:
    if items:
        return items[0]
    return {}


def _extension_code(resource: Dict[str, Any], url_keyword: str) -> Optional[str]:
    for extension in resource.get("extension", []) or []:
        if url_keyword in extension.get("url", ""):
            coding = _first(extension.get("valueCodeableConcept", {}).get("coding"))
            return coding.get("code")
    return None


def json_to_fhir_patient(request_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform a flattened CoverageEligibilityRequest JSON into a Dhamani-format
    FHIR message Bundle (MessageHeader, CoverageEligibilityRequest, Organization,
    Patient, Coverage, Organization)

    Args:
        request_data: Flattened eligibility request JSON

    Returns:
        FHIR message Bundle
    """
    try:
        message_header_in = request_data.get("messageHeader", {}) or {}

        patient_identifier = request_data.get("patientIdentifier")
        provider_identifier = request_data.get("providerIdentifier")
        insurer_identifier = request_data.get("insurerIdentifier")
        provider_name = request_data.get("providerName")
        insurer_name = request_data.get("insurerName")

        request_id = (
            request_data.get("id")
            or request_data.get("identifier")
            or str(uuid.uuid4())
        )

        bundle_id = str(uuid.uuid4())
        message_header_id = message_header_in.get("id") or str(uuid.uuid4())

        insurance = request_data.get("insurances") or [{
            "focal": True,
            "coverage": {"reference": f"Coverage/{patient_identifier}"}
        }]

        fhir_bundle = {
            "resourceType": "Bundle",
            "id": bundle_id,
            "meta": {
                "profile": ["http://dhamani.om/fhir/om/dhamani-fs/StructureDefinition/bundle|1.0.0"]
            },
            "type": "message",
            "timestamp": datetime.utcnow().isoformat(),
            "entry": [
                {
                    "fullUrl": f"http://{provider_identifier}.Dhamani.om/MessageHeader/{message_header_id}",
                    "resource": {
                        "resourceType": "MessageHeader",
                        "id": message_header_id,
                        "meta": {
                            "profile": ["http://dhamani.om/fhir/om/dhamani-fs/StructureDefinition/message-header|1.0.0"]
                        },
                        "eventCoding": {
                            "system": "http://dhamani.om/terminology/CodeSystem/om-message-events",
                            "code": message_header_in.get("eventCoding", "eligibility-request")
                        },
                        "destination": [{
                            "endpoint": f"http://{insurer_identifier}.Dhamani.om/$process-message",
                            "receiver": {
                                "type": "Organization",
                                "identifier": {
                                    "system": "http://dhamani.om/license/payer-license",
                                    "value": message_header_in.get("destinationReceiverIdentifier") or insurer_identifier
                                },
                                "display": insurer_name
                            }
                        }],
                        "sender": {
                            "type": "Organization",
                            "identifier": {
                                "system": "http://dhamani.om/license/provider-license",
                                "value": message_header_in.get("senderIdentifier") or provider_identifier
                            },
                            "display": provider_name
                        },
                        "source": {
                            "endpoint": f"http://{provider_identifier}.Dhamani.om"
                        },
                        "focus": [{
                            "reference": message_header_in.get("focus")
                            or f"http://{provider_identifier}.Dhamani.om/CoverageEligibilityRequest/{request_id}"
                        }]
                    }
                },
                {
                    "fullUrl": f"http://{provider_identifier}.Dhamani.om/CoverageEligibilityRequest/{request_id}",
                    "resource": {
                        "resourceType": "CoverageEligibilityRequest",
                        "id": request_id,
                        "meta": {
                            "profile": ["http://dhamani.om/fhir/om/dhamani-fs/StructureDefinition/eligibility-request|1.0.0"]
                        },
                        "identifier": [{
                            "system": request_data.get("identifierSystem"),
                            "value": request_data.get("identifier") or request_id
                        }],
                        "status": request_data.get("status", "active"),
                        "priority": {
                            "coding": [{
                                "system": "http://terminology.hl7.org/CodeSystem/processpriority",
                                "code": request_data.get("priority", "normal")
                            }]
                        },
                        "purpose": request_data.get("purpose", ["discovery"]),
                        "patient": {"reference": f"Patient/{patient_identifier}"},
                        "servicedDate": request_data.get("servicedDate"),
                        "created": request_data.get("created"),
                        "provider": {"reference": f"Organization/{provider_identifier}"},
                        "insurer": {"reference": f"Organization/{insurer_identifier}"},
                        "insurance": insurance
                    }
                },
                {
                    "fullUrl": f"http://{provider_identifier}.Dhamani.om/Organization/{provider_identifier}",
                    "resource": {
                        "resourceType": "Organization",
                        "id": provider_identifier,
                        "meta": {
                            "profile": ["http://dhamani.om/fhir/om/dhamani-fs/StructureDefinition/provider-organization|1.0.0"]
                        },
                        "identifier": [{
                            "system": "http://dhamani.om/license/provider-license",
                            "value": provider_identifier
                        }],
                        "active": True,
                        "type": [{
                            "coding": [{
                                "system": "http://dhamani.om/terminology/CodeSystem/organization-type",
                                "code": "prov"
                            }]
                        }],
                        "name": provider_name
                    }
                },
                {
                    "fullUrl": f"http://{provider_identifier}.Dhamani.om/Patient/{patient_identifier}",
                    "resource": {
                        "resourceType": "Patient",
                        "id": patient_identifier,
                        "meta": {
                            "profile": ["http://dhamani.om/fhir/om/dhamani-fs/StructureDefinition/patient|1.0.0"]
                        },
                        "identifier": [{
                            "type": {
                                "coding": [{
                                    "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                                    "code": request_data.get("patientIdentifierType", "NI")
                                }]
                            },
                            "system": request_data.get("patientIdentifierSystem"),
                            "value": patient_identifier
                        }],
                        "active": True
                    }
                },
                {
                    "fullUrl": f"http://{provider_identifier}.Dhamani.om/Coverage/{patient_identifier}",
                    "resource": {
                        "resourceType": "Coverage",
                        "id": patient_identifier,
                        "meta": {
                            "profile": ["http://dhamani.om/fhir/om/dhamani-fs/StructureDefinition/coverage|1.0.0"]
                        },
                        "status": "active",
                        "beneficiary": {"reference": f"Patient/{patient_identifier}"},
                        "relationship": {
                            "coding": [{
                                "system": "http://terminology.hl7.org/CodeSystem/subscriber-relationship",
                                "code": "self"
                            }]
                        },
                        "payor": [{"reference": f"Organization/{insurer_identifier}"}]
                    }
                },
                {
                    "fullUrl": f"http://{provider_identifier}.Dhamani.om/Organization/{insurer_identifier}",
                    "resource": {
                        "resourceType": "Organization",
                        "id": insurer_identifier,
                        "meta": {
                            "profile": ["http://dhamani.om/fhir/om/dhamani-fs/StructureDefinition/insurer-organization|1.0.0"]
                        },
                        "identifier": [{
                            "system": "http://dhamani.om/license/payer-license",
                            "value": insurer_identifier
                        }],
                        "active": True,
                        "type": [{
                            "coding": [{
                                "system": "http://dhamani.om/terminology/CodeSystem/organization-type",
                                "code": "ins"
                            }]
                        }],
                        "name": insurer_name
                    }
                }
            ]
        }

        logger.info(f"✅ Transformed JSON to FHIR eligibility Bundle: {bundle_id}")
        return fhir_bundle

    except Exception as e:
        logger.error(f"❌ Error transforming JSON to FHIR: {e}")
        raise


def fhir_to_json_response(fhir_bundle: Dict[str, Any], original_patient_id: str) -> Dict[str, Any]:
    """
    Transform a Dhamani CoverageEligibilityResponse FHIR Bundle back into the
    flattened JSON response format

    Args:
        fhir_bundle: FHIR message Bundle received from the hospital/Dhamani
        original_patient_id: Correlation id used only as a fallback

    Returns:
        Flattened JSON eligibility response
    """
    try:
        message_header = _find_resource(fhir_bundle, "MessageHeader")
        elig_response = _find_resource(fhir_bundle, "CoverageEligibilityResponse")
        coverage = _find_resource(fhir_bundle, "Coverage")

        patient_identifier = elig_response.get("patient", {}).get("identifier", {}) or {}
        insurer_identifier = elig_response.get("insurer", {}).get("identifier", {}) or {}
        request_identifier = elig_response.get("request", {}).get("identifier", {}) or {}
        coverage_class = _first(coverage.get("class"))
        errors = elig_response.get("error", []) or []

        outcome = elig_response.get("outcome")

        json_response = {
            "id": elig_response.get("id"),
            "requestIdentifierSystem": request_identifier.get("system"),
            "resourceType": elig_response.get("resourceType"),
            "extensionNotInForceReason": _extension_code(elig_response, "not-in-force-reason"),
            "identifier": _first(elig_response.get("identifier")).get("value"),
            "status": elig_response.get("status"),
            "purpose": elig_response.get("purpose"),
            "patientIdentifier": patient_identifier.get("value") or original_patient_id,
            "patientIdentifierSystem": patient_identifier.get("system"),
            "patientIdentifierType": _first(patient_identifier.get("type", {}).get("coding")).get("code"),
            "servicedDate": elig_response.get("servicedDate"),
            "servicedPeriodStart": elig_response.get("servicedPeriod", {}).get("start"),
            "servicedPeriodEnd": elig_response.get("servicedPeriod", {}).get("end"),
            "created": elig_response.get("created"),
            "requestIdentifier": request_identifier.get("value"),
            "outcome": outcome,
            "disposition": elig_response.get("disposition"),
            "insurerIdentifier": insurer_identifier.get("value"),
            "insuranceExtensionNotInForceReason": _extension_code(elig_response, "insurance-not-in-force-reason"),
            "coverageId": coverage.get("id"),
            "coverageIdentifier": _first(coverage.get("identifier")).get("value"),
            "coverageType": _first(coverage.get("type", {}).get("coding")).get("code"),
            "coverageBeneficiaryIdentifier": patient_identifier.get("value") or original_patient_id,
            "coverageBeneficiaryIdentifierType": _first(patient_identifier.get("type", {}).get("coding")).get("code"),
            "coverageBeneficiaryIdentifierSystem": patient_identifier.get("system"),
            "coverageRelationship": _first(coverage.get("relationship", {}).get("coding")).get("code"),
            "coveragePeriodStart": coverage.get("period", {}).get("start"),
            "coveragePeriodEnd": coverage.get("period", {}).get("end"),
            "coveragePayorIdentifier": insurer_identifier.get("value"),
            "coverageClassType": coverage_class.get("type", {}).get("coding", [{}])[0].get("code") if coverage_class.get("type") else None,
            "coverageClassValue": coverage_class.get("value"),
            "coverageClassName": coverage_class.get("name"),
            "insurerName": message_header.get("destination", [{}])[0].get("receiver", {}).get("display"),
            "providerName": message_header.get("sender", {}).get("display"),
            "costToBeneficiaries": elig_response.get("insurance", []),
            "item": elig_response.get("item", []),
            "error": [
                {
                    "errorExtensionExpression": _first(error.get("code", {}).get("coding")).get("display"),
                    "errorCode": _first(error.get("code", {}).get("coding")).get("code")
                }
                for error in errors
            ],
            "messageHeader": {
                "id": message_header.get("id"),
                "eventCoding": message_header.get("eventCoding", {}).get("code"),
                "destinationReceiverIdentifier": message_header.get("destination", [{}])[0].get("receiver", {}).get("identifier", {}).get("value"),
                "senderIdentifier": message_header.get("sender", {}).get("identifier", {}).get("value"),
                "focus": _first(message_header.get("focus")).get("reference"),
                "responseIdentifier": message_header.get("response", {}).get("identifier"),
                "responseCode": message_header.get("response", {}).get("code")
            },
            "sync_status": "SUCCESS" if outcome == "complete" and not errors else "FAILED",
            "completed_at": datetime.utcnow().isoformat()
        }

        logger.info(f"✅ Transformed FHIR eligibility response to JSON: {json_response.get('patientIdentifier')}")
        return json_response

    except Exception as e:
        logger.error(f"❌ Error transforming FHIR to JSON: {e}")
        raise


def validate_fhir_patient(fhir_bundle: Dict[str, Any]) -> tuple[bool, Optional[list]]:
    """
    Validate a Dhamani eligibility request Bundle

    Args:
        fhir_bundle: FHIR message Bundle

    Returns:
        Tuple of (is_valid, error_list)
    """
    errors = []

    if fhir_bundle.get("resourceType") != "Bundle":
        errors.append("resourceType must be 'Bundle'")

    if fhir_bundle.get("type") != "message":
        errors.append("Bundle type must be 'message'")

    if not fhir_bundle.get("id"):
        errors.append("Bundle id is required")

    message_header = _find_resource(fhir_bundle, "MessageHeader")
    if not message_header:
        errors.append("Bundle must contain a MessageHeader entry")

    elig_request = _find_resource(fhir_bundle, "CoverageEligibilityRequest")
    if not elig_request:
        errors.append("Bundle must contain a CoverageEligibilityRequest entry")
    else:
        if not elig_request.get("patient", {}).get("reference"):
            errors.append("CoverageEligibilityRequest.patient reference is required")
        if not elig_request.get("insurer", {}).get("reference"):
            errors.append("CoverageEligibilityRequest.insurer reference is required")

    is_valid = len(errors) == 0

    if is_valid:
        logger.info(f"✅ FHIR eligibility Bundle validated: {fhir_bundle.get('id')}")
    else:
        logger.warning(f"⚠️ FHIR validation errors for {fhir_bundle.get('id')}: {errors}")

    return is_valid, errors if errors else None
