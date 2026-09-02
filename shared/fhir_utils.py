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


def _format_datetime(value: Optional[str]) -> Optional[str]:
    """Normalize a FHIR ISO-8601 datetime into 'YYYY-MM-DD HH:MM:SS'"""
    if not value:
        return value
    try:
        return datetime.fromisoformat(value).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return value


def _find_organization_by_type(bundle: Dict[str, Any], type_code: str) -> Dict[str, Any]:
    """Find the Organization entry tagged with the given organization-type code (e.g. 'prov', 'ins')"""
    for entry in bundle.get("entry", []) or []:
        resource = entry.get("resource", {})
        if resource.get("resourceType") != "Organization":
            continue
        for type_entry in resource.get("type", []) or []:
            coding = _first(type_entry.get("coding"))
            if coding.get("code") == type_code:
                return resource
    return {}


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


def fhir_to_json_request(fhir_bundle: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform an incoming Dhamani CoverageEligibilityRequest FHIR Bundle
    (sent to us by Dhamani on behalf of a provider) into a flattened JSON
    eligibility request

    Args:
        fhir_bundle: FHIR message Bundle received from Dhamani

    Returns:
        Flattened JSON eligibility request
    """
    try:
        message_header = _find_resource(fhir_bundle, "MessageHeader")
        elig_request = _find_resource(fhir_bundle, "CoverageEligibilityRequest")
        patient = _find_resource(fhir_bundle, "Patient")
        coverage = _find_resource(fhir_bundle, "Coverage")
        provider_org = _find_organization_by_type(fhir_bundle, "prov")
        insurer_org = _find_organization_by_type(fhir_bundle, "ins")

        patient_identifier = _first(patient.get("identifier"))
        request_identifier = _first(elig_request.get("identifier"))
        coverage_payor = _first(coverage.get("payor"))
        coverage_class = _first(coverage.get("class"))

        provider_identifier = (
            _first(provider_org.get("identifier")).get("value")
            or message_header.get("sender", {}).get("identifier", {}).get("value")
        )
        insurer_identifier = (
            _first(insurer_org.get("identifier")).get("value")
            or elig_request.get("insurer", {}).get("reference", "").split("/")[-1]
            or message_header.get("destination", [{}])[0].get("receiver", {}).get("identifier", {}).get("value")
        )

        purpose = elig_request.get("purpose")
        if isinstance(purpose, list):
            purpose = _first(purpose) if purpose and isinstance(purpose[0], dict) else (purpose[0] if purpose else None)

        json_request = {
            "id": elig_request.get("id"),
            "resourceType": elig_request.get("resourceType"),
            "identifier": request_identifier.get("value"),
            "identifierSystem": request_identifier.get("system"),
            "status": elig_request.get("status"),
            "priority": _first(elig_request.get("priority", {}).get("coding")).get("code"),
            "purpose": purpose,
            "patientIdentifier": patient_identifier.get("value"),
            "patientIdentifierType": _first(patient_identifier.get("type", {}).get("coding")).get("code"),
            "patientIdentifierSystem": patient_identifier.get("system"),
            "servicedDate": elig_request.get("servicedDate"),
            "created": _format_datetime(elig_request.get("created")),
            "insurerIdentifier": insurer_identifier,
            "insurerIdentifierType": None,
            "insurerName": insurer_org.get("name") or message_header.get("destination", [{}])[0].get("receiver", {}).get("display"),
            "providerIdentifier": provider_identifier,
            "providerIdentifierType": None,
            "providerName": provider_org.get("name") or message_header.get("sender", {}).get("display"),
            "insurances": elig_request.get("insurance", []),
            "coverageId": coverage.get("id"),
            "coverageIdentifier": _first(coverage.get("identifier")).get("value"),
            "coverageType": _first(coverage.get("type", {}).get("coding")).get("code"),
            "coverageBeneficiaryIdentifier": patient_identifier.get("value"),
            "coverageRelationship": _first(coverage.get("relationship", {}).get("coding")).get("code"),
            "coveragePayorIdentifier": coverage_payor.get("identifier", {}).get("value") or insurer_identifier,
            "coverageClassType": coverage_class.get("type", {}).get("coding", [{}])[0].get("code") if coverage_class.get("type") else None,
            "coverageClassValue": coverage_class.get("value"),
            "coverageClassName": coverage_class.get("name"),
            "extensionKhatmAbsenceReason": _extension_code(elig_request, "khatm-absence-reason"),
            "messageHeader": {
                "id": message_header.get("id"),
                "eventCoding": message_header.get("eventCoding", {}).get("code"),
                "destinationReceiverIdentifier": message_header.get("destination", [{}])[0].get("receiver", {}).get("identifier", {}).get("value"),
                "senderIdentifier": message_header.get("sender", {}).get("identifier", {}).get("value"),
                "focus": _first(message_header.get("focus")).get("reference")
            }
        }

        logger.info(f"✅ Transformed FHIR eligibility request to JSON: {json_request.get('patientIdentifier')}")
        return json_request

    except Exception as e:
        logger.error(f"❌ Error transforming FHIR request to JSON: {e}")
        raise


def json_to_fhir_response(response_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform our flattened JSON eligibility decision into a Dhamani-format
    FHIR CoverageEligibilityResponse message Bundle, to send back to Dhamani

    Args:
        response_data: Flattened eligibility response JSON (our decision)

    Returns:
        FHIR message Bundle
    """
    try:
        message_header_in = response_data.get("messageHeader", {}) or {}

        response_id = response_data.get("id") or str(uuid.uuid4())
        bundle_id = str(uuid.uuid4())
        message_header_id = message_header_in.get("id") or str(uuid.uuid4())

        provider_identifier = message_header_in.get("destinationReceiverIdentifier")
        insurer_identifier = response_data.get("insurerIdentifier") or message_header_in.get("senderIdentifier")
        provider_name = response_data.get("providerName")
        insurer_name = response_data.get("insurerName")

        purpose = response_data.get("purpose")
        if purpose and not isinstance(purpose, list):
            purpose = [purpose]

        extensions = []
        if response_data.get("extensionNotInForceReason"):
            extensions.append({
                "url": "http://dhamani.om/fhir/om/dhamani-fs/StructureDefinition/extension-not-in-force-reason",
                "valueCodeableConcept": {
                    "coding": [{
                        "system": "http://dhamani.om/terminology/CodeSystem/not-in-force-reason",
                        "code": response_data.get("extensionNotInForceReason")
                    }]
                }
            })

        insurance_extensions = []
        if response_data.get("insuranceExtensionNotInForceReason"):
            insurance_extensions.append({
                "url": "http://dhamani.om/fhir/om/dhamani-fs/StructureDefinition/insurance-not-in-force-reason",
                "valueCodeableConcept": {
                    "coding": [{
                        "system": "http://dhamani.om/terminology/CodeSystem/not-in-force-reason",
                        "code": response_data.get("insuranceExtensionNotInForceReason")
                    }]
                }
            })

        insurance = [{
            "coverage": {"reference": f"Coverage/{response_data.get('coverageBeneficiaryIdentifier')}"},
            "inforce": response_data.get("insuranceInforce"),
            "benefitPeriod": {
                "start": response_data.get("insuranceBenefitPeriodStart") or None,
                "end": response_data.get("insuranceBenefitPeriodEnd") or None
            },
            "extension": insurance_extensions,
            "item": response_data.get("item", [])
        }]

        errors = [
            {
                "code": {
                    "coding": [{
                        "code": error.get("errorCode"),
                        "display": error.get("errorExtensionExpression")
                    }]
                }
            }
            for error in response_data.get("error", []) or []
        ]

        elig_response_resource = {
            "resourceType": "CoverageEligibilityResponse",
            "id": response_id,
            "meta": {
                "profile": ["http://dhamani.om/fhir/om/dhamani-fs/StructureDefinition/eligibility-response|1.0.0"]
            },
            "extension": extensions,
            "identifier": [{
                "system": f"http://{insurer_identifier}.Dhamani.om/coverageeligibilityresponse",
                "value": response_data.get("identifier") or str(uuid.uuid4())
            }],
            "status": response_data.get("status", "active"),
            "purpose": purpose or ["discovery"],
            "patient": {
                "identifier": {
                    "type": {
                        "coding": [{
                            "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                            "code": response_data.get("patientIdentifierType", "NI")
                        }]
                    },
                    "system": response_data.get("patientIdentifierSystem"),
                    "value": response_data.get("patientIdentifier")
                }
            },
            "servicedDate": response_data.get("servicedDate"),
            "created": response_data.get("created"),
            "request": {
                "type": "CoverageEligibilityRequest",
                "identifier": {
                    "system": response_data.get("requestIdentifierSystem"),
                    "value": response_data.get("requestIdentifier")
                }
            },
            "outcome": response_data.get("outcome", "complete"),
            "disposition": response_data.get("disposition"),
            "insurer": {
                "identifier": {
                    "type": {
                        "coding": [{
                            "system": "http://dhamani.om/terminology/CodeSystem/organization-identifier-type",
                            "code": "NIP"
                        }]
                    },
                    "system": "http://dhamani.om/license/payer-license",
                    "value": insurer_identifier
                }
            },
            "insurance": insurance,
            "error": errors
        }

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
                    "fullUrl": f"http://{insurer_identifier}.Dhamani.om/MessageHeader/{message_header_id}",
                    "resource": {
                        "resourceType": "MessageHeader",
                        "id": message_header_id,
                        "meta": {
                            "profile": ["http://dhamani.om/fhir/om/dhamani-fs/StructureDefinition/message-header|1.0.0"]
                        },
                        "eventCoding": {
                            "system": "http://dhamani.om/terminology/CodeSystem/om-message-events",
                            "code": message_header_in.get("eventCoding", "eligibility-response")
                        },
                        "destination": [{
                            "endpoint": f"http://{provider_identifier}.Dhamani.om/$process-message",
                            "receiver": {
                                "type": "Organization",
                                "identifier": {
                                    "system": "http://dhamani.om/license/provider-license",
                                    "value": provider_identifier
                                },
                                "display": provider_name
                            }
                        }],
                        "sender": {
                            "type": "Organization",
                            "identifier": {
                                "system": "http://dhamani.om/license/payer-license",
                                "value": insurer_identifier
                            },
                            "display": insurer_name
                        },
                        "source": {
                            "endpoint": f"http://{insurer_identifier}.Dhamani.om"
                        },
                        "response": {
                            "identifier": message_header_in.get("responseIdentifier") or str(uuid.uuid4()),
                            "code": message_header_in.get("responseCode", "ok")
                        },
                        "focus": [{
                            "reference": message_header_in.get("focus")
                            or f"http://{insurer_identifier}.Dhamani.om/CoverageEligibilityResponse/{response_id}"
                        }]
                    }
                },
                {
                    "fullUrl": f"http://{insurer_identifier}.Dhamani.om/CoverageEligibilityResponse/{response_id}",
                    "resource": elig_response_resource
                }
            ]
        }

        coverage_fields_present = any([
            response_data.get("coverageId"), response_data.get("coverageIdentifier"),
            response_data.get("coverageType"), response_data.get("coverageClassType")
        ])
        if coverage_fields_present:
            coverage_resource = {
                "resourceType": "Coverage",
                "id": response_data.get("coverageId") or response_data.get("coverageBeneficiaryIdentifier"),
                "meta": {
                    "profile": ["http://dhamani.om/fhir/om/dhamani-fs/StructureDefinition/coverage|1.0.0"]
                },
                "identifier": [{"value": response_data.get("coverageIdentifier")}] if response_data.get("coverageIdentifier") else [],
                "status": "active",
                "type": {"coding": [{"code": response_data.get("coverageType")}]} if response_data.get("coverageType") else None,
                "subscriber": {
                    "identifier": {
                        "value": response_data.get("coverageSubscriberIdentifier"),
                        "type": {"coding": [{"code": response_data.get("coverageSubscriberIdentifierType")}]},
                        "system": response_data.get("coverageSubscriberIdentifierSystem")
                    }
                } if response_data.get("coverageSubscriberIdentifier") else None,
                "subscriberId": response_data.get("coverageSubscriberId") or None,
                "beneficiary": {
                    "identifier": {
                        "value": response_data.get("coverageBeneficiaryIdentifier"),
                        "type": {"coding": [{"code": response_data.get("coverageBeneficiaryIdentifierType")}]},
                        "system": response_data.get("coverageBeneficiaryIdentifierSystem")
                    }
                },
                "policyHolder": {
                    "identifier": {
                        "value": response_data.get("coveragePolicyHolderIdentifier"),
                        "type": {
                            "coding": [{
                                "code": response_data.get("coveragePolicyHolderIdentifierType"),
                                "system": response_data.get("coveragePolicyHolderIdentifierTypeSystem")
                            }]
                        },
                        "system": response_data.get("coveragePolicyHolderIdentifierSystem")
                    }
                } if response_data.get("coveragePolicyHolderIdentifier") else None,
                "dependent": response_data.get("coverageDependent") or None,
                "relationship": {
                    "coding": [{
                        "system": "http://terminology.hl7.org/CodeSystem/subscriber-relationship",
                        "code": response_data.get("coverageRelationship")
                    }]
                } if response_data.get("coverageRelationship") else None,
                "period": {
                    "start": response_data.get("coveragePeriodStart"),
                    "end": response_data.get("coveragePeriodEnd")
                },
                "payor": [{
                    "identifier": {
                        "value": response_data.get("coveragePayorIdentifier"),
                        "type": {"coding": [{"code": response_data.get("coveragePayorIdentifierType")}]}
                    }
                }],
                "subrogation": response_data.get("coverageSubrogation"),
                "class": [{
                    "type": {"coding": [{"code": response_data.get("coverageClassType")}]},
                    "value": response_data.get("coverageClassValue"),
                    "name": response_data.get("coverageClassName")
                }] if response_data.get("coverageClassType") else [],
                "network": response_data.get("coverageNetwork") or None,
                "costToBeneficiary": response_data.get("costToBeneficiaries", [])
            }
            coverage_resource = {k: v for k, v in coverage_resource.items() if v is not None}

            fhir_bundle["entry"].append({
                "fullUrl": f"http://{insurer_identifier}.Dhamani.om/Coverage/{coverage_resource['id']}",
                "resource": coverage_resource
            })

        logger.info(f"✅ Transformed JSON to FHIR eligibility response Bundle: {bundle_id}")
        return fhir_bundle

    except Exception as e:
        logger.error(f"❌ Error transforming JSON to FHIR response: {e}")
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

        insurance_entry = _first(elig_response.get("insurance"))

        coverage_subscriber_identifier = (coverage.get("subscriber", {}) or {}).get("identifier", {}) or {}

        coverage_policy_holder_identifier = (coverage.get("policyHolder", {}) or {}).get("identifier", {}) or {}
        coverage_policy_holder_type_coding = _first(coverage_policy_holder_identifier.get("type", {}).get("coding"))

        coverage_payor_identifier = _first(coverage.get("payor")).get("identifier", {}) or insurer_identifier

        purpose = elig_response.get("purpose")
        if isinstance(purpose, list):
            purpose = _first(purpose) if purpose and isinstance(purpose[0], dict) else (purpose[0] if purpose else None)

        outcome = elig_response.get("outcome")

        json_response = {
            "id": elig_response.get("id"),
            "requestIdentifierSystem": request_identifier.get("system"),
            "resourceType": elig_response.get("resourceType"),
            "extensionNotInForceReason": _extension_code(elig_response, "not-in-force-reason"),
            "identifier": _first(elig_response.get("identifier")).get("value"),
            "status": elig_response.get("status"),
            "purpose": purpose,
            "patientIdentifier": patient_identifier.get("value") or original_patient_id,
            "patientIdentifierSystem": patient_identifier.get("system"),
            "patientIdentifierType": _first(patient_identifier.get("type", {}).get("coding")).get("code"),
            "servicedDate": elig_response.get("servicedDate"),
            "servicedPeriodStart": elig_response.get("servicedPeriod", {}).get("start"),
            "servicedPeriodEnd": elig_response.get("servicedPeriod", {}).get("end"),
            "created": _format_datetime(elig_response.get("created")),
            "requestIdentifier": request_identifier.get("value"),
            "outcome": outcome,
            "disposition": elig_response.get("disposition"),
            "insurerIdentifier": insurer_identifier.get("value"),
            "insuranceExtensionNotInForceReason": _extension_code(insurance_entry, "insurance-not-in-force-reason"),
            "coverageId": coverage.get("id"),
            "coverageIdentifier": _first(coverage.get("identifier")).get("value"),
            "coverageType": _first(coverage.get("type", {}).get("coding")).get("code"),
            "coverageSubscriberIdentifier": coverage_subscriber_identifier.get("value"),
            "coverageSubscriberIdentifierType": _first(coverage_subscriber_identifier.get("type", {}).get("coding")).get("code"),
            "coverageSubscriberIdentifierSystem": coverage_subscriber_identifier.get("system"),
            "coverageSubscriberId": coverage.get("subscriberId"),
            "coverageBeneficiaryIdentifier": patient_identifier.get("value") or original_patient_id,
            "coverageBeneficiaryIdentifierType": _first(patient_identifier.get("type", {}).get("coding")).get("code"),
            "coverageBeneficiaryIdentifierSystem": patient_identifier.get("system"),
            "coveragePolicyHolderIdentifier": coverage_policy_holder_identifier.get("value"),
            "coveragePolicyHolderIdentifierType": coverage_policy_holder_type_coding.get("code"),
            "coveragePolicyHolderIdentifierSystem": coverage_policy_holder_identifier.get("system"),
            "coveragePolicyHolderIdentifierTypeSystem": coverage_policy_holder_type_coding.get("system"),
            "coverageDependent": coverage.get("dependent"),
            "coverageRelationship": _first(coverage.get("relationship", {}).get("coding")).get("code"),
            "coveragePeriodStart": coverage.get("period", {}).get("start"),
            "coveragePeriodEnd": coverage.get("period", {}).get("end"),
            "coveragePayorIdentifier": coverage_payor_identifier.get("value") or insurer_identifier.get("value"),
            "coveragePayorIdentifierType": _first(coverage_payor_identifier.get("type", {}).get("coding")).get("code"),
            "coverageSubrogation": coverage.get("subrogation"),
            "coverageClassType": coverage_class.get("type", {}).get("coding", [{}])[0].get("code") if coverage_class.get("type") else None,
            "coverageClassValue": coverage_class.get("value"),
            "coverageClassName": coverage_class.get("name"),
            "coverageNetwork": coverage.get("network"),
            "insuranceInforce": insurance_entry.get("inforce"),
            "insuranceBenefitPeriodStart": insurance_entry.get("benefitPeriod", {}).get("start"),
            "insuranceBenefitPeriodEnd": insurance_entry.get("benefitPeriod", {}).get("end"),
            "insurerName": message_header.get("sender", {}).get("display"),
            "providerName": message_header.get("destination", [{}])[0].get("receiver", {}).get("display"),
            "costToBeneficiaries": coverage.get("costToBeneficiary", []),
            "item": insurance_entry.get("item", []),
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


def json_to_fhir_claim(preauth_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform a PreAuth claim JSON payload (as returned by
    usp_get_preauth_claims_details_by_claim_id) into a Dhamani-format FHIR
    message Bundle carrying a Claim resource.

    BEST-EFFORT MAPPING - not verified against a real Dhamani Claim profile
    or example (unlike json_to_fhir_patient/json_to_fhir_response, which were
    checked against real bundles). Standard FHIR R4 Claim fields map
    directly; columns with no FHIR Claim equivalent (batch_number,
    payer_share, patient_share, teleconsultation, package_extenstion, tax,
    payer_code*) are carried as placeholder custom extensions under
    http://dhamani.om/fhir/om/dhamani-fs/StructureDefinition/extension-claim-<field>,
    following the extension convention already used for CoverageEligibility.
    Replace these extension URLs with the real Dhamani Claim profile's once
    available.

    diagnosis[] and careTeam[] are left empty: FHIR requires
    diagnosis[x]/provider respectively on each entry, and the source tables
    (diagnosis, care_team) are still schema stubs (id + claim_id only, see
    preauth_tables_mysql.sql) with no real columns to populate them from.

    Args:
        preauth_json: Combined JSON payload from the PreAuth SP
            (usp_get_preauth_claims_details_by_claim_id, @AsJson-style output)

    Returns:
        FHIR message Bundle
    """
    try:
        claim = _first(preauth_json.get("Claims"))
        message_header_in = _first(preauth_json.get("MessageHeaders"))
        related_list = preauth_json.get("ClaimRelateds") or []
        insurance_list = preauth_json.get("ClaimRequestInsurances") or []
        supporting_info_list = preauth_json.get("Supportinginfo") or []
        item_list = preauth_json.get("ClaimRequestItems") or []
        detail_list = preauth_json.get("ClaimRequestDetail") or []

        claim_id = claim.get("id") or str(uuid.uuid4())
        patient_identifier = claim.get("patient_identifier")
        provider_identifier = claim.get("provider_identifier")
        insurer_identifier = claim.get("insurer_identifier")

        bundle_id = str(uuid.uuid4())
        message_header_id = message_header_in.get("id") or str(uuid.uuid4())

        ext_base = "http://dhamani.om/fhir/om/dhamani-fs/StructureDefinition/extension-claim-"

        def _value_x(entry: Dict[str, Any]) -> Dict[str, Any]:
            if entry.get("value_boolean") is not None:
                return {"valueBoolean": bool(entry.get("value_boolean"))}
            if entry.get("value_quantity") is not None:
                return {"valueQuantity": {"value": entry.get("value_quantity")}}
            if entry.get("value_attachment_url") or entry.get("value_attachment_data"):
                return {"valueAttachment": {
                    "contentType": entry.get("value_attachment_content_type"),
                    "data": entry.get("value_attachment_data"),
                    "title": entry.get("value_attachment_title"),
                    "url": entry.get("value_attachment_url")
                }}
            if entry.get("value_string") is not None:
                return {"valueString": entry.get("value_string")}
            return {}

        supporting_info = []
        for si in supporting_info_list:
            entry = {
                "sequence": si.get("sequence"),
                "category": {"coding": [{"code": si.get("category")}]} if si.get("category") else None,
                "code": {"coding": [{
                    "code": si.get("supporting_info_code"),
                    "display": si.get("supporting_info_display")
                }]} if si.get("supporting_info_code") else None,
                "reason": {"coding": [{"code": si.get("reason")}]} if si.get("reason") else None
            }
            if si.get("timing_start") or si.get("timing_end"):
                entry["timingPeriod"] = {"start": si.get("timing_start"), "end": si.get("timing_end")}
            elif si.get("timing_date"):
                entry["timingDate"] = si.get("timing_date")
            entry.update(_value_x(si))
            supporting_info.append({k: v for k, v in entry.items() if v is not None})

        def _build_detail(item_id: str) -> List[Dict[str, Any]]:
            details = []
            for d in detail_list:
                if d.get("item_id") != item_id:
                    continue
                detail = {
                    "sequence": d.get("sequence"),
                    "productOrService": {"coding": [{
                        "system": d.get("product_or_service_system"),
                        "code": d.get("product_or_service_code"),
                        "display": d.get("product_or_service_description")
                    }]},
                    "quantity": {"value": d.get("quantity")} if d.get("quantity") is not None else None,
                    "unitPrice": {"value": d.get("unit_price")} if d.get("unit_price") is not None else None,
                    "net": {"value": d.get("net")} if d.get("net") is not None else None,
                    "extension": [{"url": ext_base + "tax", "valueDecimal": d.get("tax")}]
                    if d.get("tax") is not None else None
                }
                details.append({k: v for k, v in detail.items() if v is not None})
            return details

        item = []
        for it in item_list:
            entry = {
                "sequence": it.get("sequence"),
                "productOrService": {"coding": [{
                    "system": it.get("product_or_service_system"),
                    "code": it.get("product_or_service_code"),
                    "display": it.get("product_or_service_description")
                }]},
                "quantity": {"value": it.get("quantity")} if it.get("quantity") is not None else None,
                "unitPrice": {"value": it.get("unit_price")} if it.get("unit_price") is not None else None,
                "factor": it.get("factor"),
                "net": {"value": it.get("net")} if it.get("net") is not None else None,
                "bodySite": {"coding": [{"code": it.get("body_site")}]} if it.get("body_site") else None,
                "subSite": [{"coding": [{"code": it.get("sub_site")}]}] if it.get("sub_site") else None
            }
            if it.get("care_team_sequence") is not None:
                entry["careTeamSequence"] = [it.get("care_team_sequence")]
            if it.get("diagnosis_sequence") is not None:
                entry["diagnosisSequence"] = [it.get("diagnosis_sequence")]
            if it.get("information_sequence") is not None:
                entry["informationSequence"] = [it.get("information_sequence")]
            if it.get("serviced_start") or it.get("serviced_end"):
                entry["servicedPeriod"] = {"start": it.get("serviced_start"), "end": it.get("serviced_end")}
            elif it.get("serviced_date"):
                entry["servicedDate"] = it.get("serviced_date")

            extensions = []
            for field, key in [
                ("batch_number", "batch-number"), ("expiry_date", "expiry-date"),
                ("patient_share", "patient-share"), ("payer_share", "payer-share"),
                ("serial_number", "serial-number"), ("teleconsultation", "teleconsultation"),
                ("package_extenstion", "package-extension"), ("payer_code", "payer-code"),
                ("payer_code_display", "payer-code-display"), ("payer_code_system", "payer-code-system"),
                ("tax", "tax")
            ]:
                value = it.get(field)
                if value is not None and value != "":
                    extensions.append({"url": ext_base + key, "valueString": str(value)})
            if extensions:
                entry["extension"] = extensions

            details = _build_detail(it.get("id"))
            if details:
                entry["detail"] = details

            item.append({k: v for k, v in entry.items() if v is not None})

        insurance = []
        for idx, ins in enumerate(insurance_list, start=1):
            insurance.append({
                "sequence": ins.get("sequence") or idx,
                "focal": bool(ins.get("focal")),
                "coverage": {"reference": f"Coverage/{ins.get('coverage_identifier') or patient_identifier}"}
            })
        if not insurance:
            insurance = [{"sequence": 1, "focal": True, "coverage": {"reference": f"Coverage/{patient_identifier}"}}]

        related = []
        for rel in related_list:
            entry = {
                "relationship": {"coding": [{"code": rel.get("relationship")}]} if rel.get("relationship") else None,
                "reference": {"value": rel.get("claim_identifier")} if rel.get("claim_identifier") else None
            }
            related.append({k: v for k, v in entry.items() if v is not None})

        claim_resource = {
            "resourceType": "Claim",
            "id": claim_id,
            "meta": {
                "profile": ["http://dhamani.om/fhir/om/dhamani-fs/StructureDefinition/claim|1.0.0"]
            },
            "identifier": [{
                "system": claim.get("identifier_system"),
                "value": claim.get("identifier")
            }] if claim.get("identifier") else [],
            "status": claim.get("status", "active"),
            "type": {"coding": [{"code": claim.get("type")}]} if claim.get("type") else None,
            "subType": {"coding": [{"code": claim.get("sub_type")}]} if claim.get("sub_type") else None,
            "use": claim.get("use", "preauthorization"),
            "patient": {"reference": f"Patient/{patient_identifier}"},
            "created": _format_datetime(claim.get("created")),
            "insurer": {"reference": f"Organization/{insurer_identifier}"},
            "provider": {"reference": f"Organization/{provider_identifier}"},
            "priority": {"coding": [{"code": claim.get("priority")}]} if claim.get("priority") else None,
            "related": related,
            # diagnosis / careTeam intentionally omitted - see docstring
            "supportingInfo": supporting_info,
            "insurance": insurance,
            "item": item,
            "total": {"value": claim.get("total")} if claim.get("total") is not None else None
        }
        if claim.get("billable_start") or claim.get("billable_end"):
            claim_resource["billablePeriod"] = {
                "start": claim.get("billable_start"),
                "end": claim.get("billable_end")
            }

        claim_resource = {k: v for k, v in claim_resource.items() if v not in (None, [], {})}

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
                            "code": message_header_in.get("event_coding", "preauth-request")
                        },
                        "destination": [{
                            "endpoint": f"http://{insurer_identifier}.Dhamani.om/$process-message",
                            "receiver": {
                                "type": "Organization",
                                "identifier": {
                                    "system": "http://dhamani.om/license/payer-license",
                                    "value": message_header_in.get("destination_receiver_identifier") or insurer_identifier
                                }
                            }
                        }],
                        "sender": {
                            "type": "Organization",
                            "identifier": {
                                "system": "http://dhamani.om/license/provider-license",
                                "value": message_header_in.get("sender_identifier") or provider_identifier
                            }
                        },
                        "source": {"endpoint": f"http://{provider_identifier}.Dhamani.om"},
                        "focus": [{
                            "reference": message_header_in.get("focus")
                            or f"http://{provider_identifier}.Dhamani.om/Claim/{claim_id}"
                        }]
                    }
                },
                {
                    "fullUrl": f"http://{provider_identifier}.Dhamani.om/Claim/{claim_id}",
                    "resource": claim_resource
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
                                    "code": claim.get("patient_identifier_type", "NI")
                                }]
                            },
                            "system": claim.get("patient_identifier_system"),
                            "value": patient_identifier
                        }],
                        "name": [{"text": claim.get("patient_name")}] if claim.get("patient_name") else [],
                        "gender": claim.get("patient_gender"),
                        "birthDate": claim.get("patient_birth_date"),
                        "telecom": [{"value": claim.get("patient_telecom")}] if claim.get("patient_telecom") else []
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
                        "name": claim.get("provider_name")
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
                        "name": claim.get("insurer_name")
                    }
                }
            ]
        }

        for ins in insurance_list:
            coverage_id = ins.get("coverage_identifier")
            if not coverage_id:
                continue
            fhir_bundle["entry"].append({
                "fullUrl": f"http://{provider_identifier}.Dhamani.om/Coverage/{coverage_id}",
                "resource": {
                    "resourceType": "Coverage",
                    "id": coverage_id,
                    "meta": {
                        "profile": ["http://dhamani.om/fhir/om/dhamani-fs/StructureDefinition/coverage|1.0.0"]
                    },
                    "status": "active",
                    "beneficiary": {"reference": f"Patient/{patient_identifier}"},
                    "payor": [{"reference": f"Organization/{insurer_identifier}"}]
                }
            })

        logger.info(f"✅ Transformed PreAuth JSON to FHIR Claim Bundle: {bundle_id}")
        return fhir_bundle

    except Exception as e:
        logger.error(f"❌ Error transforming PreAuth JSON to FHIR Claim: {e}")
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
