"""
Eligibility mapping, both directions:
  to_fhir_eligibility_bundle    - flattened CoverageEligibilityRequest JSON -> Dhamani FHIR message Bundle
  to_eligibility_response_json  - Dhamani CoverageEligibilityResponse Bundle -> flattened response JSON
The request side is kept separate from fhir_utils.json_to_fhir_patient on purpose: this one emits
the full-URL references, per-resource UUIDs and message-header wiring of the
Dhamani eligibility-request example bundle.
"""
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from .fhir_utils import fhir_to_json_response

DHAMANI = "http://dhamani.om/fhir/om/dhamani-fs/StructureDefinition"
OMAN_TZ = timezone(timedelta(hours=4))  # Oman has no DST

PRIORITY_DISPLAY = {"stat": "Immediate", "normal": "Normal", "deferred": "Deferred"}
IDENTIFIER_TYPE_DISPLAY = {"NI": "National unique individual identifier"}


def _fhir_datetime(value: Optional[str]) -> Optional[str]:
    """'2026-08-31 00:00:00' -> '2026-08-31T00:00:00+04:00' (naive values are taken as Oman local time)"""
    if not value:
        return value
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=OMAN_TZ)
    return parsed.isoformat(timespec="milliseconds") if parsed.microsecond else parsed.isoformat()


def _org_id(kind: str, identifier: str) -> str:
    """Stable UUID per organization so a retried request yields the same bundle references"""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{kind}:{identifier}"))


def to_fhir_eligibility_bundle(src: Dict[str, Any]) -> Dict[str, Any]:
    """Map an eligibility request JSON (see shared.schemas.PatientRequest) to a FHIR message Bundle"""
    patient_ident = src["patientIdentifier"]
    provider_ident = src["providerIdentifier"]
    insurer_ident = src["insurerIdentifier"]
    provider_name = src.get("providerName")
    insurer_name = src.get("insurerName")
    header_in = src.get("messageHeader") or {}

    request_id = src.get("id") or src.get("identifier") or str(uuid.uuid4())
    header_id = header_in.get("id") or str(uuid.uuid4())
    provider_org_id = _org_id("provider", provider_ident)
    insurer_org_id = _org_id("insurer", insurer_ident)

    base = f"http://{provider_ident}.Dhamani.om"
    request_url = f"{base}/CoverageEligibilityRequest/{request_id}"
    patient_url = f"{base}/Patient/{patient_ident}"
    coverage_url = f"{base}/Coverage/{patient_ident}"
    provider_url = f"{base}/Organization/{provider_org_id}"
    insurer_url = f"{base}/Organization/{insurer_org_id}"

    purpose = src.get("purpose") or ["discovery"]
    if isinstance(purpose, str):  # the API's schema normalizes this; a raw workflow seed may not
        purpose = [purpose]

    priority = src.get("priority") or "normal"
    priority_coding = {"system": "http://terminology.hl7.org/CodeSystem/processpriority", "code": priority}
    if priority in PRIORITY_DISPLAY:
        priority_coding["display"] = PRIORITY_DISPLAY[priority]

    id_type = src.get("patientIdentifierType") or "NI"
    id_type_coding = {"system": "http://terminology.hl7.org/CodeSystem/v2-0203", "code": id_type}
    if id_type in IDENTIFIER_TYPE_DISPLAY:
        id_type_coding["display"] = IDENTIFIER_TYPE_DISPLAY[id_type]

    def organization(org_id: str, profile: str, license_kind: str, type_code: str, ident: str, name: Optional[str]):
        org = {
            "resourceType": "Organization",
            "id": org_id,
            "meta": {"profile": [f"{DHAMANI}/{profile}|1.0.0"]},
            "identifier": [{"system": f"http://dhamani.om/license/{license_kind}", "value": ident}],
            "active": True,
            "type": [{"coding": [{
                "system": "http://dhamani.om/terminology/CodeSystem/organization-type",
                "code": type_code,
            }]}],
        }
        if name:
            org["name"] = name
        return org

    def entry(url: str, resource: Dict[str, Any]) -> Dict[str, Any]:
        return {"fullUrl": url, "resource": resource}

    eligibility_request = {
        "resourceType": "CoverageEligibilityRequest",
        "id": request_id,
        "meta": {"profile": [f"{DHAMANI}/eligibility-request|1.0.0"]},
        "identifier": [{
            "system": src.get("identifierSystem"),
            "value": src.get("identifier") or request_id,
        }],
        "status": src.get("status") or "active",
        "priority": {"coding": [priority_coding]},
        "purpose": purpose,
        "patient": {"reference": patient_url},
        "servicedDate": src.get("servicedDate"),
        "created": _fhir_datetime(src.get("created")),
        "provider": {"reference": provider_url},
        "insurer": {"reference": insurer_url},
        "insurance": src.get("insurances") or [{"focal": True, "coverage": {"reference": coverage_url}}],
    }

    message_header = {
        "resourceType": "MessageHeader",
        "id": header_id,
        "meta": {"profile": [f"{DHAMANI}/message-header|1.0.0"]},
        "eventCoding": {
            "system": "http://dhamani.om/terminology/CodeSystem/om-message-events",
            "code": header_in.get("eventCoding") or "eligibility-request",
        },
        "destination": [{
            "endpoint": f"http://{insurer_ident}.Dhamani.om/$process-message",
            "receiver": {
                "type": "Organization",
                "identifier": {
                    "system": "http://dhamani.om/license/payer-license",
                    "value": header_in.get("destinationReceiverIdentifier") or insurer_ident,
                },
                "display": insurer_name,
            },
        }],
        "sender": {
            "type": "Organization",
            "identifier": {
                "system": "http://dhamani.om/license/provider-license",
                "value": header_in.get("senderIdentifier") or provider_ident,
            },
            "display": provider_name,
        },
        "source": {"endpoint": base},
        # Always the bundle's own entry - messageHeader.focus in the JSON is a
        # MOH-side URL that would not resolve inside the bundle.
        "focus": [{"reference": request_url}],
    }

    patient = {
        "resourceType": "Patient",
        "id": patient_ident,
        "meta": {"profile": [f"{DHAMANI}/patient|1.0.0"]},
        "identifier": [{
            "type": {"coding": [id_type_coding]},
            "system": src.get("patientIdentifierSystem"),
            "value": patient_ident,
        }],
        "active": True,
    }

    coverage = {
        "resourceType": "Coverage",
        "id": patient_ident,
        "meta": {"profile": [f"{DHAMANI}/coverage|1.0.0"]},
        "status": "active",
        "beneficiary": {"reference": patient_url},
        "relationship": {"coding": [{
            "system": "http://terminology.hl7.org/CodeSystem/subscriber-relationship",
            "code": "self",
            "display": "Self",
        }]},
        "payor": [{"reference": insurer_url}],
    }

    return {
        "resourceType": "Bundle",
        "id": str(uuid.uuid4()),
        "meta": {"profile": [f"{DHAMANI}/bundle|1.0.0"]},
        "type": "message",
        "timestamp": datetime.now(OMAN_TZ).isoformat(timespec="milliseconds"),
        "entry": [
            entry(f"{base}/MessageHeader/{header_id}", message_header),
            entry(request_url, eligibility_request),
            entry(provider_url, organization(
                provider_org_id, "provider-organization", "provider-license", "prov", provider_ident, provider_name)),
            entry(patient_url, patient),
            entry(coverage_url, coverage),
            entry(insurer_url, organization(
                insurer_org_id, "insurer-organization", "payer-license", "ins", insurer_ident, insurer_name)),
        ],
    }


def to_eligibility_response_json(bundle: Dict[str, Any]) -> Dict[str, Any]:
    """
    Dhamani CoverageEligibilityResponse message Bundle -> flattened response JSON.
    Reuses fhir_utils.fhir_to_json_response and drops its sync_status /
    completed_at bookkeeping keys, which aren't part of the response JSON.
    Fields the Bundle doesn't carry (e.g. coverage details when there is no
    Coverage entry) come out null.
    """
    out = fhir_to_json_response(bundle, None)
    out.pop("sync_status", None)
    out.pop("completed_at", None)
    return out
