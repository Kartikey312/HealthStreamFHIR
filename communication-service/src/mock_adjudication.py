"""
STUB adjudication for the PreAuth flow.

No real Dhamani/payer adjudicator is connected to this system - there is no
real approve/deny/pend logic anywhere here. This module exists only to keep
the PreAuth Kafka pipeline exercised end-to-end (request -> response ->
logged), so the audit-logging flow has something real to observe on the
response side. Replace before any real use.

Kept deliberately separate from shared/fhir_utils.py: everything in that
module is a genuine best-effort transformation of real input; this is a
placeholder standing in for a whole missing system, not a mapping of
anything, and shouldn't be mistaken for a reusable conversion or exposed as
a workflow-canvas node option.
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
import uuid


def _find_resource(bundle: Dict[str, Any], resource_type: str) -> Dict[str, Any]:
    for entry in bundle.get("entry", []) or []:
        resource = entry.get("resource", {})
        if resource.get("resourceType") == resource_type:
            return resource
    return {}


def _first(items: Optional[List[Any]]) -> Dict[str, Any]:
    return items[0] if items else {}


def build_mock_claim_response(fhir_claim_bundle: Dict[str, Any], correlation_id: str) -> Dict[str, Any]:
    """
    STUB - approves every item on the incoming Claim at its requested net
    amount, unconditionally. No real adjudication rules, no denial/pend
    paths, no real payer connection. The output is tagged with an explicit
    extension-mock-response marker so it's unambiguous downstream (and in
    the audit log) that any given response is synthetic.
    """
    message_header_in = _find_resource(fhir_claim_bundle, "MessageHeader")
    claim = _find_resource(fhir_claim_bundle, "Claim")

    claim_id = claim.get("id") or str(uuid.uuid4())
    patient_ref = claim.get("patient", {}).get("reference", "")
    insurer_ref = claim.get("insurer", {}).get("reference", "")
    provider_ref = claim.get("provider", {}).get("reference", "")

    insurer_identifier = insurer_ref.split("/")[-1] if insurer_ref else None
    provider_identifier = provider_ref.split("/")[-1] if provider_ref else None

    bundle_id = str(uuid.uuid4())
    message_header_id = str(uuid.uuid4())

    items = claim.get("item", []) or []
    total_net = sum((item.get("net", {}) or {}).get("value") or 0 for item in items)

    adjudicated_items = []
    for item in items:
        net_value = (item.get("net", {}) or {}).get("value")
        adjudicated_items.append({
            "sequence": item.get("sequence"),
            "productOrService": item.get("productOrService"),
            "adjudication": [
                {
                    "category": {"coding": [{"code": "approved"}]},
                    "amount": {"value": net_value}
                }
            ]
        })

    claim_response_resource = {
        "resourceType": "ClaimResponse",
        "id": claim_id,
        "meta": {
            "profile": ["http://dhamani.om/fhir/om/dhamani-fs/StructureDefinition/claim-response|1.0.0"]
        },
        "extension": [{
            "url": "http://dhamani.om/fhir/om/dhamani-fs/StructureDefinition/extension-mock-response",
            "valueBoolean": True
        }],
        "identifier": [{"value": str(uuid.uuid4())}],
        "status": claim.get("status", "active"),
        "type": claim.get("type"),
        "use": claim.get("use", "preauthorization"),
        "patient": {"reference": patient_ref} if patient_ref else None,
        "created": datetime.utcnow().isoformat(),
        "insurer": {"reference": insurer_ref} if insurer_ref else None,
        "requestor": {"reference": provider_ref} if provider_ref else None,
        "request": {"reference": f"Claim/{claim_id}"},
        "outcome": "complete",
        "disposition": "Mock approval - stub adjudication, no real payer logic",
        "preAuthRef": f"PA-{uuid.uuid4().hex[:12].upper()}",
        "item": adjudicated_items,
        "total": [
            {"category": {"coding": [{"code": "submitted"}]}, "amount": {"value": total_net}},
            {"category": {"coding": [{"code": "benefit"}]}, "amount": {"value": total_net}}
        ]
    }
    claim_response_resource = {k: v for k, v in claim_response_resource.items() if v is not None}

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
                        "code": "preauth-response"
                    },
                    "sender": {
                        "type": "Organization",
                        "identifier": {
                            "system": "http://dhamani.om/license/payer-license",
                            "value": insurer_identifier
                        }
                    },
                    "destination": [{
                        "endpoint": f"http://{provider_identifier}.Dhamani.om/$process-message",
                        "receiver": {
                            "type": "Organization",
                            "identifier": {
                                "system": "http://dhamani.om/license/provider-license",
                                "value": provider_identifier
                            }
                        }
                    }],
                    "source": {"endpoint": f"http://{insurer_identifier}.Dhamani.om"},
                    "response": {
                        "identifier": correlation_id,
                        "code": "ok"
                    },
                    "focus": [{"reference": f"ClaimResponse/{claim_id}"}]
                }
            },
            {
                "fullUrl": f"http://{insurer_identifier}.Dhamani.om/ClaimResponse/{claim_id}",
                "resource": claim_response_resource
            }
        ]
    }

    return fhir_bundle
