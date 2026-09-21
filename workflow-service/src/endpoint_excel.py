"""
Works out which endpoint's stored tables an Excel Export node should show
(shared/table_export.PROFILES), and which record of that endpoint this run
created - from the graph and the run's own recorded steps.
"""
import json
from typing import Any, Dict, List, Optional, Tuple

# (path, profile): a POST to it stores rows in that profile's tables
URL_PROFILES: List[Tuple[str, str]] = [
    ("/api/v1/preauth/responses", "preauth-claim"),
    ("/api/v1/preauth/requests", "preauth-claim"),
    ("/api/v1/eligibility/responses", "eligibility-response"),
    ("/api/v1/eligibility/requests", "eligibility-request"),
    ("/api/v1/patients", "patient"),
]
# the in-line converters mirror what the endpoint's worker does with the same payload
FUNCTION_PROFILES: Dict[str, str] = {
    "to_preauth_tables": "preauth-claim",
    "to_fhir_preauth_bundle": "preauth-claim",
    "to_fhir_eligibility_bundle": "eligibility-request",
    "to_eligibility_response_json": "eligibility-response",
}


def node_profile(node: Dict[str, Any]) -> Optional[str]:
    config = node.get("config") or {}
    if node.get("type") == "http_request" and (config.get("method") or "GET").upper() == "POST":
        url = config.get("url") or ""
        return next((profile for path, profile in URL_PROFILES if path in url), None)
    if node.get("type") in ("json_to_fhir", "fhir_to_json"):
        return FUNCTION_PROFILES.get(config.get("function"))
    return None


def detect_profile(node_id: str, node_by_id: Dict[str, Any], incoming: Dict[str, List[str]]) -> Optional[str]:
    """Nearest upstream node that stores to (or mirrors) an endpoint, walking back from node_id"""
    visited, frontier = set(), list(incoming.get(node_id, []))
    while frontier:
        nid = frontier.pop(0)
        if nid in visited:
            continue
        visited.add(nid)
        profile = node_profile(node_by_id.get(nid, {}))
        if profile:
            return profile
        frontier.extend(incoming.get(nid, []))
    return None


def _load(raw: Optional[str]) -> Any:
    try:
        return json.loads(raw) if raw else None
    except (TypeError, ValueError):
        return None


def _claim_id_in(payload: Any) -> Optional[str]:
    """Claim id from a Claim Bundle or from to_preauth_tables output"""
    if not isinstance(payload, dict):
        return None
    if isinstance(payload.get("claim"), list) and payload["claim"] and isinstance(payload["claim"][0], dict):
        return payload["claim"][0].get("id")
    for entry in payload.get("entry") or []:
        resource = entry.get("resource") or {}
        if resource.get("resourceType") == "Claim":
            return resource.get("id")
    return None


def find_key(profile: str, node_by_id: Dict[str, Any], steps: List[Any]) -> Optional[str]:
    """
    The record this run stored: claim_id for PreAuth, correlation_id for the
    others. Taken from the endpoint's own reply in this run when there is one
    (the row that actually exists), else - PreAuth only - from the claim in the payload.
    """
    outputs = {s.node_id: _load(s.output) for s in steps}

    for node_id, node in node_by_id.items():
        if node_profile(node) != profile or node.get("type") != "http_request":
            continue
        body = (outputs.get(node_id) or {}).get("body")
        if isinstance(body, dict):
            key = body.get("claim_id") if profile == "preauth-claim" else body.get("correlation_id")
            if key:
                return key

    if profile == "preauth-claim":
        for step in steps:
            key = _claim_id_in(outputs.get(step.node_id)) or _claim_id_in(_load(step.input))
            if key:
                return key
    return None
