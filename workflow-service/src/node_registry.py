"""
Static registry of node types - the single source of truth the palette and
config panel render from, so field definitions live here once instead of
drifting between backend and frontend.
"""
from typing import Dict, Any, List

NODE_TYPES: List[Dict[str, Any]] = [
    {
        "type": "manual_trigger",
        "label": "Manual Trigger",
        "category": "trigger",
        "description": "Holds/edits the JSON that seeds a run",
        "config_schema": [
            {"key": "data", "label": "Seed JSON", "type": "json", "default": {}}
        ]
    },
    {
        "type": "http_request",
        "label": "HTTP Request",
        "category": "action",
        "description": "Call any HTTP endpoint (method, headers, body)",
        "config_schema": [
            {"key": "method", "label": "Method", "type": "select",
             "options": ["GET", "POST", "PUT", "PATCH", "DELETE"], "default": "GET"},
            {"key": "url", "label": "URL", "type": "string"},
            {"key": "headers", "label": "Headers", "type": "keyvalue", "default": {}},
            {"key": "bodyMode", "label": "Body", "type": "select",
             "options": ["passthrough", "custom"], "default": "passthrough"},
            {"key": "body", "label": "Custom body (JSON)", "type": "json", "default": {}}
        ]
    },
    {
        "type": "kafka_publish",
        "label": "Kafka Publish",
        "category": "action",
        "description": "Publish the current data to a Kafka topic",
        "config_schema": [
            {"key": "topic", "label": "Topic", "type": "select", "optionsFrom": "/topics"},
            {"key": "key", "label": "Message key", "type": "string",
             "placeholder": "static value or $input.transaction_id"}
        ]
    },
    {
        "type": "db_query",
        "label": "DB Stored Procedure",
        "category": "action",
        "description": "Call a stored procedure with bound parameters (no free-form SQL)",
        "config_schema": [
            {"key": "procedure", "label": "Procedure name", "type": "string",
             "placeholder": "usp_get_preauth_claims_details_by_claim_id"},
            {"key": "params", "label": "Parameters", "type": "keyvalue",
             "placeholder": "value or $input.claim_id", "default": {}}
        ]
    },
    {
        "type": "json_to_fhir",
        "label": "JSON → FHIR",
        "category": "action",
        "description": "Convert flat JSON into a FHIR Bundle",
        "config_schema": [
            {"key": "function", "label": "Function", "type": "select", "options": [
                "json_to_fhir_patient", "json_to_fhir_response", "json_to_fhir_claim"
            ]}
        ]
    },
    {
        "type": "fhir_to_json",
        "label": "FHIR → JSON",
        "category": "action",
        "description": "Convert a FHIR Bundle into flat JSON (or validate it)",
        "config_schema": [
            {"key": "function", "label": "Function", "type": "select", "options": [
                "fhir_to_json_response", "fhir_to_json_request", "validate_fhir_patient"
            ]},
            {"key": "originalPatientIdField", "label": "Original patient id field", "type": "string",
             "default": "patientIdentifier", "showWhen": {"function": "fhir_to_json_response"}}
        ]
    },
    {
        "type": "view",
        "label": "View / Passthrough",
        "category": "utility",
        "description": "No-op node - just records input/output at this point in the graph",
        "config_schema": []
    }
]

NODE_TYPES_BY_KEY: Dict[str, Dict[str, Any]] = {nt["type"]: nt for nt in NODE_TYPES}
