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
            {"key": "url", "label": "URL", "type": "string",
             "placeholder": "http://host/path - or .../$input.body.correlation_id"},
            {"key": "delaySeconds", "label": "Wait before calling (seconds)", "type": "string",
             "placeholder": "0 - set ~3 for a GET that reads an async result", "default": "0"},
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
             "placeholder": "usp_get_preauth_claim_source_data_by_claim_id"},
            {"key": "params", "label": "Parameters", "type": "keyvalue",
             "placeholder": "value or $input.claim_id", "default": {}}
        ]
    },
    {
        "type": "json_to_fhir",
        "label": "JSON → FHIR",
        "category": "action",
        "description": "Convert flat JSON into a FHIR Bundle - always the request direction: something we're sending out to Dhamani, never a response we're constructing",
        "config_schema": [
            {"key": "function", "label": "Function", "type": "select", "options": [
                "to_fhir_eligibility_bundle", "json_to_fhir_claim"
            ]},
            {"key": "claimId", "label": "Claim ID (for Excel download)", "type": "string",
             "placeholder": "e.g. c_demo_1 or $input.claim_id",
             "showWhen": {"function": "json_to_fhir_claim"}}
        ]
    },
    {
        "type": "fhir_to_json",
        "label": "FHIR → JSON",
        "category": "action",
        "description": "Convert a FHIR Bundle into flat JSON (or validate it) - always the response direction: a request/claim/preauth response coming back from Dhamani, never an inbound request",
        "config_schema": [
            {"key": "function", "label": "Function", "type": "select", "options": [
                "to_eligibility_response_json", "to_preauth_tables", "validate_fhir_patient",
                "fhir_to_json_claim_response"
            ]},
            {"key": "claimId", "label": "Claim ID (for Excel download)", "type": "string",
             "placeholder": "e.g. c_demo_1 or $input.claim_id",
             "showWhen": {"function": "fhir_to_json_claim_response"}}
        ]
    },
    {
        "type": "view",
        "label": "View / Passthrough",
        "category": "utility",
        "description": "No-op node - just records input/output at this point in the graph",
        "config_schema": []
    },
    {
        "type": "excel_export",
        "label": "Excel Export",
        "category": "utility",
        "description": "No-op pass-through, like View - but after a Run, its Download button exports whatever actually flowed through the node right before it in THIS run: that upstream node's own input and output (e.g. wired right after a JSON -> FHIR node, it downloads the JSON that came in and the FHIR that came out; after FHIR -> JSON, vice versa). No claim ID or DB lookup needed - it reads straight from this run's own recorded step data.",
        "config_schema": []
    }
]

NODE_TYPES_BY_KEY: Dict[str, Dict[str, Any]] = {nt["type"]: nt for nt in NODE_TYPES}
