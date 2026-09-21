"""
Tables for the eligibility endpoints, and the mapping of their JSON into rows:
  eligibility_request         one row per request JSON (POST /api/v1/eligibility/requests)
  eligibility_response        one row per response JSON (the mapped CoverageEligibilityResponse)
  eligibility_response_error  the response's error list, one row per error

Columns are the JSON's own keys in snake_case (coveragePolicyHolderIdentifier ->
coverage_policy_holder_identifier), so a row reads like the JSON it came from.
The message header keys carry a header_ prefix, and lists (insurances,
costToBeneficiaries, item) are stored as JSON text. Every row also carries the
correlation_id of the request it belongs to, which is how a run's rows are found.

Same rules as the PreAuth tables: created if missing, columns only ever added.
"""
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    MetaData, Table, Column, String, DateTime, Boolean, Text, Integer, ForeignKey, delete, insert,
)

from .preauth_mapper import _dt
from .preauth_tables import ensure_schema, coerce_value

metadata = MetaData()


def _cols(specs: List[tuple]) -> List[Column]:
    return [Column(name, String(length), nullable=True) for name, length in specs]


def _dates(*names: str) -> List[Column]:
    return [Column(n, DateTime, nullable=True) for n in names]


eligibility_request = Table(
    "eligibility_request", metadata,
    Column("id", String(200), primary_key=True),
    Column("correlation_id", String(36), nullable=True, index=True),
    *_cols([
        ("resource_type", 100), ("identifier", 200), ("identifier_system", 255), ("status", 50), ("priority", 50),
        ("purpose", 200), ("patient_identifier", 100), ("patient_identifier_type", 50),
        ("patient_identifier_system", 255),
    ]),
    *_dates("serviced_date", "created"),
    *_cols([
        ("insurer_identifier", 100), ("insurer_identifier_type", 50), ("insurer_name", 255),
        ("provider_identifier", 100), ("provider_identifier_type", 50), ("provider_name", 255),
    ]),
    Column("insurances", Text, nullable=True),
    *_cols([
        ("message_header_id", 200), ("event_coding", 100), ("destination_receiver_identifier", 200),
        ("sender_identifier", 200), ("focus", 500),
    ]),
    *_dates("inserted_on"),
)

_RESPONSE_STRINGS = [
    ("request_identifier", 200), ("request_identifier_system", 255), ("resource_type", 100),
    ("extension_not_in_force_reason", 100), ("identifier", 200), ("status", 50), ("purpose", 200),
    ("patient_identifier", 100), ("patient_identifier_system", 255), ("patient_identifier_type", 50),
    ("outcome", 50), ("disposition", 500), ("insurer_identifier", 100),
    ("insurance_extension_not_in_force_reason", 100), ("coverage_id", 200), ("coverage_identifier", 200),
    ("coverage_type", 100), ("coverage_subscriber_identifier", 200), ("coverage_subscriber_identifier_type", 100),
    ("coverage_subscriber_identifier_system", 255), ("coverage_subscriber_id", 200),
    ("coverage_beneficiary_identifier", 100), ("coverage_beneficiary_identifier_type", 50),
    ("coverage_beneficiary_identifier_system", 255), ("coverage_policy_holder_identifier", 200),
    ("coverage_policy_holder_identifier_type", 100), ("coverage_policy_holder_identifier_system", 255),
    ("coverage_policy_holder_identifier_type_system", 255), ("coverage_dependent", 100),
    ("coverage_relationship", 100), ("coverage_payor_identifier", 100), ("coverage_payor_identifier_type", 50),
    ("coverage_subrogation", 100), ("coverage_class_type", 100), ("coverage_class_value", 200),
    ("coverage_class_name", 255), ("coverage_network", 100), ("insurer_name", 255), ("provider_name", 255),
    ("header_id", 200), ("header_event_coding", 100), ("header_destination_receiver_identifier", 200),
    ("header_sender_identifier", 200), ("header_focus", 500), ("header_response_identifier", 200),
    ("header_response_code", 50),
]
_RESPONSE_DATES = [
    "serviced_date", "serviced_period_start", "serviced_period_end", "created", "coverage_period_start",
    "coverage_period_end", "insurance_benefit_period_start", "insurance_benefit_period_end", "inserted_on",
]

eligibility_response = Table(
    "eligibility_response", metadata,
    Column("id", String(200), primary_key=True),
    Column("correlation_id", String(36), nullable=True, index=True),
    *_cols(_RESPONSE_STRINGS),
    *_dates(*_RESPONSE_DATES),
    Column("insurance_inforce", Boolean, nullable=True),
    Column("cost_to_beneficiaries", Text, nullable=True),
    Column("item", Text, nullable=True),
)

eligibility_response_error = Table(
    "eligibility_response_error", metadata,
    Column("id", String(200), primary_key=True),
    Column("response_id", String(200), ForeignKey("eligibility_response.id"), nullable=False, index=True),
    Column("sequence", Integer, nullable=True),
    Column("error_code", String(100), nullable=True),
    Column("error_extension_expression", String(500), nullable=True),
)


def ensure_eligibility_schema(engine) -> None:
    """Create any missing eligibility table and add any missing column to existing ones. Additive only."""
    ensure_schema(engine, metadata)


def _snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def _clean(value: Any) -> Any:
    if isinstance(value, (list, dict)):
        return json.dumps(value) if value else None
    return None if value == "" else value


def _row(table: Table, values: Dict[str, Any]) -> Dict[str, Any]:
    """Only the table's own columns; dates in the DATETIME wall-clock form"""
    row = {}
    for name, value in values.items():
        if name not in table.c:
            continue
        value = _clean(value)
        if isinstance(table.c[name].type, DateTime) and isinstance(value, str):
            value = _dt(value)
        row[name] = value
    return row


def to_eligibility_request_rows(payload: Dict[str, Any], correlation_id: str) -> Dict[str, List[Dict[str, Any]]]:
    """The flat eligibility request JSON -> {"eligibility_request": [row]}"""
    values = {_snake(k): v for k, v in payload.items() if k not in ("messageHeader", "purpose")}
    header = payload.get("messageHeader") or {}
    purpose = payload.get("purpose")
    values.update(
        id=payload.get("id") or payload.get("identifier") or correlation_id,
        correlation_id=correlation_id,
        purpose=", ".join(purpose) if isinstance(purpose, list) else purpose,
        message_header_id=header.get("id"),
        event_coding=header.get("eventCoding"),
        destination_receiver_identifier=header.get("destinationReceiverIdentifier"),
        sender_identifier=header.get("senderIdentifier"),
        focus=header.get("focus"),
        inserted_on=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    )
    return {"eligibility_request": [_row(eligibility_request, values)]}


def to_eligibility_response_rows(payload: Dict[str, Any], correlation_id: str) -> Dict[str, List[Dict[str, Any]]]:
    """The flat eligibility response JSON -> {"eligibility_response": [row], "eligibility_response_error": [rows]}"""
    values = {_snake(k): v for k, v in payload.items() if k not in ("messageHeader", "error")}
    header = payload.get("messageHeader") or {}
    response_id = payload.get("id") or correlation_id
    values.update(
        id=response_id,
        correlation_id=correlation_id,
        inserted_on=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        **{f"header_{_snake(k)}": v for k, v in header.items()},
    )
    errors = [
        _row(eligibility_response_error, {
            "id": f"{response_id}-ERR-{n}", "response_id": response_id, "sequence": n,
            "error_code": e.get("errorCode"), "error_extension_expression": e.get("errorExtensionExpression"),
        })
        for n, e in enumerate(payload.get("error") or [], start=1)
    ]
    return {"eligibility_response": [_row(eligibility_response, values)], "eligibility_response_error": errors}


def _store(engine, tables: Dict[str, List[Dict[str, Any]]], order: List[str], clear) -> Dict[str, int]:
    with engine.begin() as conn:
        clear(conn)
        written: Dict[str, int] = {}
        for name in order:
            table = metadata.tables[name]
            rows = [{c.name: coerce_value(c, row.get(c.name)) for c in table.columns} for row in tables.get(name) or []]
            if rows:
                conn.execute(insert(table), rows)
            written[name] = len(rows)
    return written


def store_eligibility_request(engine, tables: Dict[str, List[Dict[str, Any]]]) -> Dict[str, int]:
    """Save the request row, replacing any earlier row with the same id"""
    ids = [r["id"] for r in tables["eligibility_request"]]
    return _store(engine, tables, ["eligibility_request"],
                  lambda conn: conn.execute(delete(eligibility_request).where(eligibility_request.c.id.in_(ids))))


def store_eligibility_response(engine, tables: Dict[str, List[Dict[str, Any]]]) -> Dict[str, int]:
    """Save the response and its errors, replacing any earlier response with the same id"""
    ids = [r["id"] for r in tables["eligibility_response"]]

    def clear(conn):
        conn.execute(delete(eligibility_response_error).where(eligibility_response_error.c.response_id.in_(ids)))
        conn.execute(delete(eligibility_response).where(eligibility_response.c.id.in_(ids)))
    return _store(engine, tables, ["eligibility_response", "eligibility_response_error"], clear)
