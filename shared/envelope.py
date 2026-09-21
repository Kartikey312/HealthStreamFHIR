"""
Event envelope - the single message contract every Kafka message in the
rebuilt pipeline uses, per implementation.md section 4, so tracing, retries
and audit work uniformly across integration-api, json-fhir-service,
communication-service, fhir-json-service and processing-service.
"""
from datetime import datetime, timezone
from typing import Any, Literal, Optional
from uuid import uuid4
from pydantic import BaseModel, Field


class Envelope(BaseModel):
    message_id: str = Field(default_factory=lambda: str(uuid4()))
    correlation_id: str
    source: str
    event_type: Literal[
        "patient.request", "patient.fhir.outgoing",
        "patient.fhir.incoming", "patient.response",
        "eligibility.request", "eligibility.fhir.outgoing",
        "eligibility.fhir.incoming", "eligibility.response",
        "preauth.claim.fhir.incoming", "preauth.claim.response",
    ]
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    attempt: int = 0
    payload: dict[str, Any]
    error: Optional[dict[str, Any]] = None
