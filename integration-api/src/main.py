"""
Integration API - FastAPI service
Rebuilt per implementation.md section 6.2. Responsibilities: validate, log,
audit, publish. Returns 202 Accepted with a correlation_id; does not wait
for the FHIR round-trip. Only two routes exist now - the plan's endpoint
surface for this service, nothing more (no PreAuth, no eligibility-specific
schema, no Dhamani-facing dummy endpoint - those belonged to the pre-rebuild
version of this service).
"""
import logging
import json
from contextlib import asynccontextmanager
from datetime import date
from typing import Literal
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy.orm import Session
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from shared import (
    get_db, create_kafka_producer, send_kafka_message, TOPICS,
    Base, engine, Envelope, AuditLog, MessageTracking
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    Base.metadata.create_all(bind=engine)
    logger.info("✅ Database tables initialized")
except Exception as e:
    logger.error(f"❌ Error initializing database: {e}")

producer = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = await create_kafka_producer()
    logger.info("🚀 Integration API started")
    yield
    if producer:
        await producer.stop()
    logger.info("🛑 Integration API stopped")


app = FastAPI(title="Integration API", version="1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class PatientIn(BaseModel):
    patientId: int
    name: str
    gender: Literal["Male", "Female", "Other", "Unknown"]
    dob: date


@app.get("/health", tags=["Health"])
async def health():
    return {"status": "ok", "service": "Integration API", "version": "1.0"}


@app.post("/api/v1/patients", status_code=202, tags=["patients"])
async def submit_patient(body: PatientIn, db: Session = Depends(get_db)):
    """
    Flow: JSON -> Kafka (json.request) -> json-fhir-service
    Returns 202 immediately with a correlation_id - does not wait for the
    FHIR round-trip. Poll GET /api/v1/patients/status/{correlation_id}.
    """
    try:
        correlation_id = str(uuid4())
        payload = body.model_dump(mode="json")

        envelope = Envelope(
            correlation_id=correlation_id,
            source="integration-api",
            event_type="patient.request",
            payload=payload,
        )

        db.add(AuditLog(
            correlation_id=correlation_id, service="integration-api",
            action="patient.request.received", payload=payload
        ))
        db.add(MessageTracking(
            correlation_id=correlation_id, patient_id=str(body.patientId),
            status="RECEIVED"
        ))
        db.commit()

        logger.info(f"📝 Received patient request: {correlation_id}")
        logger.info(f"📦 Payload:\n{json.dumps(payload, indent=2, default=str)}")

        await send_kafka_message(
            producer, TOPICS["json_request"], str(body.patientId),
            envelope.model_dump(mode="json")
        )

        tracking = db.query(MessageTracking).filter(
            MessageTracking.correlation_id == correlation_id
        ).first()
        tracking.status = "SENT"
        db.commit()

        logger.info(f"✅ Published to json.request: {correlation_id}")

        return {"correlation_id": correlation_id, "status": "RECEIVED"}

    except Exception as e:
        logger.error(f"❌ Error submitting patient: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to submit patient: {str(e)}")


@app.get("/api/v1/patients/status/{correlation_id}", tags=["patients"])
async def get_status(correlation_id: str, db: Session = Depends(get_db)):
    """Read message_tracking for the given correlation_id"""
    tracking = db.query(MessageTracking).filter(
        MessageTracking.correlation_id == correlation_id
    ).first()
    if not tracking:
        raise HTTPException(status_code=404, detail="correlation_id not found")
    return {
        "correlation_id": tracking.correlation_id,
        "patient_id": tracking.patient_id,
        "status": tracking.status,
        "fhir_resource_id": tracking.fhir_resource_id,
        "last_error": tracking.last_error,
        "created_at": tracking.created_at,
        "updated_at": tracking.updated_at,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
