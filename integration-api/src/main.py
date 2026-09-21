"""
Integration API - FastAPI service
Rebuilt per implementation.md section 6.2. Responsibilities: validate, log,
audit, publish. Returns 202 Accepted with a correlation_id; does not wait
for the FHIR round-trip. Routes: the plan's patient submit/status pair, plus
POST/GET /api/v1/eligibility/requests (eligibility JSON -> Kafka -> Dhamani FHIR Bundle),
plus the PreAuth claim routes:
  POST /api/v1/preauth/requests        - a PreAuth claim as table-row JSON: saved, then published
                                          for json-fhir-service to turn into a Claim Bundle
  GET  /api/v1/preauth/claims/{id}     - the rows stored for a claim
  GET  /api/v1/preauth/export/excel    - Excel copy of the stored PreAuth claim tables
Eligibility requests are saved to the eligibility_request table when they arrive.
It also runs Kafka consumers that save what fhir-json-service mapped: an incoming
Claim Bundle (preauth.claim.json.response) into the PreAuth tables, and an incoming
eligibility response (eligibility.json.response) into the eligibility_response tables.
No Dhamani-facing dummy endpoint - that belonged to the pre-rebuild version.
"""
import asyncio
import logging
import json
from contextlib import asynccontextmanager
from datetime import date, datetime
from typing import Literal
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Depends, Response, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy.orm import Session
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from shared import (
    get_db, create_kafka_producer, create_kafka_consumer, consume_kafka_messages, send_kafka_message, TOPICS,
    Base, engine, SessionLocal, Envelope, AuditLog, MessageTracking,
    PatientRequest as EligibilityRequestIn,
    ensure_preauth_schema, store_preauth_tables,
    ensure_eligibility_schema, store_eligibility_request, store_eligibility_response,
    to_eligibility_request_rows, to_eligibility_response_rows
)
from shared.preauth_excel import build_preauth_workbook
from shared.table_export import read_endpoint_rows

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    Base.metadata.create_all(bind=engine)
    logger.info("✅ Database tables initialized")
except Exception as e:
    logger.error(f"❌ Error initializing database: {e}")

producer = None


async def handle_preauth_json_response(message):
    """Save one preauth.claim.json.response Envelope (table rows) into the PreAuth tables"""
    try:
        env = Envelope.model_validate(message.value)
        if env.event_type != "preauth.claim.response":
            return
        logger.info(f"📥 [integration-api] PreAuth claim rows received: {env.correlation_id}")

        db = SessionLocal()
        tracking = None
        try:
            tracking = db.query(MessageTracking).filter(
                MessageTracking.correlation_id == env.correlation_id
            ).first()
            written = store_preauth_tables(engine, env.payload)
            db.add(AuditLog(
                correlation_id=env.correlation_id, service="integration-api",
                action="preauth.claim.stored", payload=written
            ))
            if tracking:
                tracking.status = "COMPLETED"
            db.commit()
            logger.info(f"✅ Saved PreAuth claim to tables: {written}")
        except Exception as e:
            db.rollback()
            if tracking:
                tracking.status = "FAILED"
                tracking.last_error = str(e)
                db.commit()
            raise
        finally:
            db.close()

    except Exception as e:
        logger.error(f"❌ [integration-api] Error saving PreAuth claim: {e}", exc_info=True)


async def handle_eligibility_json_response(message):
    """Save one eligibility.json.response Envelope (the response JSON) into the eligibility_response tables"""
    try:
        env = Envelope.model_validate(message.value)
        if env.event_type != "eligibility.response":
            return
        logger.info(f"📥 [integration-api] Eligibility response received: {env.correlation_id}")

        db = SessionLocal()
        tracking = None
        try:
            tracking = db.query(MessageTracking).filter(
                MessageTracking.correlation_id == env.correlation_id
            ).first()
            written = store_eligibility_response(engine, to_eligibility_response_rows(env.payload, env.correlation_id))
            db.add(AuditLog(
                correlation_id=env.correlation_id, service="integration-api",
                action="eligibility.response.stored", payload=written
            ))
            if tracking:
                tracking.status = "COMPLETED"
            db.commit()
            logger.info(f"✅ Saved eligibility response to tables: {written}")
        except Exception as e:
            db.rollback()
            if tracking:
                tracking.status = "FAILED"
                tracking.last_error = str(e)
                db.commit()
            raise
        finally:
            db.close()

    except Exception as e:
        logger.error(f"❌ [integration-api] Error saving eligibility response: {e}", exc_info=True)


async def run_consumer(group_id: str, topic: str, handler):
    consumer = await create_kafka_consumer(group_id, [topic])
    logger.info(f"⚙️ Listening for messages on '{topic}'...")
    await consume_kafka_messages(consumer, handler)


def _log_consumer_exit(task: asyncio.Task):
    if not task.cancelled() and task.exception():
        logger.error(f"❌ Kafka consumer stopped: {task.exception()!r}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = await create_kafka_producer()
    for name, ensure in (("PreAuth", ensure_preauth_schema), ("Eligibility", ensure_eligibility_schema)):
        try:
            ensure(engine)
            logger.info(f"✅ {name} tables ready")
        except Exception as e:
            logger.error(f"❌ Could not prepare {name} tables: {e}", exc_info=True)
    consumer_tasks = [
        asyncio.create_task(run_consumer("integration-api-preauth-group", TOPICS["preauth_claim_json_response"],
                                         handle_preauth_json_response)),
        asyncio.create_task(run_consumer("integration-api-eligibility-group", TOPICS["eligibility_json_response"],
                                         handle_eligibility_json_response)),
    ]
    for task in consumer_tasks:
        task.add_done_callback(_log_consumer_exit)
    logger.info("🚀 Integration API started")
    yield
    for task in consumer_tasks:
        task.cancel()
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


@app.post("/api/v1/eligibility/requests", status_code=202, tags=["eligibility"])
async def submit_eligibility_request(body: EligibilityRequestIn, db: Session = Depends(get_db)):
    """
    Flow: JSON -> saved to eligibility_request -> Kafka (json.request) -> json-fhir-service -> eligibility.fhir.outgoing
    Returns 202 immediately with a correlation_id. The JSON -> FHIR Bundle
    mapping happens in json-fhir-service; read the result with
    GET /api/v1/eligibility/requests/{correlation_id}.
    """
    try:
        correlation_id = str(uuid4())
        payload = body.model_dump(mode="json")

        envelope = Envelope(
            correlation_id=correlation_id,
            source="integration-api",
            event_type="eligibility.request",
            payload=payload,
        )

        written = store_eligibility_request(engine, to_eligibility_request_rows(payload, correlation_id))

        db.add(AuditLog(
            correlation_id=correlation_id, service="integration-api",
            action="eligibility.request.received", payload=payload
        ))
        db.add(AuditLog(
            correlation_id=correlation_id, service="integration-api",
            action="eligibility.request.stored", payload=written
        ))
        db.add(MessageTracking(
            correlation_id=correlation_id, patient_id=body.patientIdentifier,
            status="RECEIVED"
        ))
        db.commit()

        logger.info(f"📝 Received eligibility request: {correlation_id}")
        logger.info(f"📦 Payload:\n{json.dumps(payload, indent=2, default=str)}")

        # Status is left alone after publishing - json-fhir-service may already
        # have moved it to TRANSFORMED by the time send_and_wait returns.
        await send_kafka_message(
            producer, TOPICS["json_request"], body.patientIdentifier,
            envelope.model_dump(mode="json")
        )

        logger.info(f"✅ Published eligibility request to json.request: {correlation_id}")

        return {"correlation_id": correlation_id, "status": "RECEIVED"}

    except Exception as e:
        logger.error(f"❌ Error submitting eligibility request: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to submit eligibility request: {str(e)}")


@app.get("/api/v1/eligibility/requests/{correlation_id}", tags=["eligibility"])
async def get_eligibility_request(correlation_id: str, db: Session = Depends(get_db)):
    """Tracking status, the FHIR Bundle json-fhir-service built, the response JSON fhir-json-service mapped (each null until it exists), and the rows saved for this correlation_id in the eligibility tables"""
    tracking = db.query(MessageTracking).filter(
        MessageTracking.correlation_id == correlation_id
    ).first()

    def latest(action: str):
        row = db.query(AuditLog).filter(
            AuditLog.correlation_id == correlation_id, AuditLog.action == action,
        ).order_by(AuditLog.id.desc()).first()
        return row.payload if row else None

    fhir, response = latest("eligibility.fhir.built"), latest("eligibility.response.built")

    # A response whose request.identifier matched no submitted request gets a
    # fresh correlation_id with audit rows but no message_tracking row.
    if not tracking and response is None:
        raise HTTPException(status_code=404, detail="correlation_id not found")

    tables = {**read_endpoint_rows(engine, "eligibility-request", correlation_id),
              **read_endpoint_rows(engine, "eligibility-response", correlation_id)}
    return {
        "correlation_id": correlation_id,
        "status": tracking.status if tracking else "UNMATCHED_RESPONSE",
        "last_error": tracking.last_error if tracking else None,
        "fhir": fhir,
        "response": response,
        "rows": {name: len(rows) for name, rows in tables.items()},
        "tables": tables,
    }


@app.post("/api/v1/preauth/requests", status_code=202, tags=["preauth"])
async def submit_preauth_request(body: dict = Body(...), db: Session = Depends(get_db)):
    """
    A PreAuth claim to send out, as table-row JSON ({"claim": [...], "claim_request_item": [...], ...} -
    the shape GET /api/v1/preauth/claims/{id} returns). It is saved to the
    PreAuth tables here, then published to json.request as a preauth.claim.request
    event; json-fhir-service turns it into a Dhamani Claim Bundle and publishes
    that to preauth.claim.fhir.outgoing. Returns 202 immediately.
    """
    claim_rows = body.get("claim")
    if not isinstance(claim_rows, list) or not claim_rows or not isinstance(claim_rows[0], dict) or not claim_rows[0].get("id"):
        raise HTTPException(status_code=422, detail="Body needs a 'claim' list whose first row has an 'id'")
    claim = claim_rows[0]
    claim_id = claim["id"]

    try:
        written = store_preauth_tables(engine, body)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    try:
        correlation_id = str(uuid4())
        envelope = Envelope(
            correlation_id=correlation_id, source="integration-api",
            event_type="preauth.claim.request", payload=body,
        )
        db.add(AuditLog(
            correlation_id=correlation_id, service="integration-api",
            action="preauth.claim.request.received", payload=body
        ))
        db.add(AuditLog(
            correlation_id=correlation_id, service="integration-api",
            action="preauth.claim.stored", payload=written
        ))
        db.add(MessageTracking(
            correlation_id=correlation_id, patient_id=claim.get("patient_identifier") or "unknown",
            status="RECEIVED", fhir_resource_id=claim_id
        ))
        db.commit()

        await send_kafka_message(producer, TOPICS["json_request"], claim_id, envelope.model_dump(mode="json"))

        logger.info(f"✅ Saved PreAuth request {claim_id} and published to json.request: {correlation_id}")
        return {"correlation_id": correlation_id, "status": "RECEIVED", "claim_id": claim_id}

    except Exception as e:
        logger.error(f"❌ Error submitting PreAuth request: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to submit PreAuth request: {str(e)}")


@app.get("/api/v1/preauth/claims/{claim_id}", tags=["preauth"])
def get_preauth_claim(claim_id: str):
    """The rows stored for one claim in each PreAuth table (404 if the claim is not stored)"""
    tables = read_endpoint_rows(engine, "preauth-claim", claim_id)
    if not tables["claim"]:
        raise HTTPException(status_code=404, detail="claim_id not found")
    return {"claim_id": claim_id, "rows": {name: len(rows) for name, rows in tables.items()}, "tables": tables}


@app.get("/api/v1/preauth/export/excel", tags=["preauth"])
def export_preauth_excel():
    """
    Excel copy of every PreAuth claim table (one sheet per table, same columns,
    all stored rows) - the claims fhir-json-service stored from
    POST /api/v1/preauth/responses on communication-service.
    """
    try:
        content = build_preauth_workbook(engine)
    except Exception as e:
        logger.error(f"❌ Error building PreAuth Excel export: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to build Excel export: {str(e)}")
    filename = f"preauth_tables_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
    return Response(
        content=content,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


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
