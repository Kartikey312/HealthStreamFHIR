"""
FHIR Communication Service ("fhir-comm" per implementation.md section 6.4)
Kafka worker: consumes fhir.outgoing, calls the external FHIR server (HAPI
FHIR locally), publishes both success AND failure results to fhir.incoming so
downstream (fhir-json-service) stays uniform.

Also serves one HTTP route, the side Dhamani/the insurer calls into:
POST /api/v1/eligibility/responses accepts a CoverageEligibilityResponse
FHIR Bundle and publishes it to fhir.incoming as an eligibility.fhir.incoming
event. The worker runs as a background task of the FastAPI app, so both live
in this one process.
"""
import asyncio
import logging
import json
import os
import sys
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from shared import (
    create_kafka_consumer, create_kafka_producer, send_kafka_message,
    consume_kafka_messages, TOPICS, Envelope,
    SessionLocal, MessageTracking, AuditLog
)

from .fhir_client import FhirClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FHIR_SERVER_URL = os.getenv("FHIR_SERVER_URL", "http://fhir-server:8080/fhir")

consumer = None
producer = None
client = None


async def handle_fhir_outgoing(message):
    """Consume one fhir.outgoing Envelope, call the external FHIR server, publish the result"""
    try:
        env = Envelope.model_validate(message.value)

        logger.info(f"📥 [fhir-comm] Received outbound FHIR: {env.correlation_id}")
        logger.info(f"📦 Patient resource:\n{json.dumps(env.payload, indent=2, default=str)}")

        r = await client.upsert_patient(env.payload)
        result = {"http_status": r.status_code, "body": r.json() if r.content else None}

        out = Envelope(
            correlation_id=env.correlation_id, source="fhir-comm",
            event_type="patient.fhir.incoming", payload=result,
        )
        if r.status_code >= 400:
            out.error = {"type": "FhirServerError", "detail": r.text}

        db = SessionLocal()
        try:
            tracking = db.query(MessageTracking).filter(
                MessageTracking.correlation_id == env.correlation_id
            ).first()
            if tracking:
                tracking.status = "RESPONDED"
                if result.get("body"):
                    tracking.fhir_resource_id = result["body"].get("id")
                if out.error:
                    tracking.last_error = out.error["detail"]
                db.commit()
        finally:
            db.close()

        key = env.payload.get("identifier", [{}])[0].get("value", env.correlation_id)
        await send_kafka_message(producer, TOPICS["fhir_incoming"], key, out.model_dump(mode="json"))

        logger.info(f"✅ [fhir-comm] Published to fhir.incoming: {env.correlation_id} (status={r.status_code})")

    except Exception as e:
        logger.error(f"❌ [fhir-comm] Error handling fhir.outgoing message: {e}", exc_info=True)


async def start_service():
    global consumer, producer, client
    try:
        client = FhirClient(FHIR_SERVER_URL)
        producer = await create_kafka_producer()
        logger.info(f"✅ FHIR client targeting {FHIR_SERVER_URL}")

        consumer = await create_kafka_consumer("fhir-comm-group", [TOPICS["fhir_outgoing"]])
        logger.info("⚙️ Listening for messages on 'fhir.outgoing'...")

        await consume_kafka_messages(consumer, handle_fhir_outgoing)

    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        raise
    finally:
        if producer:
            await producer.stop()
        if consumer:
            await consumer.stop()
        if client:
            await client.close()


def _log_worker_exit(task: asyncio.Task):
    if not task.cancelled() and task.exception():
        logger.error(f"❌ Kafka worker stopped: {task.exception()!r}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    worker = asyncio.create_task(start_service())
    worker.add_done_callback(_log_worker_exit)
    logger.info("🚀 FHIR Communication Service (fhir-comm) starting...")
    yield
    worker.cancel()


app = FastAPI(title="Communication Service", version="1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", tags=["Health"])
async def health():
    return {"status": "ok", "service": "Communication Service", "version": "1.0"}


@app.post("/api/v1/eligibility/responses", status_code=202, tags=["eligibility"])
async def receive_eligibility_response(bundle: dict = Body(...)):
    """
    Inbound CoverageEligibilityResponse FHIR Bundle from Dhamani/the insurer.
    Publishes it to fhir.incoming (event_type eligibility.fhir.incoming);
    fhir-json-service maps it to the response JSON. Returns 202 immediately.

    The original request is found by the response's request.identifier, which
    is the CoverageEligibilityRequest id we sent (message_tracking
    .fhir_resource_id). An unmatched response is still processed, under a new
    correlation_id.
    """
    if producer is None:
        raise HTTPException(status_code=503, detail="Kafka producer not ready")

    if bundle.get("resourceType") != "Bundle":
        raise HTTPException(status_code=422, detail="Body must be a FHIR Bundle")
    response = next(
        (e.get("resource", {}) for e in bundle.get("entry", []) or []
         if e.get("resource", {}).get("resourceType") == "CoverageEligibilityResponse"),
        None,
    )
    if response is None:
        raise HTTPException(status_code=422, detail="Bundle has no CoverageEligibilityResponse entry")

    request_id = (response.get("request", {}).get("identifier", {}) or {}).get("value")

    try:
        db = SessionLocal()
        try:
            tracking = None
            if request_id:
                tracking = db.query(MessageTracking).filter(
                    MessageTracking.fhir_resource_id == request_id
                ).order_by(MessageTracking.created_at.desc()).first()

            if tracking:
                correlation_id = tracking.correlation_id
                tracking.status = "RESPONDED"
            else:
                correlation_id = str(uuid4())
                logger.warning(f"⚠️ No request found for eligibility response request id {request_id!r}")

            db.add(AuditLog(
                correlation_id=correlation_id, service="fhir-comm",
                action="eligibility.fhir.response.received", payload=bundle
            ))
            db.commit()
        finally:
            db.close()

        envelope = Envelope(
            correlation_id=correlation_id, source="fhir-comm",
            event_type="eligibility.fhir.incoming", payload=bundle,
        )
        await send_kafka_message(
            producer, TOPICS["fhir_incoming"], request_id or correlation_id,
            envelope.model_dump(mode="json")
        )

        logger.info(f"✅ [fhir-comm] Published eligibility response to fhir.incoming: {correlation_id}")
        return {"correlation_id": correlation_id, "status": "RECEIVED", "matched_request": tracking is not None}

    except Exception as e:
        logger.error(f"❌ Error receiving eligibility response: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to receive eligibility response: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")
