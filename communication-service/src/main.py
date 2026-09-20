"""
FHIR Communication Service ("fhir-comm" per implementation.md section 6.4)
Rebuilt as a pure Kafka worker - NOT an HTTP service. Consumes fhir.outgoing,
calls the external FHIR server (HAPI FHIR locally), publishes both success
AND failure results to fhir.incoming so downstream (fhir-json-service) stays
uniform. This replaces the previous FastAPI app entirely - Dhamani/hospital
calling INTO this service over HTTP is not part of the rebuilt pipeline.
"""
import asyncio
import logging
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from shared import (
    create_kafka_consumer, create_kafka_producer, send_kafka_message,
    consume_kafka_messages, TOPICS, Envelope,
    SessionLocal, MessageTracking
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


if __name__ == "__main__":
    logger.info("🚀 FHIR Communication Service (fhir-comm) starting...")
    asyncio.run(start_service())
