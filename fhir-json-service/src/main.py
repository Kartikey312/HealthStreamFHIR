"""
FHIR to JSON Transformer ("fhir-to-json" per implementation.md section 6.5)
Reads an Envelope from fhir.incoming, maps the FHIR communication result
(or an error) to the internal JSON response format, publishes an Envelope
to json.response.
"""
import asyncio
import logging
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from shared import (
    create_kafka_consumer, create_kafka_producer, send_kafka_message,
    consume_kafka_messages, TOPICS, Envelope, to_internal_response,
    SessionLocal, MessageTracking, AuditLog
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

consumer = None
producer = None


async def process_fhir_incoming(message):
    """Transform one fhir.incoming Envelope into the internal JSON response and publish to json.response"""
    try:
        env = Envelope.model_validate(message.value)

        logger.info(f"📥 [fhir-to-json] Received: {env.correlation_id}")
        logger.info(f"📦 Payload:\n{json.dumps(env.payload, indent=2, default=str)}")

        json_response = to_internal_response(env.payload, env.error)

        logger.info(f"📦 Transformed JSON response: {json_response}")

        db = SessionLocal()
        try:
            db.add(AuditLog(
                correlation_id=env.correlation_id, service="fhir-to-json",
                action="patient.response.built", payload=json_response
            ))
            tracking = db.query(MessageTracking).filter(
                MessageTracking.correlation_id == env.correlation_id
            ).first()
            if tracking:
                tracking.status = "COMPLETED" if json_response["status"] == "SUCCESS" else "FAILED"
                if json_response.get("fhirId"):
                    tracking.fhir_resource_id = json_response["fhirId"]
                if json_response["status"] == "FAILED":
                    tracking.last_error = json_response.get("reason")
            db.commit()
        finally:
            db.close()

        out = Envelope(
            correlation_id=env.correlation_id, source="fhir-to-json",
            event_type="patient.response", payload=json_response,
        )

        await send_kafka_message(producer, TOPICS["json_response"], env.correlation_id, out.model_dump(mode="json"))

        logger.info(f"✅ Published to json.response: {env.correlation_id}")

    except Exception as e:
        logger.error(f"❌ [fhir-to-json] Error processing message: {e}", exc_info=True)


async def start_service():
    global consumer, producer
    try:
        producer = await create_kafka_producer()
        logger.info("✅ Producer connected")

        consumer = await create_kafka_consumer("fhir-to-json-group", [TOPICS["fhir_incoming"]])
        logger.info("⚙️ Listening for messages on 'fhir.incoming'...")

        await consume_kafka_messages(consumer, process_fhir_incoming)

    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        raise
    finally:
        if producer:
            await producer.stop()
        if consumer:
            await consumer.stop()


if __name__ == "__main__":
    logger.info("🚀 FHIR-to-JSON Service starting...")
    asyncio.run(start_service())
