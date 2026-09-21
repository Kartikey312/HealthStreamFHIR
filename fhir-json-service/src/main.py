"""
FHIR to JSON Transformer ("fhir-to-json" per implementation.md section 6.5)
Reads an Envelope from fhir.incoming, maps the FHIR communication result
(or an error) to the internal JSON response format, publishes an Envelope
to json.response. eligibility.fhir.incoming envelopes (a Dhamani
CoverageEligibilityResponse Bundle) are mapped to the eligibility response
JSON instead and published to eligibility.json.response (integration-api consumes
that event and saves the rows). preauth.claim.fhir.incoming
envelopes (an incoming PreAuth Claim Bundle) are mapped to the rows of the
PreAuth tables and published to preauth.claim.json.response - integration-api
consumes that event and saves the rows.
"""
import asyncio
import logging
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from shared import (
    create_kafka_consumer, create_kafka_producer, send_kafka_message,
    consume_kafka_messages, TOPICS, Envelope, to_internal_response, to_eligibility_response_json,
    to_preauth_tables,
    SessionLocal, MessageTracking, AuditLog
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

consumer = None
producer = None


async def process_eligibility_response(env: Envelope):
    """Map one eligibility.fhir.incoming Envelope (FHIR Bundle) to the response JSON and publish to eligibility.json.response"""
    db = SessionLocal()
    tracking = None
    try:
        tracking = db.query(MessageTracking).filter(
            MessageTracking.correlation_id == env.correlation_id
        ).first()

        json_response = to_eligibility_response_json(env.payload)  # raises on bad input -> tracking FAILED below

        db.add(AuditLog(
            correlation_id=env.correlation_id, service="fhir-to-json",
            action="eligibility.response.built", payload=json_response
        ))
        db.commit()  # status stays as it is - integration-api sets COMPLETED once it has saved the rows

        logger.info(f"✅ Mapped eligibility response to JSON: {json_response.get('requestIdentifier')}")
        logger.info(f"📦 Response JSON:\n{json.dumps(json_response, indent=2, default=str)}")

        out = Envelope(
            correlation_id=env.correlation_id, source="fhir-to-json",
            event_type="eligibility.response", payload=json_response,
        )
        await send_kafka_message(
            producer, TOPICS["eligibility_json_response"], env.correlation_id,
            out.model_dump(mode="json")
        )

        logger.info(f"📤 Published to eligibility.json.response: {env.correlation_id}")

    except Exception as e:
        if tracking:
            tracking.status = "FAILED"
            tracking.last_error = str(e)
            db.commit()
        raise
    finally:
        db.close()


async def process_preauth_claim(env: Envelope):
    """Map one preauth.claim.fhir.incoming Envelope (Claim Bundle) to table rows and publish them to preauth.claim.json.response"""
    db = SessionLocal()
    tracking = None
    try:
        tracking = db.query(MessageTracking).filter(
            MessageTracking.correlation_id == env.correlation_id
        ).first()

        tables = to_preauth_tables(env.payload)  # raises on bad input -> tracking FAILED below
        claim_id = tables["claim"][0]["id"]

        db.add(AuditLog(
            correlation_id=env.correlation_id, service="fhir-to-json",
            action="preauth.claim.response.built", payload=tables
        ))
        if tracking:
            tracking.status = "TRANSFORMED"
        db.commit()

        logger.info(f"✅ Mapped PreAuth claim {claim_id} to table rows: {({t: len(r) for t, r in tables.items()})}")

        out = Envelope(
            correlation_id=env.correlation_id, source="fhir-to-json",
            event_type="preauth.claim.response", payload=tables,
        )
        await send_kafka_message(
            producer, TOPICS["preauth_claim_json_response"], claim_id, out.model_dump(mode="json")
        )

        logger.info(f"📤 Published to preauth.claim.json.response: {env.correlation_id}")

    except Exception as e:
        db.rollback()
        if tracking:
            tracking.status = "FAILED"
            tracking.last_error = str(e)
            db.commit()
        raise
    finally:
        db.close()


async def process_fhir_incoming(message):
    """Transform one fhir.incoming Envelope into the internal JSON response and publish to json.response"""
    try:
        env = Envelope.model_validate(message.value)

        logger.info(f"📥 [fhir-to-json] Received: {env.correlation_id}")
        logger.info(f"📦 Payload:\n{json.dumps(env.payload, indent=2, default=str)}")

        if env.event_type == "eligibility.fhir.incoming":
            await process_eligibility_response(env)
            return

        if env.event_type == "preauth.claim.fhir.incoming":
            await process_preauth_claim(env)
            return

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
