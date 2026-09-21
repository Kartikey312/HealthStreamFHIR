"""
JSON to FHIR Transformer ("json-to-fhir" per implementation.md section 6.3)
Reads an Envelope from json.request, maps JSON -> FHIR Patient, publishes an
Envelope to fhir.outgoing. eligibility.request envelopes are mapped to a
Dhamani eligibility Bundle instead and published to eligibility.fhir.outgoing.
preauth.claim.request envelopes (a PreAuth claim as table rows) are mapped to a
Dhamani Claim Bundle and published to preauth.claim.fhir.outgoing.
"""
import asyncio
import logging
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from shared import (
    create_kafka_consumer, create_kafka_producer, send_kafka_message,
    consume_kafka_messages, TOPICS, Envelope, to_fhir_patient, to_fhir_eligibility_bundle, to_fhir_preauth_bundle,
    SessionLocal, MessageTracking, AuditLog
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

consumer = None
producer = None


async def process_eligibility_request(env: Envelope):
    """Transform one eligibility.request Envelope into a Dhamani FHIR Bundle and publish to eligibility.fhir.outgoing"""
    db = SessionLocal()
    tracking = None
    try:
        tracking = db.query(MessageTracking).filter(
            MessageTracking.correlation_id == env.correlation_id
        ).first()

        bundle = to_fhir_eligibility_bundle(env.payload)  # raises on bad input -> tracking FAILED below
        request_id = bundle["entry"][1]["resource"]["id"]

        db.add(AuditLog(
            correlation_id=env.correlation_id, service="json-to-fhir",
            action="eligibility.fhir.built", payload=bundle
        ))
        if tracking:
            tracking.status = "TRANSFORMED"
            tracking.fhir_resource_id = request_id
        db.commit()

        logger.info(f"✅ Transformed to FHIR eligibility Bundle: {bundle['id']}")
        logger.info(f"📦 Bundle:\n{json.dumps(bundle, indent=2, default=str)}")

        out = Envelope(
            correlation_id=env.correlation_id, source="json-to-fhir",
            event_type="eligibility.fhir.outgoing", payload=bundle,
        )
        await send_kafka_message(
            producer, TOPICS["eligibility_fhir_outgoing"], env.payload["patientIdentifier"],
            out.model_dump(mode="json")
        )

        logger.info(f"📤 Published to eligibility.fhir.outgoing: {env.correlation_id}")

    except Exception as e:
        if tracking:
            tracking.status = "FAILED"
            tracking.last_error = str(e)
            db.commit()
        raise
    finally:
        db.close()


async def process_preauth_request(env: Envelope):
    """Transform one preauth.claim.request Envelope (table rows) into a Dhamani Claim Bundle and publish to preauth.claim.fhir.outgoing"""
    db = SessionLocal()
    tracking = None
    try:
        tracking = db.query(MessageTracking).filter(
            MessageTracking.correlation_id == env.correlation_id
        ).first()

        bundle = to_fhir_preauth_bundle(env.payload)  # raises on bad input -> tracking FAILED below
        claim_id = env.payload["claim"][0]["id"]

        db.add(AuditLog(
            correlation_id=env.correlation_id, service="json-to-fhir",
            action="preauth.claim.fhir.built", payload=bundle
        ))
        if tracking:
            tracking.status = "TRANSFORMED"
        db.commit()

        logger.info(f"✅ Transformed PreAuth claim {claim_id} to a Claim Bundle: {bundle['id']}")

        out = Envelope(
            correlation_id=env.correlation_id, source="json-to-fhir",
            event_type="preauth.claim.fhir.outgoing", payload=bundle,
        )
        await send_kafka_message(
            producer, TOPICS["preauth_claim_fhir_outgoing"], claim_id, out.model_dump(mode="json")
        )

        logger.info(f"📤 Published to preauth.claim.fhir.outgoing: {env.correlation_id}")

    except Exception as e:
        db.rollback()
        if tracking:
            tracking.status = "FAILED"
            tracking.last_error = str(e)
            db.commit()
        raise
    finally:
        db.close()


async def process_json_request(message):
    """Transform one json.request Envelope into FHIR and publish to fhir.outgoing"""
    try:
        env = Envelope.model_validate(message.value)

        logger.info(f"📥 [json-to-fhir] Received: {env.correlation_id}")
        logger.info(f"📦 Payload:\n{json.dumps(env.payload, indent=2, default=str)}")

        if env.event_type == "eligibility.request":
            await process_eligibility_request(env)
            return

        if env.event_type == "preauth.claim.request":
            await process_preauth_request(env)
            return

        db = SessionLocal()
        tracking = None
        try:
            tracking = db.query(MessageTracking).filter(
                MessageTracking.correlation_id == env.correlation_id
            ).first()

            fhir_patient = to_fhir_patient(env.payload)  # raises on bad input -> logged, message still committed

            db.add(AuditLog(
                correlation_id=env.correlation_id, service="json-to-fhir",
                action="patient.fhir.outgoing.built", payload=fhir_patient
            ))
            if tracking:
                tracking.status = "TRANSFORMED"
                tracking.fhir_resource_id = fhir_patient.get("id")
            db.commit()

            logger.info(f"✅ Transformed to FHIR Patient: {fhir_patient['id']}")

            out = Envelope(
                correlation_id=env.correlation_id, source="json-to-fhir",
                event_type="patient.fhir.outgoing", payload=fhir_patient,
            )

            await send_kafka_message(
                producer, TOPICS["fhir_outgoing"], str(env.payload["patientId"]),
                out.model_dump(mode="json")
            )

            if tracking:
                tracking.status = "SENT"
                db.commit()

            logger.info(f"📤 Published to fhir.outgoing: {env.correlation_id}")

        except Exception as e:
            if tracking:
                tracking.status = "FAILED"
                tracking.last_error = str(e)
                db.commit()
            raise
        finally:
            db.close()

    except Exception as e:
        logger.error(f"❌ [json-to-fhir] Error processing message: {e}", exc_info=True)


async def start_service():
    global consumer, producer
    try:
        producer = await create_kafka_producer()
        logger.info("✅ Producer connected")

        consumer = await create_kafka_consumer("json-to-fhir-group", [TOPICS["json_request"]])
        logger.info("⚙️ Listening for messages on 'json.request'...")

        await consume_kafka_messages(consumer, process_json_request)

    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        raise
    finally:
        if producer:
            await producer.stop()
        if consumer:
            await consumer.stop()


if __name__ == "__main__":
    logger.info("🚀 JSON-to-FHIR Service starting...")
    asyncio.run(start_service())
