"""
Internal Processing Service ("internal-processing" per implementation.md
section 6.6) - final consumer. Reads an Envelope from json.response, marks
message_tracking COMPLETED/FAILED, and logs completion. (Workflows /
notifications are the plan's extension point, not implemented here.)
"""
import asyncio
import logging
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from shared import (
    create_kafka_consumer, consume_kafka_messages, TOPICS, Envelope,
    SessionLocal, MessageTracking, AuditLog
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

consumer = None


async def process_json_response(message):
    """Final step: mark message_tracking COMPLETED/FAILED for one json.response Envelope"""
    try:
        env = Envelope.model_validate(message.value)

        logger.info(f"📥 [internal-processing] Received: {env.correlation_id}")
        logger.info(f"📦 Payload:\n{json.dumps(env.payload, indent=2, default=str)}")

        status = env.payload.get("status", "FAILED")

        db = SessionLocal()
        try:
            db.add(AuditLog(
                correlation_id=env.correlation_id, service="internal-processing",
                action="patient.response.processed", payload=env.payload
            ))
            tracking = db.query(MessageTracking).filter(
                MessageTracking.correlation_id == env.correlation_id
            ).first()
            if tracking:
                tracking.status = "COMPLETED" if status == "SUCCESS" else "FAILED"
                if status == "FAILED":
                    tracking.last_error = env.payload.get("reason")
            db.commit()
        finally:
            db.close()

        logger.info("=" * 60)
        logger.info("🎉 END-TO-END FLOW COMPLETE!")
        logger.info("=" * 60)
        logger.info(f"Correlation ID: {env.correlation_id}")
        logger.info(f"Status: {status}")
        logger.info(f"FHIR ID: {env.payload.get('fhirId')}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"❌ [internal-processing] Error processing message: {e}", exc_info=True)


async def start_service():
    global consumer
    try:
        consumer = await create_kafka_consumer("internal-processing-group", [TOPICS["json_response"]])
        logger.info("⚙️ Listening for messages on 'json.response'...")

        await consume_kafka_messages(consumer, process_json_response)

    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        raise
    finally:
        if consumer:
            await consumer.stop()


if __name__ == "__main__":
    logger.info("🚀 Internal Processing Service starting...")
    asyncio.run(start_service())
