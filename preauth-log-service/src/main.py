"""
PreAuth Log Service
Dedicated Kafka consumer that persists every stage of the PreAuth
request/response flow to the audit log tables. Subscribes to all 4 PreAuth
topics in one consumer group and branches on the topic each message arrived
on to decide which table/stage to write.

This is a wholly separate process from every producer of these messages
(integration-api, communication-service, fhir-json-service) - a failure here
can never block or break the request/response flow it's observing, since
there is no shared call stack between this consumer and those services.
"""
import asyncio
import logging
import sys
import os

# Add shared module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from shared import (
    create_kafka_consumer, consume_kafka_messages, TOPICS,
    SessionLocal, PreAuthRequestLog, PreAuthResponseLog
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

consumer = None

# topic -> (model class, stage)
TOPIC_ROUTING = {
    TOPICS["preauth_json"]: (PreAuthRequestLog, "JSON_REQUEST"),
    TOPICS["preauth_fhir_outgoing"]: (PreAuthRequestLog, "FHIR_REQUEST"),
    TOPICS["preauth_fhir_incoming"]: (PreAuthResponseLog, "FHIR_RESPONSE"),
    TOPICS["preauth_json_response"]: (PreAuthResponseLog, "JSON_RESPONSE"),
}


async def log_preauth_stage(message):
    """Persist one PreAuth stage message to the appropriate log table"""
    try:
        topic = message.topic
        value = message.value or {}

        routing = TOPIC_ROUTING.get(topic)
        if not routing:
            logger.warning(f"⚠️ No routing for topic '{topic}', skipping")
            return

        model_class, stage = routing
        claim_id = value.get("claim_id")
        correlation_id = value.get("correlation_id")
        # Different producers use "payload" for flat JSON and "fhir_resource"
        # for FHIR bundles - accept either, matching the envelope convention
        # already established across this project's Kafka messages.
        payload = value.get("payload")
        if payload is None:
            payload = value.get("fhir_resource")

        if not claim_id or not correlation_id or payload is None:
            logger.warning(
                f"⚠️ Message on '{topic}' missing claim_id/correlation_id/payload, skipping: {value.keys()}"
            )
            return

        db = SessionLocal()
        try:
            log_row = model_class(
                claim_id=claim_id,
                correlation_id=correlation_id,
                stage=stage,
                payload=payload,  # dict assigned directly - the JSON column serializes it, do not json.dumps() here
                kafka_topic=topic
            )
            db.add(log_row)
            db.commit()
            logger.info(f"📝 Logged {stage} for claim {claim_id} (correlation_id={correlation_id}) from '{topic}'")
        finally:
            db.close()

    except Exception as e:
        # Logging must never propagate - this consumer's whole job is to be a
        # safe, isolated sink. consume_kafka_messages already catches per-message
        # errors too, but be explicit here since this IS the logging layer.
        logger.error(f"❌ Error logging PreAuth stage: {e}", exc_info=True)


async def start_service():
    """Start PreAuth Log Service"""
    global consumer

    try:
        topics = list(TOPIC_ROUTING.keys())
        consumer = await create_kafka_consumer("preauth-log-group", topics)
        logger.info(f"⚙️ Listening for messages on: {topics}")

        await consume_kafka_messages(consumer, log_preauth_stage)

    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        raise
    finally:
        if consumer:
            await consumer.stop()


if __name__ == "__main__":
    logger.info("🚀 PreAuth Log Service starting...")
    asyncio.run(start_service())
