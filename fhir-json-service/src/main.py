"""
FHIR to JSON Transformation Service
Reads from fhir.incoming topic, transforms back to JSON, publishes to json.response
"""
import asyncio
import logging
import json
import sys
import os
from datetime import datetime

# Add shared module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from shared import (
    create_kafka_consumer, create_kafka_producer, send_kafka_message,
    consume_kafka_messages, TOPICS,
    fhir_to_json_response,
    SessionLocal, Transaction, FHIRResponse
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

consumer = None
producer = None


async def process_fhir_response(message):
    """Process incoming FHIR response and transform back to JSON"""
    try:
        # Parse message
        key = message.key
        value = message.value
        
        logger.info(f"📥 Received FHIR response: {key}")
        
        transaction_id = value.get("transaction_id")
        patient_id = value.get("patient_id")
        fhir_response = value.get("fhir_response", {})

        logger.info(f"📦 Incoming FHIR Bundle:\n{json.dumps(value, indent=2, default=str)}")

        # Get database session
        db = SessionLocal()

        try:
            # Transform FHIR Bundle back to flattened JSON
            logger.info(f"🔄 Transforming FHIR eligibility response to JSON for {patient_id}...")
            json_response = fhir_to_json_response(fhir_response, patient_id)
            logger.info(f"📦 Transformed JSON response:\n{json.dumps(json_response, indent=2, default=str)}")

            sync_status = json_response.get("sync_status")

            # Store FHIR response in database
            fhir_response_record = FHIRResponse(
                transaction_id=transaction_id,
                response_id=fhir_response.get("id") or f"RESP-{key}",
                fhir_payload=json.dumps(fhir_response),
                hospital_response_code=201 if sync_status == "SUCCESS" else 400,
                hospital_response_message=json_response.get("disposition") or json_response.get("outcome"),
                received_at=datetime.utcnow()
            )
            db.add(fhir_response_record)

            # Update transaction
            transaction = db.query(Transaction).filter(
                Transaction.transaction_id == transaction_id
            ).first()

            if transaction:
                transaction.status = sync_status

            db.commit()

            # Publish to next topic - carry the full flattened eligibility response
            kafka_message = {
                "transaction_id": transaction_id,
                **json_response
            }

            logger.info(f"📤 Publishing to json.response:\n{json.dumps(kafka_message, indent=2, default=str)}")

            await send_kafka_message(
                producer,
                TOPICS["json_response"],
                transaction_id,
                kafka_message
            )

            logger.info(f"📤 Published JSON to json.response: {transaction_id}")
            logger.info(f"✅ Transformation complete for patient {patient_id}")
            
            # Mark response as processed
            fhir_response_record.processed = True
            fhir_response_record.processed_at = datetime.utcnow()
            db.commit()
            
        finally:
            db.close()
    
    except Exception as e:
        logger.error(f"❌ Error processing FHIR response: {e}", exc_info=True)


async def start_service():
    """Start FHIR-JSON service"""
    global consumer, producer
    
    try:
        # Create producer
        producer = await create_kafka_producer()
        logger.info("✅ Producer connected")
        
        # Create consumer
        consumer = await create_kafka_consumer(
            "fhir-json-group",
            [TOPICS["fhir_incoming"]]
        )
        logger.info("⚙️ Listening for messages on 'fhir.incoming'...")
        
        # Consume messages
        await consume_kafka_messages(consumer, process_fhir_response)
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        raise
    finally:
        if producer:
            await producer.stop()
        if consumer:
            await consumer.stop()


if __name__ == "__main__":
    logger.info("🚀 FHIR-JSON Service starting...")
    asyncio.run(start_service())
