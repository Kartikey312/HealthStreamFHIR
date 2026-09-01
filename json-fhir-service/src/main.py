"""
JSON to FHIR Transformation Service
Reads from json.request topic, transforms to FHIR, publishes to fhir.outgoing
"""
import asyncio
import logging
import json
import sys
import os
from sqlalchemy.orm import Session

# Add shared module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from shared import (
    create_kafka_consumer, create_kafka_producer, send_kafka_message,
    consume_kafka_messages, TOPICS,
    json_to_fhir_patient, validate_fhir_patient,
    SessionLocal, Transaction, FHIRRequest
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

consumer = None
producer = None


async def process_json_message(message):
    """Process incoming JSON message and transform to FHIR"""
    try:
        # Parse message
        key = message.key
        value = message.value
        
        logger.info(f"📥 Received JSON message: {key}")
        
        transaction_id = value.get("transaction_id")
        patient_id = value.get("patient_id")
        patient_name = value.get("patient_name")
        patient_data = value.get("payload", {})
        
        # Get database session
        db = SessionLocal()
        
        try:
            # Update transaction status
            transaction = db.query(Transaction).filter(
                Transaction.transaction_id == transaction_id
            ).first()
            
            if transaction:
                transaction.status = "PROCESSING"
                db.commit()
            
            # Transform JSON to FHIR
            logger.info(f"🔄 Transforming patient {patient_id} to FHIR...")
            fhir_patient = json_to_fhir_patient(patient_data)
            
            # Validate FHIR
            is_valid, errors = validate_fhir_patient(fhir_patient)
            
            if not is_valid:
                logger.error(f"❌ FHIR validation failed: {errors}")
                if transaction:
                    transaction.status = "FAILED"
                    db.commit()
                return
            
            logger.info(f"✅ FHIR Patient validated: {fhir_patient['id']}")
            
            # Store in database
            fhir_request = FHIRRequest(
                transaction_id=transaction_id,
                request_id=fhir_patient["id"],
                fhir_resource_type="Patient",
                fhir_payload=json.dumps(fhir_patient),
                validation_status="VALID"
            )
            db.add(fhir_request)
            
            if transaction:
                transaction.fhir_payload = json.dumps(fhir_patient)
            
            db.commit()
            
            # Publish to next topic
            kafka_message = {
                "transaction_id": transaction_id,
                "patient_id": patient_id,
                "patient_name": patient_name,
                "fhir_resource": fhir_patient
            }
            
            await send_kafka_message(
                producer,
                TOPICS["fhir_outgoing"],
                transaction_id,
                kafka_message
            )
            
            logger.info(f"📤 Published FHIR to fhir.outgoing: {transaction_id}")
            
        finally:
            db.close()
    
    except Exception as e:
        logger.error(f"❌ Error processing JSON message: {e}", exc_info=True)


async def start_service():
    """Start JSON-FHIR service"""
    global consumer, producer
    
    try:
        # Create producer
        producer = await create_kafka_producer()
        logger.info("✅ Producer connected")
        
        # Create consumer
        consumer = await create_kafka_consumer(
            "json-fhir-group",
            [TOPICS["json_request"]]
        )
        logger.info("⚙️ Listening for messages on 'json.request'...")
        
        # Consume messages
        await consume_kafka_messages(consumer, process_json_message)
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        raise
    finally:
        if producer:
            await producer.stop()
        if consumer:
            await consumer.stop()


if __name__ == "__main__":
    logger.info("🚀 JSON-FHIR Service starting...")
    asyncio.run(start_service())
