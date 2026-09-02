"""
Processing Service
Consumes final JSON responses and updates database
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
    create_kafka_consumer, consume_kafka_messages, TOPICS,
    SessionLocal, Transaction, ResponseMapping
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

consumer = None


async def process_final_response(message):
    """Process final JSON response and update database"""
    try:
        # Parse message
        key = message.key
        value = message.value
        
        logger.info(f"📥 Received final JSON response: {key}")
        
        transaction_id = value.get("transaction_id")
        patient_identifier = value.get("patientIdentifier")
        outcome = value.get("outcome")
        disposition = value.get("disposition")
        sync_status = value.get("sync_status")
        completed_at = value.get("completed_at")

        logger.info(f"📦 Final JSON response:\n{json.dumps(value, indent=2, default=str)}")

        # Get database session
        db = SessionLocal()
        
        try:
            # Update transaction status
            transaction = db.query(Transaction).filter(
                Transaction.transaction_id == transaction_id
            ).first()
            
            if transaction:
                transaction.status = sync_status
                db.commit()
            
            # Create response mapping
            response_mapping = ResponseMapping(
                transaction_id=transaction_id,
                original_json=json.dumps({"patient_id": patient_identifier}),
                final_json=json.dumps(value),
                status="COMPLETED"
            )
            db.add(response_mapping)
            db.commit()

            # Log completion
            logger.info("=" * 60)
            logger.info("🎉 END-TO-END FLOW COMPLETE!")
            logger.info("=" * 60)
            logger.info(f"Transaction ID: {transaction_id}")
            logger.info(f"Patient Identifier: {patient_identifier}")
            logger.info(f"Outcome: {outcome}")
            logger.info(f"Disposition: {disposition}")
            logger.info(f"Sync Status: {sync_status}")
            logger.info(f"Completed At: {completed_at}")
            logger.info("=" * 60)
            logger.info("")
            
        finally:
            db.close()
    
    except Exception as e:
        logger.error(f"❌ Error processing final response: {e}", exc_info=True)


async def start_service():
    """Start Processing service"""
    global consumer
    
    try:
        # Create consumer
        consumer = await create_kafka_consumer(
            "processing-group",
            [TOPICS["json_response"]]
        )
        logger.info("⚙️ Listening for messages on 'json.response'...")
        
        # Consume messages
        await consume_kafka_messages(consumer, process_final_response)
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        raise
    finally:
        if consumer:
            await consumer.stop()


if __name__ == "__main__":
    logger.info("🚀 Processing Service starting...")
    asyncio.run(start_service())
