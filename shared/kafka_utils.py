"""
Kafka utilities for message handling
"""
import json
import logging
import os
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from typing import Callable, Optional
import asyncio

logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")


async def create_kafka_producer():
    """Create and connect Kafka producer"""
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8') if v else b'',
        key_serializer=lambda k: str(k).encode('utf-8') if k else b''
    )
    await producer.start()
    logger.info("✅ Kafka Producer connected")
    return producer


async def create_kafka_consumer(group_id: str, topics: list):
    """Create and connect Kafka consumer"""
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BROKER,
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
        key_deserializer=lambda k: k.decode('utf-8') if k else None,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        session_timeout_ms=30000
    )
    await consumer.start()
    logger.info(f"✅ Kafka Consumer connected to topics: {topics}")
    return consumer


async def send_kafka_message(producer, topic: str, key: str, value: dict):
    """Send message to Kafka"""
    try:
        await producer.send_and_wait(topic, value=value, key=key)
        logger.info(f"📤 Message sent to {topic}: {key}")
    except Exception as e:
        logger.error(f"❌ Error sending to {topic}: {e}")
        raise


async def consume_kafka_messages(
    consumer,
    message_handler: Callable,
    timeout: Optional[int] = None
):
    """
    Consume messages from Kafka and process them
    
    Args:
        consumer: Kafka consumer instance
        message_handler: Async function to handle each message
        timeout: Optional timeout in seconds
    """
    try:
        async for message in consumer:
            try:
                await message_handler(message)
            except Exception as e:
                logger.error(f"❌ Error processing message: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"❌ Consumer error: {e}", exc_info=True)
    finally:
        await consumer.stop()


# Kafka Topics
TOPICS = {
    "json_request": "json.request",           # Input from client system
    "fhir_outgoing": "fhir.outgoing",         # JSON transformed to FHIR
    "fhir_incoming": "fhir.incoming",         # Response from hospital
    "json_response": "json.response",         # FHIR transformed back to JSON
    "fhir_response_outgoing": "fhir.response.outgoing",  # Our JSON decision transformed to FHIR, for delivery to Dhamani
    "json_request_incoming": "json.request.incoming",    # Dhamani's FHIR request transformed to JSON
}
