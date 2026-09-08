"""
Communication Service - FastAPI
Entry point for RESPONSE-direction traffic only: inbound FHIR responses
Dhamani/hospital sends us (POST /fhir/response) and outbound JSON->FHIR
responses we send to Dhamani (POST /response). Request-direction traffic
lives in integration-api.

Also runs a background Kafka consumer (not HTTP-triggered) for the PreAuth
flow: consumes the FHIR Claim requests integration-api publishes, runs them
through a STUB adjudicator (see mock_adjudication.py - no real payer
connection exists), and publishes the mock ClaimResponse onward.
"""
import asyncio
import logging
import json
import uuid
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
import sys
import os

# Add shared module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from shared import (
    HospitalResponse, EligibilityResponseIn, get_db,
    create_kafka_producer, create_kafka_consumer, consume_kafka_messages,
    send_kafka_message, TOPICS,
    Base, engine,
    json_to_fhir_response
)

from .mock_adjudication import build_mock_claim_response

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Communication Service",
    description="Receives FHIR responses from hospital/Dhamani system",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize database tables
try:
    Base.metadata.create_all(bind=engine)
    logger.info("✅ Database tables initialized")
except Exception as e:
    logger.error(f"❌ Error initializing database: {e}")

# Global producer
producer = None
preauth_consumer = None
preauth_consumer_task = None


async def handle_preauth_fhir_request(message):
    """
    Consumes a FHIR Claim request (published by integration-api's
    /preauth/{claim_id}), runs it through the stub adjudicator, and publishes
    the mock FHIR ClaimResponse. correlation_id/claim_id are carried forward
    unchanged from the message consumed here - never regenerated - so
    preauth-log-service's audit trail can tie this response back to its
    originating request.
    """
    try:
        value = message.value or {}
        claim_id = value.get("claim_id")
        correlation_id = value.get("correlation_id")
        fhir_bundle = value.get("fhir_resource")

        if not claim_id or not correlation_id or not fhir_bundle:
            logger.warning(f"⚠️ PreAuth FHIR request message missing fields, skipping: {list(value.keys())}")
            return

        logger.info(f"📥 Received PreAuth FHIR request for claim {claim_id} (correlation_id={correlation_id})")

        mock_response_bundle = build_mock_claim_response(fhir_bundle, correlation_id)

        logger.info(f"📦 Mock ClaimResponse Bundle:\n{json.dumps(mock_response_bundle, indent=2, default=str)}")

        kafka_message = {
            "transaction_id": claim_id,
            "claim_id": claim_id,
            "correlation_id": correlation_id,
            "fhir_resource": mock_response_bundle
        }

        logger.info(f"📤 Publishing to preauth.fhir.incoming:\n{json.dumps(kafka_message, indent=2, default=str)}")

        await send_kafka_message(
            producer,
            TOPICS["preauth_fhir_incoming"],
            claim_id,
            kafka_message
        )

        logger.info(f"✅ Mock PreAuth FHIR response published to preauth.fhir.incoming: {claim_id}")

    except Exception as e:
        logger.error(f"❌ Error handling PreAuth FHIR request: {e}", exc_info=True)


async def run_preauth_consumer():
    """Background loop: consumes preauth.fhir.outgoing for the mock adjudicator"""
    global preauth_consumer
    try:
        preauth_consumer = await create_kafka_consumer(
            "communication-preauth-group",
            [TOPICS["preauth_fhir_outgoing"]]
        )
        logger.info("⚙️ Listening for PreAuth FHIR requests on 'preauth.fhir.outgoing'...")
        await consume_kafka_messages(preauth_consumer, handle_preauth_fhir_request)
    except Exception as e:
        logger.error(f"❌ PreAuth consumer fatal error: {e}", exc_info=True)


@app.on_event("startup")
async def startup_event():
    """Initialize producer and background PreAuth consumer on startup"""
    global producer, preauth_consumer_task
    try:
        producer = await create_kafka_producer()
        preauth_consumer_task = asyncio.create_task(run_preauth_consumer())
        logger.info("🚀 Communication Service started")
    except Exception as e:
        logger.error(f"❌ Failed to start producer: {e}")
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Close producer and background consumer on shutdown"""
    global producer, preauth_consumer, preauth_consumer_task
    if preauth_consumer_task:
        preauth_consumer_task.cancel()
    if preauth_consumer:
        await preauth_consumer.stop()
    if producer:
        await producer.stop()
        logger.info("🛑 Communication Service stopped")


@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "Communication Service",
        "version": "1.0.0"
    }


@app.post("/fhir/response", tags=["FHIR"])
async def receive_fhir_response(response: HospitalResponse):
    """
    Receive a CoverageEligibilityResponse FHIR Bundle from hospital/Dhamani system

    Flow: Dhamani → Communication Service → Kafka (fhir.incoming) → FHIR-JSON Service
    """
    try:
        # Validate response
        if not response.original_id:
            raise HTTPException(
                status_code=400,
                detail="original_id is required"
            )

        # Generate response ID
        response_id = f"RESP-{uuid.uuid4().hex[:12].upper()}"

        logger.info(f"📨 Received FHIR response from hospital: {response_id}")
        logger.info(f"📦 Incoming hospital FHIR Bundle:\n{json.dumps(response.dict(), indent=2, default=str)}")

        # Create Kafka message - pass the raw Dhamani Bundle straight through
        kafka_message = {
            "transaction_id": response.original_id,
            "patient_id": response.original_id,
            "fhir_response": response.fhir_response
        }

        logger.info(f"📤 Publishing to fhir.incoming:\n{json.dumps(kafka_message, indent=2, default=str)}")

        # Publish to Kafka
        await send_kafka_message(
            producer,
            TOPICS["fhir_incoming"],
            response.original_id,
            kafka_message
        )

        logger.info(f"✅ FHIR response published to fhir.incoming: {response_id}")
        
        return {
            "status": "received",
            "message": "FHIR response received and published",
            "response_id": response_id,
            "transaction_id": response.original_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error processing FHIR response: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process FHIR response: {str(e)}"
        )


@app.post("/response", tags=["Eligibility"])
async def create_response(
    response_data: EligibilityResponseIn,
    db: Session = Depends(get_db)
):
    """
    Accept our flattened CoverageEligibilityResponse decision JSON, convert it
    to a FHIR Bundle, and publish it for delivery to Dhamani.

    Flow: JSON Input → FHIR Bundle → Kafka (fhir.response.outgoing)
    """
    try:
        if not response_data.patientIdentifier or not response_data.insurerIdentifier:
            raise HTTPException(
                status_code=400,
                detail="patientIdentifier and insurerIdentifier are required"
            )

        fhir_bundle = json_to_fhir_response(response_data.dict())

        transaction_id = (
            response_data.requestIdentifier
            or response_data.id
            or f"TXN-{uuid.uuid4().hex[:12].upper()}"
        )

        logger.info(f"📝 Built FHIR response for transaction: {transaction_id}")
        logger.info(f"📦 Transformed FHIR Bundle:\n{json.dumps(fhir_bundle, indent=2, default=str)}")

        kafka_message = {
            "transaction_id": transaction_id,
            "patient_id": response_data.patientIdentifier,
            "fhir_resource": fhir_bundle
        }

        logger.info(f"📤 Publishing to fhir.response.outgoing:\n{json.dumps(kafka_message, indent=2, default=str)}")

        await send_kafka_message(
            producer,
            TOPICS["fhir_response_outgoing"],
            transaction_id,
            kafka_message
        )

        logger.info(f"✅ Eligibility response for patient {response_data.patientIdentifier} published to Kafka")

        return {
            "status": "ACCEPTED",
            "message": "Eligibility response converted to FHIR and published to Kafka",
            "transaction_id": transaction_id,
            "fhir_bundle": fhir_bundle
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error processing eligibility response: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process eligibility response: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8001,
        log_level="info"
    )
