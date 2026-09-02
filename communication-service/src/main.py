"""
Communication Service - FastAPI
Receives FHIR responses from Dhamani/Hospital system and publishes to Kafka
"""
import logging
import json
import uuid
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import sys
import os

# Add shared module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from shared import (
    HospitalResponse,
    create_kafka_producer, send_kafka_message, TOPICS,
    Base, engine
)

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


@app.on_event("startup")
async def startup_event():
    """Initialize producer on startup"""
    global producer
    try:
        producer = await create_kafka_producer()
        logger.info("🚀 Communication Service started")
    except Exception as e:
        logger.error(f"❌ Failed to start producer: {e}")
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Close producer on shutdown"""
    global producer
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8001,
        log_level="info"
    )
