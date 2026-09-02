"""
Integration API - FastAPI service
Entry point for JSON patient data from external systems
"""
import logging
import json
import uuid
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text
import sys
import os

# Add shared module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from shared import (
    PatientRequest, EligibilityResponseIn, TransactionResponse, get_db,
    create_kafka_producer, send_kafka_message, TOPICS,
    Transaction, Base, engine,
    json_to_fhir_response, json_to_fhir_claim
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Integration API",
    description="Entry point for JSON to FHIR conversion",
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
        logger.info("🚀 Integration API started")
    except Exception as e:
        logger.error(f"❌ Failed to start producer: {e}")
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Close producer on shutdown"""
    global producer
    if producer:
        await producer.stop()
        logger.info("🛑 Integration API stopped")


@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "Integration API",
        "version": "1.0.0"
    }


@app.post("/patient", response_model=TransactionResponse, tags=["Eligibility"])
async def create_patient(
    patient_data: PatientRequest,
    db: Session = Depends(get_db)
):
    """
    Accept a flattened CoverageEligibilityRequest JSON and publish to Kafka

    Flow: JSON Input → Kafka (json.request) → JSON-FHIR Service
    """
    try:
        # Generate unique transaction ID
        transaction_id = f"TXN-{uuid.uuid4().hex[:12].upper()}"

        if not patient_data.patientIdentifier or not patient_data.insurerIdentifier or not patient_data.providerIdentifier:
            raise HTTPException(
                status_code=400,
                detail="patientIdentifier, insurerIdentifier and providerIdentifier are required"
            )

        # Create transaction record
        transaction = Transaction(
            transaction_id=transaction_id,
            patient_id=patient_data.patientIdentifier,
            patient_name=patient_data.providerName,
            status="PENDING",
            json_payload=json.dumps(patient_data.dict())
        )
        db.add(transaction)
        db.commit()
        db.refresh(transaction)

        logger.info(f"📝 Created transaction: {transaction_id}")
        logger.info(f"📦 Incoming JSON payload:\n{json.dumps(patient_data.dict(), indent=2, default=str)}")

        # Publish to Kafka
        kafka_message = {
            "transaction_id": transaction_id,
            "patient_id": patient_data.patientIdentifier,
            "patient_name": patient_data.providerName,
            "status": patient_data.status,
            "payload": patient_data.dict()
        }

        logger.info(f"📤 Publishing to json.request:\n{json.dumps(kafka_message, indent=2, default=str)}")

        await send_kafka_message(
            producer,
            TOPICS["json_request"],
            transaction_id,
            kafka_message
        )

        # Update transaction status
        transaction.status = "PROCESSING"
        db.commit()

        logger.info(f"✅ Eligibility request for patient {patient_data.patientIdentifier} published to Kafka")

        return TransactionResponse(
            status="ACCEPTED",
            message="Eligibility request published to Kafka successfully",
            transaction_id=transaction_id,
            data=patient_data.dict()
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error processing eligibility request: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process eligibility request: {str(e)}"
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


@app.post("/preauth/{claim_id}", tags=["PreAuth"])
async def get_preauth_claim(
    claim_id: str,
    db: Session = Depends(get_db)
):
    """
    Call usp_get_preauth_claims_details_by_claim_id for the given claim id
    (matched against claim.id, claim.identifier, or
    claim.eligibility_response_identifier), and publish the resulting JSON
    for downstream JSON-to-FHIR conversion.

    Flow: MySQL SP -> JSON -> Kafka (preauth.json)
    """
    try:
        result = db.execute(
            text("CALL usp_get_preauth_claims_details_by_claim_id(:claim_id)"),
            {"claim_id": claim_id}
        )
        row = result.fetchone()
        db.commit()

        if not row or not row[0]:
            raise HTTPException(
                status_code=404,
                detail=f"No claim found for id '{claim_id}'"
            )

        preauth_json = json.loads(row[0])

        logger.info(f"📥 Fetched PreAuth claim data for: {claim_id}")
        logger.info(f"📦 PreAuth JSON:\n{json.dumps(preauth_json, indent=2, default=str)}")

        kafka_message = {
            "transaction_id": claim_id,
            "claim_id": claim_id,
            "payload": preauth_json
        }

        logger.info(f"📤 Publishing to preauth.json:\n{json.dumps(kafka_message, indent=2, default=str)}")

        await send_kafka_message(
            producer,
            TOPICS["preauth_json"],
            claim_id,
            kafka_message
        )

        logger.info(f"✅ PreAuth JSON published to preauth.json: {claim_id}")

        fhir_bundle = json_to_fhir_claim(preauth_json)

        logger.info(f"📦 Transformed PreAuth JSON to FHIR Claim Bundle:\n{json.dumps(fhir_bundle, indent=2, default=str)}")

        fhir_kafka_message = {
            "transaction_id": claim_id,
            "claim_id": claim_id,
            "fhir_resource": fhir_bundle
        }

        logger.info(f"📤 Publishing to preauth.fhir.outgoing:\n{json.dumps(fhir_kafka_message, indent=2, default=str)}")

        await send_kafka_message(
            producer,
            TOPICS["preauth_fhir_outgoing"],
            claim_id,
            fhir_kafka_message
        )

        logger.info(f"✅ PreAuth FHIR Bundle published to preauth.fhir.outgoing: {claim_id}")

        return {
            "status": "ACCEPTED",
            "message": "PreAuth claim data fetched, converted to FHIR, and published to Kafka",
            "claim_id": claim_id,
            "preauth_json": preauth_json,
            "fhir_bundle": fhir_bundle
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error fetching/publishing preauth claim: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process preauth claim: {str(e)}"
        )


@app.get("/transaction/{transaction_id}", tags=["Transactions"])
async def get_transaction_status(
    transaction_id: str,
    db: Session = Depends(get_db)
):
    """Get transaction status"""
    try:
        transaction = db.query(Transaction).filter(
            Transaction.transaction_id == transaction_id
        ).first()
        
        if not transaction:
            raise HTTPException(
                status_code=404,
                detail="Transaction not found"
            )
        
        return {
            "transaction_id": transaction_id,
            "status": transaction.status,
            "patient_id": transaction.patient_id,
            "patient_name": transaction.patient_name,
            "created_at": transaction.created_at,
            "updated_at": transaction.updated_at
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error retrieving transaction: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve transaction"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
