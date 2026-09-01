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
import sys
import os

# Add shared module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from shared import (
    PatientRequest, TransactionResponse, get_db,
    create_kafka_producer, send_kafka_message, TOPICS,
    Transaction, Base, engine
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


@app.post("/patient", response_model=TransactionResponse, tags=["Patients"])
async def create_patient(
    patient_data: PatientRequest,
    db: Session = Depends(get_db)
):
    """
    Accept patient JSON data and publish to Kafka
    
    Flow: JSON Input → Kafka (json.request) → JSON-FHIR Service
    """
    try:
        # Generate unique transaction ID
        transaction_id = f"TXN-{uuid.uuid4().hex[:12].upper()}"
        
        # Validate patient data
        if not patient_data.patient_id or not patient_data.name:
            raise HTTPException(
                status_code=400,
                detail="patient_id and name are required"
            )
        
        # Create transaction record
        transaction = Transaction(
            transaction_id=transaction_id,
            patient_id=patient_data.patient_id,
            patient_name=patient_data.name,
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
            "patient_id": patient_data.patient_id,
            "patient_name": patient_data.name,
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
        
        logger.info(f"✅ Patient {patient_data.patient_id} published to Kafka")
        
        return TransactionResponse(
            status="ACCEPTED",
            message="Patient data published to Kafka successfully",
            transaction_id=transaction_id,
            data=patient_data.dict()
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error processing patient: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process patient data: {str(e)}"
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
