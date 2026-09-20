"""
Communication Service - FastAPI
This service is the side of the integration Dhamani/hospital calls into: it
accepts inbound FHIR (and JSON) payloads from them and publishes/returns
responses, so every endpoint path here is labeled "response" (POST
/fhir/response, POST /response, POST /preauth/{claim_id}/response, POST
/preauth/{claim_id}/fhir-response, POST /preauth/{claim_id}/fhir-response-result)
- opposite of integration-api, whose paths are labeled "response" for outbound
request-direction traffic. Some of these path labels are decoupled from the
literal payload direction on purpose (e.g. /preauth/{claim_id}/fhir-response
receives an inbound Claim REQUEST Bundle, not a response) - see each
endpoint's own docstring for what it actually carries.

PreAuth hosts BOTH sides of the PreAuth exchange:
  - POST /preauth/{claim_id}/fhir-response        - accepts a REAL Claim request Bundle from Dhamani
  - POST /preauth/{claim_id}/response              - generates a MOCK ClaimResponse
  - POST /preauth/{claim_id}/fhir-response-result  - accepts a REAL ClaimResponse Bundle from Dhamani
No PreAuth response is ever generated automatically - nothing calls the mock
generator or expects a real response until one of the last two is explicitly
called. (An earlier version of this service ran a background Kafka consumer
that generated a mock response automatically the instant a request was
published; that's been removed on purpose.)
"""
import logging
import json
import uuid
from typing import Optional, Dict, Any
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
import sys
import os

# Add shared module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from shared import (
    HospitalResponse, EligibilityResponseIn, get_db,
    create_kafka_producer, send_kafka_message, TOPICS,
    Base, engine,
    json_to_fhir_response, fhir_to_json_claim,
    PreAuthRequestLog
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

    Path is named "/fhir/response" - the Bundle it receives is eligibility
    RESPONSE data, matching the path name.

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

    Path is named "/response" - this endpoint builds and publishes an
    outbound eligibility RESPONSE, matching the path name.

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


@app.post("/preauth/{claim_id}/response", tags=["PreAuth"])
async def respond_to_preauth_claim(
    claim_id: str,
    correlation_id: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Manually generate and publish the mock PreAuth ClaimResponse for a claim
    that already has a FHIR request logged. This is the ONLY thing that
    produces a PreAuth response - nothing does this automatically anymore.

    Path is named "/preauth/{claim_id}/response" - it generates and
    publishes a mock RESPONSE, matching the path name.

    Reads the FHIR request straight from preauth_request_log (written
    independently by preauth-log-service, which keeps logging every request
    whether or not this endpoint is ever called) rather than needing to hold
    or re-fetch anything itself. If correlation_id isn't given, uses the
    most recently logged FHIR_REQUEST for this claim_id.

    Flow: preauth_request_log -> mock adjudicator -> Kafka (preauth.fhir.incoming)
    """
    try:
        query = db.query(PreAuthRequestLog).filter(
            PreAuthRequestLog.claim_id == claim_id,
            PreAuthRequestLog.stage == "FHIR_REQUEST"
        )
        if correlation_id:
            query = query.filter(PreAuthRequestLog.correlation_id == correlation_id)

        log_row = query.order_by(PreAuthRequestLog.created_at.desc()).first()

        if not log_row:
            detail = f"No logged FHIR request found for claim '{claim_id}'"
            if correlation_id:
                detail += f" and correlation_id '{correlation_id}'"
            raise HTTPException(status_code=404, detail=detail)

        fhir_bundle = log_row.payload
        resolved_correlation_id = log_row.correlation_id

        logger.info(f"📥 Manually responding to claim {claim_id} (correlation_id={resolved_correlation_id})")

        mock_response_bundle = build_mock_claim_response(fhir_bundle, resolved_correlation_id)

        logger.info(f"📦 Mock ClaimResponse Bundle:\n{json.dumps(mock_response_bundle, indent=2, default=str)}")

        kafka_message = {
            "transaction_id": claim_id,
            "claim_id": claim_id,
            "correlation_id": resolved_correlation_id,
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

        return {
            "status": "ACCEPTED",
            "message": "Mock PreAuth response generated and published to Kafka",
            "claim_id": claim_id,
            "correlation_id": resolved_correlation_id,
            "fhir_bundle": mock_response_bundle
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error responding to preauth claim: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to respond to preauth claim: {str(e)}"
        )


@app.post("/preauth/{claim_id}/fhir-response-result", tags=["PreAuth"])
async def receive_preauth_fhir_response(
    claim_id: str,
    fhir_bundle: Dict[str, Any],
    correlation_id: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Receive a REAL PreAuth ClaimResponse FHIR Bundle from Dhamani (as opposed
    to /preauth/{claim_id}/response, which generates a MOCK one internally).
    Named "fhir-response-result" (carries the adjudication result/decision) to
    stay distinct from /preauth/{claim_id}/fhir-response, which accepts the
    Claim itself.

    If correlation_id isn't given as a query param, it's resolved from the
    most recently logged FHIR_REQUEST for this claim_id, so the response can
    be tied back to the exact request round-trip it answers.

    Publishes to preauth.fhir.incoming in the same envelope shape the mock
    path already uses, so nothing downstream needs to change: fhir-json-service
    converts it to flattened JSON (fhir_to_json_claim_response) and publishes
    to preauth.json.response, and preauth-log-service persists both the
    FHIR_RESPONSE and JSON_RESPONSE stages to preauth_response_log - which is
    also what the Excel export reads from, so it shows up there automatically.

    Flow: Dhamani -> Communication Service -> Kafka (preauth.fhir.incoming)
          -> fhir-json-service -> Kafka (preauth.json.response)
          -> preauth-log-service -> preauth_response_log
    """
    try:
        if fhir_bundle.get("resourceType") != "Bundle":
            raise HTTPException(
                status_code=400,
                detail="resourceType must be 'Bundle'"
            )

        resolved_correlation_id = correlation_id
        if not resolved_correlation_id:
            log_row = db.query(PreAuthRequestLog).filter(
                PreAuthRequestLog.claim_id == claim_id,
                PreAuthRequestLog.stage == "FHIR_REQUEST"
            ).order_by(PreAuthRequestLog.created_at.desc()).first()

            if not log_row:
                raise HTTPException(
                    status_code=404,
                    detail=(
                        f"No logged FHIR request found for claim '{claim_id}' to resolve "
                        f"a correlation_id from - pass ?correlation_id=... explicitly"
                    )
                )
            resolved_correlation_id = log_row.correlation_id

        logger.info(f"📥 Received real PreAuth FHIR ClaimResponse for claim {claim_id} (correlation_id={resolved_correlation_id})")
        logger.info(f"📦 Incoming FHIR Bundle:\n{json.dumps(fhir_bundle, indent=2, default=str)}")

        kafka_message = {
            "transaction_id": claim_id,
            "claim_id": claim_id,
            "correlation_id": resolved_correlation_id,
            "fhir_resource": fhir_bundle
        }

        logger.info(f"📤 Publishing to preauth.fhir.incoming:\n{json.dumps(kafka_message, indent=2, default=str)}")

        await send_kafka_message(
            producer,
            TOPICS["preauth_fhir_incoming"],
            claim_id,
            kafka_message
        )

        logger.info(f"✅ Real PreAuth FHIR response published to preauth.fhir.incoming: {claim_id}")

        return {
            "status": "received",
            "message": "PreAuth FHIR response received, published for JSON conversion and DB/Excel logging",
            "claim_id": claim_id,
            "correlation_id": resolved_correlation_id
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error processing PreAuth FHIR response: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process PreAuth FHIR response: {str(e)}"
        )


@app.post("/preauth/{claim_id}/fhir-response", tags=["PreAuth"])
async def receive_preauth_fhir_claim_request(
    claim_id: str,
    fhir_bundle: Dict[str, Any]
):
    """
    Receive a REAL PreAuth Claim FHIR Bundle from Dhamani - a genuine
    priorauth-request, as opposed to POST /preauth/{claim_id} on
    integration-api, which builds its own outgoing Claim from our DB.

    Named "fhir-response" for consistency with this service's other paths
    (all labeled "response", since this service is the side Dhamani calls
    into) - NOT because of the payload direction: what it actually receives
    here is a Claim REQUEST, not a response. Named distinctly from
    /preauth/{claim_id}/fhir-response-result, which accepts the ClaimResponse
    decision instead.

    Converts it to flattened JSON (fhir_to_json_claim) and publishes both
    stages to the same topics POST /preauth/{claim_id} on integration-api
    already uses (preauth.json, preauth.fhir.outgoing), so preauth-log-service
    logs it identically as JSON_REQUEST/FHIR_REQUEST in preauth_request_log -
    no new topic or routing needed. A fresh correlation_id is generated here,
    so it can be paired with a later POST /preauth/{claim_id}/response (or
    /fhir-response-result) response.

    Flow: Dhamani -> Communication Service -> Kafka (preauth.json, preauth.fhir.outgoing)
    """
    try:
        if fhir_bundle.get("resourceType") != "Bundle":
            raise HTTPException(
                status_code=400,
                detail="resourceType must be 'Bundle'"
            )

        correlation_id = str(uuid.uuid4())

        json_claim = fhir_to_json_claim(fhir_bundle)

        logger.info(f"📥 Received real PreAuth FHIR Claim request for claim {claim_id} (correlation_id={correlation_id})")
        logger.info(f"📦 Incoming FHIR Bundle:\n{json.dumps(fhir_bundle, indent=2, default=str)}")
        logger.info(f"📦 Transformed PreAuth FHIR request to JSON:\n{json.dumps(json_claim, indent=2, default=str)}")

        kafka_message = {
            "transaction_id": claim_id,
            "claim_id": claim_id,
            "correlation_id": correlation_id,
            "payload": json_claim
        }

        logger.info(f"📤 Publishing to preauth.json:\n{json.dumps(kafka_message, indent=2, default=str)}")

        await send_kafka_message(
            producer,
            TOPICS["preauth_json"],
            claim_id,
            kafka_message
        )

        fhir_kafka_message = {
            "transaction_id": claim_id,
            "claim_id": claim_id,
            "correlation_id": correlation_id,
            "fhir_resource": fhir_bundle
        }

        logger.info(f"📤 Publishing to preauth.fhir.outgoing:\n{json.dumps(fhir_kafka_message, indent=2, default=str)}")

        await send_kafka_message(
            producer,
            TOPICS["preauth_fhir_outgoing"],
            claim_id,
            fhir_kafka_message
        )

        logger.info(f"✅ Real PreAuth FHIR Claim request published and logged: {claim_id}")

        return {
            "status": "received",
            "message": "PreAuth FHIR Claim request received, converted to JSON, and published for logging",
            "claim_id": claim_id,
            "correlation_id": correlation_id,
            "json_claim": json_claim
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error processing PreAuth FHIR Claim request: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process PreAuth FHIR Claim request: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8001,
        log_level="info"
    )
