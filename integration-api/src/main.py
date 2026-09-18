"""
Integration API - FastAPI service
Naming convention only, not the data direction: endpoint paths here are
labeled "response" (POST /fhir/response, POST /preauth) while
communication-service's are labeled "request" (POST /fhir/request, etc.) -
the underlying request/response logic each endpoint performs is unchanged.
"""
import logging
import json
import uuid
from io import BytesIO
from typing import Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import text
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
import sys
import os

# Add shared module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from shared import (
    PatientRequest, TransactionResponse, get_db,
    create_kafka_producer, send_kafka_message, TOPICS,
    Transaction, Base, engine,
    json_to_fhir_claim, fhir_to_json_request
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


@app.post("/fhir/response", tags=["FHIR"])
async def receive_fhir_request(fhir_bundle: Dict[str, Any]):
    """
    Dummy Dhamani-facing endpoint: receives a CoverageEligibilityRequest FHIR
    Bundle (as Dhamani would send it, on behalf of a provider), converts it to
    flattened JSON, and publishes it for internal processing.

    Path is named "/fhir/response" per this service's naming convention
    (integration-api endpoints are labeled "response") - the Bundle it
    receives and the JSON it produces are still eligibility REQUEST data,
    unchanged from before.

    Flow: Dhamani → Integration API → Kafka (json.request.incoming)
    """
    try:
        if fhir_bundle.get("resourceType") != "Bundle":
            raise HTTPException(
                status_code=400,
                detail="resourceType must be 'Bundle'"
            )

        logger.info(f"📨 Received FHIR request Bundle from Dhamani: {fhir_bundle.get('id')}")
        logger.info(f"📦 Incoming FHIR Bundle:\n{json.dumps(fhir_bundle, indent=2, default=str)}")

        json_request = fhir_to_json_request(fhir_bundle)

        transaction_id = (
            json_request.get("identifier")
            or json_request.get("id")
            or f"REQ-{uuid.uuid4().hex[:12].upper()}"
        )

        logger.info(f"📦 Transformed FHIR request to JSON:\n{json.dumps(json_request, indent=2, default=str)}")

        kafka_message = {
            "transaction_id": transaction_id,
            "patient_id": json_request.get("patientIdentifier"),
            "payload": json_request
        }

        logger.info(f"📤 Publishing to json.request.incoming:\n{json.dumps(kafka_message, indent=2, default=str)}")

        await send_kafka_message(
            producer,
            TOPICS["json_request_incoming"],
            transaction_id,
            kafka_message
        )

        logger.info(f"✅ JSON request published to json.request.incoming: {transaction_id}")

        return {
            "status": "received",
            "message": "FHIR request received, converted to JSON, and published",
            "transaction_id": transaction_id,
            "json_request": json_request
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error processing FHIR request: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process FHIR request: {str(e)}"
        )


@app.post("/preauth/{claim_id}", tags=["PreAuth"])
async def get_preauth_claim(
    claim_id: str,
    db: Session = Depends(get_db)
):
    """
    Call usp_get_preauth_claim_source_data_by_claim_id for the given claim id
    (matched against claim.id, claim.identifier, or
    claim.eligibility_response_identifier), publish the resulting JSON and its
    FHIR conversion for downstream processing.

    A correlation_id is generated once here and carried as a sibling field on
    every message published for this invocation (request side here; response
    side downstream in communication-service/fhir-json-service), so a single
    round-trip's messages can be told apart from a later re-run of the same
    claim_id. preauth-log-service, a separate Kafka consumer, is what actually
    persists each stage - nothing here writes to the log tables directly, so a
    logging failure downstream can never affect this endpoint's response.

    Flow: MySQL SP -> JSON -> Kafka (preauth.json) -> FHIR -> Kafka (preauth.fhir.outgoing)
    """
    try:
        correlation_id = str(uuid.uuid4())

        result = db.execute(
            text("CALL usp_get_preauth_claim_source_data_by_claim_id(:claim_id)"),
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

        logger.info(f"📥 Fetched PreAuth claim data for: {claim_id} (correlation_id={correlation_id})")
        logger.info(f"📦 PreAuth JSON:\n{json.dumps(preauth_json, indent=2, default=str)}")

        kafka_message = {
            "transaction_id": claim_id,
            "claim_id": claim_id,
            "correlation_id": correlation_id,
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

        logger.info(f"✅ PreAuth FHIR Bundle published to preauth.fhir.outgoing: {claim_id}")

        return {
            "status": "ACCEPTED",
            "message": "PreAuth claim data fetched, converted to FHIR, and published to Kafka",
            "claim_id": claim_id,
            "correlation_id": correlation_id,
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


@app.get("/preauth/{claim_id}/audit-trail", tags=["PreAuth"])
async def get_preauth_audit_trail(
    claim_id: str,
    db: Session = Depends(get_db)
):
    """
    Read-only: returns the full PreAuth request+response audit log trail for a
    claim (every JSON_REQUEST/FHIR_REQUEST/FHIR_RESPONSE/JSON_RESPONSE stage
    preauth-log-service has recorded), ordered by when each stage happened.

    Flow: usp_get_preauth_claims_details_by_claim_id -> rows
    """
    try:
        result = db.execute(
            text("CALL usp_get_preauth_claims_details_by_claim_id(:claim_id)"),
            {"claim_id": claim_id}
        )
        rows = result.fetchall()

        trail = [
            {
                "id": row.id,
                "claim_id": row.claim_id,
                "correlation_id": row.correlation_id,
                "stage": row.stage,
                "log_table": row.log_table,
                "payload": row.payload if isinstance(row.payload, dict) else json.loads(row.payload),
                "created_at": row.created_at
            }
            for row in rows
        ]

        return {
            "claim_id": claim_id,
            "count": len(trail),
            "trail": trail
        }

    except Exception as e:
        logger.error(f"❌ Error fetching preauth audit trail: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch audit trail: {str(e)}"
        )


def _resolve_current_chain_entries(db: Session, claim_id: str, correlation_id: Optional[str] = None):
    """
    Fetches the full PreAuth trail for claim_id and narrows it to one chain
    execution (one correlation_id) - the most recently logged one if
    correlation_id isn't given. Returns (resolved_correlation_id, chain_entries).
    Raises HTTPException(404) if nothing matches.
    """
    result = db.execute(
        text("CALL usp_get_preauth_claims_details_by_claim_id(:claim_id)"),
        {"claim_id": claim_id}
    )
    rows = result.fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No PreAuth log entries found for claim '{claim_id}'")

    all_entries = [
        {
            "id": row.id,
            "claim_id": row.claim_id,
            "correlation_id": row.correlation_id,
            "stage": row.stage,
            "log_table": row.log_table,
            "payload": row.payload if isinstance(row.payload, dict) else json.loads(row.payload),
            "created_at": row.created_at
        }
        for row in rows
    ]

    # SP orders by created_at, id ascending - the last entry is the most recent one.
    resolved_correlation_id = correlation_id or all_entries[-1]["correlation_id"]

    chain_entries = [e for e in all_entries if e["correlation_id"] == resolved_correlation_id]

    if not chain_entries:
        raise HTTPException(
            status_code=404,
            detail=f"No PreAuth log entries found for claim '{claim_id}' and correlation_id '{resolved_correlation_id}'"
        )

    return resolved_correlation_id, chain_entries


@app.get("/preauth/{claim_id}/current-trail", tags=["PreAuth"])
async def get_preauth_current_chain_trail(
    claim_id: str,
    correlation_id: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Read-only: like /audit-trail but scoped to a single PreAuth chain
    execution instead of every chain ever run for this claim_id - the
    JSON_REQUEST/FHIR_REQUEST/FHIR_RESPONSE/JSON_RESPONSE stages that share
    one correlation_id. If correlation_id isn't given, resolves to the
    correlation_id of the most recently logged stage for this claim_id.

    Flow: usp_get_preauth_claims_details_by_claim_id -> filter to one correlation_id
    """
    try:
        resolved_correlation_id, chain_entries = _resolve_current_chain_entries(db, claim_id, correlation_id)
        by_stage = {e["stage"]: e for e in chain_entries}

        return {
            "claim_id": claim_id,
            "correlation_id": resolved_correlation_id,
            "count": len(chain_entries),
            "json_request": by_stage.get("JSON_REQUEST"),
            "fhir_request": by_stage.get("FHIR_REQUEST"),
            "fhir_response": by_stage.get("FHIR_RESPONSE"),
            "json_response": by_stage.get("JSON_RESPONSE"),
            "trail": chain_entries
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error fetching current PreAuth chain trail: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch current chain trail: {str(e)}"
        )


HEADER_FONT = Font(bold=True, color="FFFFFF")
HEADER_FILL = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
SECTION_FONT = Font(bold=True, size=13)


def _write_table_sheet(wb, db: Session, table_name: str, sample_limit: int = 20):
    structure_rows = db.execute(text("""
        SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_DEFAULT, EXTRA, COLUMN_COMMENT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name
        ORDER BY ORDINAL_POSITION
    """), {"table_name": table_name}).fetchall()

    sample_rows = db.execute(text(
        f"SELECT * FROM {table_name} ORDER BY created_at DESC, id DESC LIMIT :limit"
    ), {"limit": sample_limit}).fetchall()

    ws = wb.create_sheet(title=table_name)

    ws.cell(row=1, column=1, value=f"Table structure: {table_name}").font = SECTION_FONT
    struct_headers = ["Column Name", "Data Type", "Nullable", "Key", "Default", "Extra", "Comment"]
    header_row = 3
    for col_idx, header in enumerate(struct_headers, start=1):
        cell = ws.cell(row=header_row, column=col_idx, value=header)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL

    r = header_row + 1
    for col in structure_rows:
        ws.cell(row=r, column=1, value=col.COLUMN_NAME)
        ws.cell(row=r, column=2, value=col.COLUMN_TYPE)
        ws.cell(row=r, column=3, value=col.IS_NULLABLE)
        ws.cell(row=r, column=4, value=col.COLUMN_KEY)
        ws.cell(row=r, column=5, value=str(col.COLUMN_DEFAULT) if col.COLUMN_DEFAULT is not None else "")
        ws.cell(row=r, column=6, value=col.EXTRA)
        ws.cell(row=r, column=7, value=col.COLUMN_COMMENT)
        r += 1

    r += 2
    ws.cell(row=r, column=1, value=f"Sample data ({len(sample_rows)} most recent rows)").font = SECTION_FONT
    r += 2

    if sample_rows:
        sample_headers = list(sample_rows[0]._mapping.keys())
        for col_idx, header in enumerate(sample_headers, start=1):
            cell = ws.cell(row=r, column=col_idx, value=header)
            cell.font = HEADER_FONT
            cell.fill = HEADER_FILL
        r += 1

        for row in sample_rows:
            row_map = row._mapping
            for col_idx, header in enumerate(sample_headers, start=1):
                value = row_map[header]
                if header == "payload":
                    # SQLAlchemy's JSON column type already deserializes this
                    # to a dict/list - re-serialize it pretty for a readable cell.
                    try:
                        value = json.dumps(value, indent=2, default=str)
                    except (TypeError, ValueError):
                        value = str(value)
                elif value is not None and not isinstance(value, (str, int, float)):
                    value = str(value)
                ws.cell(row=r, column=col_idx, value=value)
            r += 1
    else:
        ws.cell(row=r, column=1, value="(no rows yet)")

    widths = {}
    for row in ws.iter_rows():
        for cell in row:
            if cell.value is None:
                continue
            length = len(str(cell.value))
            widths[cell.column_letter] = max(widths.get(cell.column_letter, 0), min(length + 2, 80))
    for col, width in widths.items():
        ws.column_dimensions[col].width = max(width, 10)


@app.get("/preauth/export/excel", tags=["PreAuth"])
async def export_preauth_log_workbook(db: Session = Depends(get_db)):
    """
    Generates and downloads an Excel workbook documenting the PreAuth audit
    log tables (preauth_request_log, preauth_response_log) - one sheet per
    table, real column structure + real recent sample rows pulled live from
    the database. Open this URL directly in a browser to download it.
    """
    try:
        wb = Workbook()
        wb.remove(wb.active)

        for table_name in ("preauth_request_log", "preauth_response_log"):
            _write_table_sheet(wb, db, table_name)

        buffer = BytesIO()
        wb.save(buffer)
        buffer.seek(0)

        return StreamingResponse(
            buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=preauth_log_tables.xlsx"}
        )

    except Exception as e:
        logger.error(f"❌ Error exporting preauth log workbook: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to export preauth log workbook: {str(e)}"
        )


CHAIN_STAGES_ALL = ["JSON_REQUEST", "FHIR_REQUEST", "FHIR_RESPONSE", "JSON_RESPONSE"]
CHAIN_STAGES_REQUEST = ["JSON_REQUEST", "FHIR_REQUEST"]
CHAIN_STAGES_RESPONSE = ["FHIR_RESPONSE", "JSON_RESPONSE"]


def _write_current_chain_sheet(
    wb, claim_id: str, correlation_id: str, chain_entries: list,
    sheet_title: str = "Current Chain", stage_order: list = CHAIN_STAGES_ALL
):
    ws = wb.create_sheet(title=sheet_title)

    ws.cell(row=1, column=1, value=f"Current chain execution for claim: {claim_id}").font = SECTION_FONT
    ws.cell(row=2, column=1, value=f"correlation_id: {correlation_id}")

    headers = ["Stage", "Log Table", "Created At", "Payload"]
    header_row = 4
    for col_idx, header in enumerate(headers, start=1):
        cell = ws.cell(row=header_row, column=col_idx, value=header)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL

    by_stage = {e["stage"]: e for e in chain_entries}

    r = header_row + 1
    for stage in stage_order:
        entry = by_stage.get(stage)
        ws.cell(row=r, column=1, value=stage)
        if entry:
            ws.cell(row=r, column=2, value=entry["log_table"])
            ws.cell(row=r, column=3, value=str(entry["created_at"]))
            payload_cell = ws.cell(row=r, column=4, value=json.dumps(entry["payload"], indent=2, default=str))
            payload_cell.alignment = Alignment(wrap_text=True, vertical="top")
        else:
            ws.cell(row=r, column=2, value="(not yet recorded for this chain)")
        r += 1

    ws.column_dimensions["A"].width = 16
    ws.column_dimensions["B"].width = 14
    ws.column_dimensions["C"].width = 26
    ws.column_dimensions["D"].width = 100


@app.get("/preauth/{claim_id}/current-trail/export/excel", tags=["PreAuth"])
async def export_preauth_current_chain_excel(
    claim_id: str,
    correlation_id: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Downloads an Excel workbook for a single PreAuth chain execution (same
    scoping as /current-trail) instead of the full-history dump
    /preauth/export/excel produces - one sheet with the
    JSON_REQUEST/FHIR_REQUEST/FHIR_RESPONSE/JSON_RESPONSE payloads for just
    that one correlation_id (the most recent one if not given explicitly).
    """
    try:
        resolved_correlation_id, chain_entries = _resolve_current_chain_entries(db, claim_id, correlation_id)

        wb = Workbook()
        wb.remove(wb.active)
        _write_current_chain_sheet(wb, claim_id, resolved_correlation_id, chain_entries)

        buffer = BytesIO()
        wb.save(buffer)
        buffer.seek(0)

        return StreamingResponse(
            buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename=preauth_{claim_id}_current_chain.xlsx"}
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error exporting current PreAuth chain excel: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to export current chain excel: {str(e)}"
        )


def _export_current_chain_segment_excel(
    db: Session, claim_id: str, correlation_id: Optional[str],
    log_table: str, stage_order: list, sheet_title: str, filename_suffix: str
):
    resolved_correlation_id, chain_entries = _resolve_current_chain_entries(db, claim_id, correlation_id)
    segment_entries = [e for e in chain_entries if e["log_table"] == log_table]

    wb = Workbook()
    wb.remove(wb.active)
    _write_current_chain_sheet(
        wb, claim_id, resolved_correlation_id, segment_entries,
        sheet_title=sheet_title, stage_order=stage_order
    )

    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)

    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=preauth_{claim_id}_current_chain_{filename_suffix}.xlsx"}
    )


@app.get("/preauth/{claim_id}/current-trail/request/export/excel", tags=["PreAuth"])
async def export_preauth_current_chain_request_excel(
    claim_id: str,
    correlation_id: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Like /current-trail/export/excel but request-direction (JSON→FHIR) only
    - just the JSON_REQUEST/FHIR_REQUEST stages for the current chain, no
    response data. Meant for the JSON → FHIR node's Download button.
    """
    try:
        return _export_current_chain_segment_excel(
            db, claim_id, correlation_id,
            log_table="REQUEST", stage_order=CHAIN_STAGES_REQUEST,
            sheet_title="Current Chain (Request)", filename_suffix="request"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error exporting current PreAuth chain request excel: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to export current chain request excel: {str(e)}"
        )


@app.get("/preauth/{claim_id}/current-trail/response/export/excel", tags=["PreAuth"])
async def export_preauth_current_chain_response_excel(
    claim_id: str,
    correlation_id: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Like /current-trail/export/excel but response-direction (FHIR→JSON) only
    - just the FHIR_RESPONSE/JSON_RESPONSE stages for the current chain, no
    request data. Meant for the FHIR → JSON node's Download button.
    """
    try:
        return _export_current_chain_segment_excel(
            db, claim_id, correlation_id,
            log_table="RESPONSE", stage_order=CHAIN_STAGES_RESPONSE,
            sheet_title="Current Chain (Response)", filename_suffix="response"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error exporting current PreAuth chain response excel: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to export current chain response excel: {str(e)}"
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
