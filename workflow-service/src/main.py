"""
Workflow Service - FastAPI
CRUD for saved workflow graphs, plus the execution engine and run history API.
"""
import logging
import json
import asyncio
from datetime import datetime
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
import sys
import os

# Add shared module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from shared import (
    get_db, create_kafka_producer, TOPICS,
    Workflow, WorkflowRun, WorkflowRunStep, Base, engine
)

from .schemas import (
    WorkflowIn, WorkflowSummary, WorkflowOut, RunRequest, RunAccepted,
    WorkflowRunOut, WorkflowRunStepOut, WorkflowRunSummary
)
from .node_registry import NODE_TYPES
from .engine import run_workflow, validate_graph, GraphValidationError
from .executors import ExecutionContext

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Workflow Service",
    description="n8n-style workflow builder for JSON2FHIR",
    version="1.0.0"
)

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

# Global producer / execution context
producer = None
ctx: ExecutionContext = None


@app.on_event("startup")
async def startup_event():
    global producer, ctx
    try:
        producer = await create_kafka_producer()
        ctx = ExecutionContext(producer)
        logger.info("🚀 Workflow Service started")
    except Exception as e:
        logger.error(f"❌ Failed to start producer: {e}")
        raise


@app.on_event("shutdown")
async def shutdown_event():
    global producer
    if producer:
        await producer.stop()
        logger.info("🛑 Workflow Service stopped")


@app.get("/health", tags=["Health"])
async def health_check():
    return {"status": "healthy", "service": "Workflow Service", "version": "1.0.0"}


@app.get("/node-types", tags=["Registry"])
async def get_node_types():
    return {"node_types": NODE_TYPES}


@app.get("/topics", tags=["Registry"])
async def get_topics():
    return {"topics": TOPICS}


def _workflow_to_out(w: Workflow) -> WorkflowOut:
    return WorkflowOut(
        id=w.id, name=w.name, description=w.description,
        definition=json.loads(w.definition),
        created_at=w.created_at, updated_at=w.updated_at
    )


@app.post("/workflows", response_model=WorkflowOut, status_code=201, tags=["Workflows"])
async def create_workflow(body: WorkflowIn, db: Session = Depends(get_db)):
    workflow = Workflow(
        name=body.name,
        description=body.description,
        definition=json.dumps(body.definition)
    )
    db.add(workflow)
    db.commit()
    db.refresh(workflow)
    logger.info(f"✅ Created workflow: {workflow.id} ({workflow.name})")
    return _workflow_to_out(workflow)


@app.get("/workflows", tags=["Workflows"])
async def list_workflows(limit: int = 50, offset: int = 0, db: Session = Depends(get_db)):
    rows = db.query(Workflow).order_by(Workflow.updated_at.desc()).offset(offset).limit(limit).all()
    return {"workflows": [WorkflowSummary.model_validate(w) for w in rows]}


@app.get("/workflows/{workflow_id}", response_model=WorkflowOut, tags=["Workflows"])
async def get_workflow(workflow_id: int, db: Session = Depends(get_db)):
    workflow = db.query(Workflow).filter(Workflow.id == workflow_id).first()
    if not workflow:
        raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")
    return _workflow_to_out(workflow)


@app.put("/workflows/{workflow_id}", response_model=WorkflowOut, tags=["Workflows"])
async def update_workflow(workflow_id: int, body: WorkflowIn, db: Session = Depends(get_db)):
    workflow = db.query(Workflow).filter(Workflow.id == workflow_id).first()
    if not workflow:
        raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")
    workflow.name = body.name
    workflow.description = body.description
    workflow.definition = json.dumps(body.definition)
    db.commit()
    db.refresh(workflow)
    logger.info(f"✅ Updated workflow: {workflow.id}")
    return _workflow_to_out(workflow)


@app.delete("/workflows/{workflow_id}", status_code=204, tags=["Workflows"])
async def delete_workflow(workflow_id: int, db: Session = Depends(get_db)):
    workflow = db.query(Workflow).filter(Workflow.id == workflow_id).first()
    if not workflow:
        raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")
    try:
        db.delete(workflow)
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail="Cannot delete a workflow with run history"
        )
    logger.info(f"✅ Deleted workflow: {workflow_id}")


@app.post("/workflows/{workflow_id}/run", response_model=RunAccepted, status_code=202, tags=["Runs"])
async def run_workflow_endpoint(workflow_id: int, body: RunRequest, db: Session = Depends(get_db)):
    workflow = db.query(Workflow).filter(Workflow.id == workflow_id).first()
    if not workflow:
        raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")

    definition = json.loads(workflow.definition)

    try:
        validate_graph(definition)
    except GraphValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    trigger_input = body.trigger_input or {}

    run = WorkflowRun(
        workflow_id=workflow_id, status="RUNNING",
        trigger_input=json.dumps(trigger_input), started_at=datetime.utcnow()
    )
    db.add(run)
    db.commit()
    db.refresh(run)

    logger.info(f"🚀 Starting run {run.id} for workflow {workflow_id}")
    asyncio.create_task(run_workflow(run.id, definition, trigger_input, ctx))

    return RunAccepted(
        run_id=run.id, workflow_id=workflow_id, status=run.status, started_at=run.started_at
    )


@app.get("/runs/{run_id}", response_model=WorkflowRunOut, tags=["Runs"])
async def get_run(run_id: int, db: Session = Depends(get_db)):
    run = db.query(WorkflowRun).filter(WorkflowRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    steps = db.query(WorkflowRunStep).filter(WorkflowRunStep.run_id == run_id).order_by(WorkflowRunStep.id).all()

    return WorkflowRunOut(
        id=run.id, workflow_id=run.workflow_id, status=run.status,
        trigger_input=json.loads(run.trigger_input) if run.trigger_input else None,
        error=run.error, started_at=run.started_at, finished_at=run.finished_at,
        steps=[
            WorkflowRunStepOut(
                node_id=s.node_id, node_type=s.node_type, status=s.status,
                input=json.loads(s.input) if s.input else None,
                output=json.loads(s.output) if s.output else None,
                error=s.error, started_at=s.started_at, finished_at=s.finished_at
            )
            for s in steps
        ]
    )


@app.get("/workflows/{workflow_id}/runs", tags=["Runs"])
async def list_runs(workflow_id: int, db: Session = Depends(get_db)):
    workflow = db.query(Workflow).filter(Workflow.id == workflow_id).first()
    if not workflow:
        raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")
    runs = db.query(WorkflowRun).filter(WorkflowRun.workflow_id == workflow_id).order_by(WorkflowRun.started_at.desc()).all()
    return {"runs": [WorkflowRunSummary.model_validate(r) for r in runs]}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8002,
        log_level="info"
    )
