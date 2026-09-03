"""
Pydantic schemas for the workflow builder (workflow-service only - not shared,
since these are only ever consumed by this one service, unlike shared/schemas.py)
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


class WorkflowIn(BaseModel):
    """Body for creating/replacing a workflow"""
    name: str
    description: Optional[str] = None
    definition: Dict[str, Any] = Field(default_factory=lambda: {"nodes": [], "edges": []})


class WorkflowSummary(BaseModel):
    """List view - omits definition to keep the payload small"""
    id: int
    name: str
    description: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class WorkflowOut(WorkflowSummary):
    """Full workflow, including its node graph"""
    definition: Dict[str, Any]


class RunRequest(BaseModel):
    """Body for POST /workflows/{id}/run"""
    trigger_input: Optional[Dict[str, Any]] = None


class RunAccepted(BaseModel):
    run_id: int
    workflow_id: int
    status: str
    started_at: datetime


class WorkflowRunStepOut(BaseModel):
    node_id: str
    node_type: str
    status: str
    input: Optional[Dict[str, Any]] = None
    output: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None


class WorkflowRunOut(BaseModel):
    id: int
    workflow_id: int
    status: str
    trigger_input: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    started_at: datetime
    finished_at: Optional[datetime] = None
    steps: List[WorkflowRunStepOut] = Field(default_factory=list)


class WorkflowRunSummary(BaseModel):
    id: int
    status: str
    started_at: datetime
    finished_at: Optional[datetime] = None

    class Config:
        from_attributes = True
