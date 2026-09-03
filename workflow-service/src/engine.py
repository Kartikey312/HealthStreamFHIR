"""
Workflow execution engine: validates the graph (single entry node, no cycles),
then runs it in topological order, dispatching each node to its executor and
persisting a WorkflowRunStep row per node as it completes.
"""
import json
import logging
from collections import deque
from datetime import datetime
from typing import Dict, Any, List, Tuple

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))
from shared import SessionLocal, WorkflowRun, WorkflowRunStep

from .executors import NODE_EXECUTORS, ExecutionContext

logger = logging.getLogger(__name__)


class GraphValidationError(Exception):
    pass


def _build_adjacency(nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]) -> Tuple[Dict, Dict]:
    node_ids = {n["id"] for n in nodes}
    incoming: Dict[str, List[str]] = {nid: [] for nid in node_ids}
    outgoing: Dict[str, List[str]] = {nid: [] for nid in node_ids}
    for edge in edges:
        src, tgt = edge.get("source"), edge.get("target")
        if src not in node_ids or tgt not in node_ids:
            raise GraphValidationError(f"Edge references unknown node: {src} -> {tgt}")
        outgoing[src].append(tgt)
        incoming[tgt].append(src)
    return incoming, outgoing


def validate_graph(definition: Dict[str, Any]) -> List[str]:
    """
    Validates the graph and returns node ids in topological order. Raises
    GraphValidationError on a cycle, no/multiple entry nodes, or an entry
    node that isn't a Manual Trigger.
    """
    nodes = definition.get("nodes", []) or []
    edges = definition.get("edges", []) or []
    if not nodes:
        raise GraphValidationError("Workflow has no nodes")

    node_by_id = {n["id"]: n for n in nodes}
    incoming, outgoing = _build_adjacency(nodes, edges)

    entry_nodes = [nid for nid in node_by_id if not incoming[nid]]
    if len(entry_nodes) != 1:
        raise GraphValidationError(
            f"Workflow must have exactly one entry node (indegree 0), found {len(entry_nodes)}"
        )
    if node_by_id[entry_nodes[0]]["type"] != "manual_trigger":
        raise GraphValidationError("The entry node must be a Manual Trigger node")

    indegree = {nid: len(incoming[nid]) for nid in node_by_id}
    queue = deque([nid for nid in node_by_id if indegree[nid] == 0])
    topo_order: List[str] = []
    while queue:
        nid = queue.popleft()
        topo_order.append(nid)
        for tgt in outgoing[nid]:
            indegree[tgt] -= 1
            if indegree[tgt] == 0:
                queue.append(tgt)

    if len(topo_order) != len(node_by_id):
        raise GraphValidationError("Workflow graph contains a cycle")

    return topo_order


def _create_step(db, run_id: int, node_id: str, node_type: str, input_data: Dict[str, Any]) -> int:
    step = WorkflowRunStep(
        run_id=run_id, node_id=node_id, node_type=node_type, status="RUNNING",
        input=json.dumps(input_data), started_at=datetime.utcnow()
    )
    db.add(step)
    db.commit()
    db.refresh(step)
    return step.id


def _finish_step(db, step_id: int, status: str, output_data: Dict[str, Any] = None, error: str = None):
    step = db.query(WorkflowRunStep).filter(WorkflowRunStep.id == step_id).first()
    step.status = status
    if output_data is not None:
        step.output = json.dumps(output_data)
    if error is not None:
        step.error = error
    step.finished_at = datetime.utcnow()
    db.commit()


def _skip_step(db, run_id: int, node_id: str, node_type: str):
    now = datetime.utcnow()
    step = WorkflowRunStep(
        run_id=run_id, node_id=node_id, node_type=node_type, status="SKIPPED",
        started_at=now, finished_at=now
    )
    db.add(step)
    db.commit()


async def run_workflow(run_id: int, definition: Dict[str, Any], trigger_input: Dict[str, Any], ctx: ExecutionContext):
    """Executes a saved workflow graph, persisting run + per-node step results as it goes."""
    db = SessionLocal()
    try:
        nodes = definition.get("nodes", []) or []
        node_by_id = {n["id"]: n for n in nodes}

        try:
            topo_order = validate_graph(definition)
        except GraphValidationError as e:
            run = db.query(WorkflowRun).filter(WorkflowRun.id == run_id).first()
            run.status = "FAILED"
            run.error = str(e)
            run.finished_at = datetime.utcnow()
            db.commit()
            return

        incoming, _outgoing = _build_adjacency(nodes, definition.get("edges", []) or [])

        outputs: Dict[str, Dict[str, Any]] = {}
        failed_ancestor = set()
        any_failed = False

        for nid in topo_order:
            node = node_by_id[nid]
            node_type = node["type"]
            preds = incoming[nid]

            if any(p in failed_ancestor for p in preds):
                _skip_step(db, run_id, nid, node_type)
                failed_ancestor.add(nid)
                continue

            if not preds:
                input_data = (trigger_input if node_type == "manual_trigger" else {}) or {}
            elif len(preds) == 1:
                input_data = outputs.get(preds[0]) or {}
            else:
                # v1 fan-in: shallow dict union, last edge wins on key collision.
                # No join/merge node in v1 - documented limitation, not silent behavior.
                input_data = {}
                for p in preds:
                    input_data.update(outputs.get(p) or {})

            step_id = _create_step(db, run_id, nid, node_type, input_data)

            try:
                output = await NODE_EXECUTORS[node_type](node.get("config") or {}, input_data, ctx)
                outputs[nid] = output
                _finish_step(db, step_id, "SUCCESS", output_data=output)
            except Exception as e:
                logger.error(f"❌ Node {nid} ({node_type}) failed: {e}", exc_info=True)
                failed_ancestor.add(nid)
                any_failed = True
                _finish_step(db, step_id, "FAILED", error=str(e))

        run = db.query(WorkflowRun).filter(WorkflowRun.id == run_id).first()
        run.status = "FAILED" if any_failed else "SUCCESS"
        run.finished_at = datetime.utcnow()
        db.commit()

    except Exception as e:
        logger.error(f"❌ Run {run_id} failed unexpectedly: {e}", exc_info=True)
        run = db.query(WorkflowRun).filter(WorkflowRun.id == run_id).first()
        if run:
            run.status = "FAILED"
            run.error = str(e)
            run.finished_at = datetime.utcnow()
            db.commit()
    finally:
        db.close()
