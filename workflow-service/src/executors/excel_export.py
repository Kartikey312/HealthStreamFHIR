from typing import Dict, Any
from .context import ExecutionContext


async def execute(config: Dict[str, Any], input_data: Dict[str, Any], ctx: ExecutionContext) -> Dict[str, Any]:
    """
    No-op pass-through, same as the View node - its entire value is that the
    WorkflowRunStep row records input/output at this point in the graph. The
    Download button in this node's config panel then reads the PRECEDING
    node's step from this same run (via GET /runs/{run_id}/nodes/{node_id}/
    export/excel in main.py) to build the Excel file - not this node's own
    input/output, which would just be a duplicate of the upstream node's
    output on both sides.
    """
    return input_data
