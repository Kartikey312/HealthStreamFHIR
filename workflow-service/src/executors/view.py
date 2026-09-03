from typing import Dict, Any
from .context import ExecutionContext


async def execute(config: Dict[str, Any], input_data: Dict[str, Any], ctx: ExecutionContext) -> Dict[str, Any]:
    """No-op: its entire value is that the WorkflowRunStep row records input/output here."""
    return input_data
