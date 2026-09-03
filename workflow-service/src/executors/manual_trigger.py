from typing import Dict, Any
from .context import ExecutionContext


async def execute(config: Dict[str, Any], input_data: Dict[str, Any], ctx: ExecutionContext) -> Dict[str, Any]:
    """Returns the run-level trigger_input if one was supplied, else the node's configured seed data"""
    if input_data:
        return input_data
    return config.get("data") or {}
