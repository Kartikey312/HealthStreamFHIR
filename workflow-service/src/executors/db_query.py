from typing import Dict, Any
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../'))
from shared import SessionLocal
from sqlalchemy import text

from .context import ExecutionContext
from .templating import resolve_dict


async def execute(config: Dict[str, Any], input_data: Dict[str, Any], ctx: ExecutionContext) -> Dict[str, Any]:
    """
    Calls a stored procedure with bound parameters only - no free-form SQL in v1.
    Mirrors the exact db.execute(text("CALL ..."), {...}) pattern already proven
    in integration-api's /preauth/{claim_id} endpoint.
    """
    procedure = config.get("procedure")
    if not procedure:
        raise ValueError("db_query node requires a procedure name")

    raw_params = config.get("params") or {}
    # Trim whitespace on keys - the config panel's keyvalue rows are free-text
    # inputs, and a stray leading/trailing space (easy to introduce by typing
    # or pasting) would otherwise silently produce a bind name that doesn't
    # match anything, breaking the whole call with a confusing SQL error.
    trimmed_params = {str(k).strip(): v for k, v in raw_params.items() if str(k).strip()}
    params = resolve_dict(trimmed_params, input_data)
    call_sql = f"CALL {procedure}(" + ", ".join(f":{k}" for k in params.keys()) + ")"

    # This runs inside a background asyncio task, not a request, so there's no
    # FastAPI Depends(get_db) - open and close a session per call instead.
    db = SessionLocal()
    try:
        result = db.execute(text(call_sql), params)
        row = result.fetchone()
        db.commit()
    finally:
        db.close()

    if not row or row[0] is None:
        return {}

    value = row[0]
    if isinstance(value, (str, bytes)):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return {"result": value}
    return {"result": value}
