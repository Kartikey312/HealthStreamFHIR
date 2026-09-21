import asyncio
from typing import Dict, Any
import httpx
from .context import ExecutionContext
from .templating import resolve_dict, resolve_url


async def execute(config: Dict[str, Any], input_data: Dict[str, Any], ctx: ExecutionContext) -> Dict[str, Any]:
    method = config.get("method", "GET")
    url = config.get("url")
    if not url:
        raise ValueError("http_request node requires a url")

    url = resolve_url(url, input_data, ctx.outputs)

    # The eligibility endpoints answer 202 and finish over Kafka, so a follow-up
    # GET needs a moment for the worker - capped so a typo can't hang a run.
    delay = float(config.get("delaySeconds") or 0)
    if delay > 0:
        await asyncio.sleep(min(delay, 30))

    headers = resolve_dict(config.get("headers"), input_data, ctx.outputs)
    body_mode = config.get("bodyMode", "passthrough")
    json_body = input_data if body_mode == "passthrough" else (config.get("body") or {})

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.request(
            method,
            url,
            headers=headers,
            json=json_body if method in ("POST", "PUT", "PATCH") else None,
            params=json_body if method in ("GET", "DELETE") and isinstance(json_body, dict) else None
        )

    try:
        return {"status_code": response.status_code, "body": response.json()}
    except ValueError:
        return {"status_code": response.status_code, "body": response.text}
