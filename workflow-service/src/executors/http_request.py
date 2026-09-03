from typing import Dict, Any
import httpx
from .context import ExecutionContext
from .templating import resolve_dict


async def execute(config: Dict[str, Any], input_data: Dict[str, Any], ctx: ExecutionContext) -> Dict[str, Any]:
    method = config.get("method", "GET")
    url = config.get("url")
    if not url:
        raise ValueError("http_request node requires a url")

    headers = resolve_dict(config.get("headers"), input_data)
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
