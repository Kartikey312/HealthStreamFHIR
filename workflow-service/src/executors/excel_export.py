from typing import Dict, Any
import httpx
from .context import ExecutionContext
from .templating import resolve


async def execute(config: Dict[str, Any], input_data: Dict[str, Any], ctx: ExecutionContext) -> Dict[str, Any]:
    """
    Confirms an Excel export endpoint is reachable and generating a file.
    Does NOT return the file's binary content as the node's output - run
    step outputs are stored as JSON, and stuffing spreadsheet bytes in there
    would bloat/break that. The actual download happens from the browser via
    the Download button in this node's config panel (ConfigPanel.tsx), which
    just links straight to the same URL - a real download is a browser
    action, not something a Kafka-driven backend node can hand back as data.
    """
    url = resolve(config.get("url"), input_data)
    if not url:
        raise ValueError("excel_export node requires a url")

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(url)

    if response.status_code != 200:
        raise ValueError(f"Export endpoint returned {response.status_code}: {response.text[:200]}")

    return {
        "status": "ready",
        "url": url,
        "content_type": response.headers.get("content-type"),
        "size_bytes": len(response.content)
    }
