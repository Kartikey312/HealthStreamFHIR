from typing import Dict, Any
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../'))
from shared import send_kafka_message

from .context import ExecutionContext
from .templating import resolve


async def execute(config: Dict[str, Any], input_data: Dict[str, Any], ctx: ExecutionContext) -> Dict[str, Any]:
    topic = config.get("topic")
    if not topic:
        raise ValueError("kafka_publish node requires a topic")

    key = resolve(config.get("key"), input_data)
    key = str(key) if key is not None else None
    await send_kafka_message(ctx.producer, topic, key, input_data)

    # Publishing is a side effect, not a transform - pass the data through unchanged
    # so downstream nodes still see it.
    return input_data
