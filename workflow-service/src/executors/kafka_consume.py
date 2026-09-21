import asyncio
import json
import time
from typing import Any, Dict, Optional
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../'))
from aiokafka import AIOKafkaConsumer, TopicPartition
from shared.kafka_utils import KAFKA_BROKER

from .context import ExecutionContext
from .templating import resolve

LOOKBACK_SECONDS = 600  # how far back a run may look for the event its own request caused
MAX_TIMEOUT_SECONDS = 120


def _parse(raw: Optional[bytes]) -> Any:
    try:
        return json.loads(raw.decode("utf-8")) if raw else None
    except (ValueError, UnicodeDecodeError):
        return None  # old non-JSON / non-envelope messages on shared topics


async def execute(config: Dict[str, Any], input_data: Dict[str, Any], ctx: ExecutionContext) -> Dict[str, Any]:
    """
    Waits for the Kafka event a service published as a result of an earlier
    step (e.g. the event the POST endpoint caused) and passes its content on -
    it reads, it does not publish. Matches the Envelope's event_type and
    correlation_id; only recent messages are considered. Output is the
    event's payload, or the whole Envelope when Output = envelope.
    """
    topic = config.get("topic")
    if not topic:
        raise ValueError("kafka_consume node requires a topic")
    event_type = (config.get("eventType") or "").strip() or None
    correlation_id = resolve(config.get("correlationId"), input_data, ctx.outputs)
    correlation_id = str(correlation_id) if correlation_id not in (None, "") else None
    if not event_type and not correlation_id:
        raise ValueError("kafka_consume node needs an event type or a correlation id to match, or it would return the first message it sees")
    timeout = min(float(config.get("timeoutSeconds") or 30), MAX_TIMEOUT_SECONDS)

    consumer = AIOKafkaConsumer(bootstrap_servers=KAFKA_BROKER, group_id=None, enable_auto_commit=False)
    await consumer.start()
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            await consumer.topics()  # loads metadata
            partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise ValueError(f"Kafka topic '{topic}' does not exist yet")
        assigned = [TopicPartition(topic, p) for p in sorted(partitions)]
        consumer.assign(assigned)

        since_ms = int((time.time() - LOOKBACK_SECONDS) * 1000)
        offsets = await consumer.offsets_for_times({tp: since_ms for tp in assigned})
        for tp in assigned:
            if offsets.get(tp):
                consumer.seek(tp, offsets[tp].offset)
            else:
                consumer.seek_to_end(tp)  # nothing recent on this partition - only wait for new messages

        async def find() -> Dict[str, Any]:
            while True:
                message = await consumer.getone()
                envelope = _parse(message.value)
                if not isinstance(envelope, dict):
                    continue
                if event_type and envelope.get("event_type") != event_type:
                    continue
                if correlation_id and envelope.get("correlation_id") != correlation_id:
                    continue
                return envelope

        try:
            envelope = await asyncio.wait_for(find(), timeout)
        except asyncio.TimeoutError:
            raise ValueError(
                f"No event on '{topic}' matched (event_type={event_type}, correlation_id={correlation_id}) within {timeout:g}s")
    finally:
        await consumer.stop()

    if (config.get("output") or "payload") == "envelope":
        return envelope
    return envelope.get("payload") or {}
