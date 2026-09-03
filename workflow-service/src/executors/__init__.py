from .context import ExecutionContext
from . import manual_trigger, http_request, kafka_publish, db_query, fhir_transform, view

NODE_EXECUTORS = {
    "manual_trigger": manual_trigger.execute,
    "http_request": http_request.execute,
    "kafka_publish": kafka_publish.execute,
    "db_query": db_query.execute,
    "json_to_fhir": fhir_transform.execute,
    "fhir_to_json": fhir_transform.execute,
    # Legacy key - no longer offered in the palette (split into json_to_fhir /
    # fhir_to_json above), kept so previously-saved workflows still execute.
    "fhir_transform": fhir_transform.execute,
    "view": view.execute,
}

__all__ = ["NODE_EXECUTORS", "ExecutionContext"]
