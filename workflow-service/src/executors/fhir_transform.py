from typing import Dict, Any
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../'))
from shared import (
    json_to_fhir_patient, fhir_to_json_response, validate_fhir_patient,
    fhir_to_json_request, json_to_fhir_response, json_to_fhir_claim
)

from .context import ExecutionContext
from .templating import resolve


FUNCTION_REGISTRY = {
    "json_to_fhir_patient": json_to_fhir_patient,
    "fhir_to_json_response": fhir_to_json_response,
    "validate_fhir_patient": validate_fhir_patient,
    "fhir_to_json_request": fhir_to_json_request,
    "json_to_fhir_response": json_to_fhir_response,
    "json_to_fhir_claim": json_to_fhir_claim,
}


async def execute(config: Dict[str, Any], input_data: Dict[str, Any], ctx: ExecutionContext) -> Dict[str, Any]:
    function_name = config.get("function")
    fn = FUNCTION_REGISTRY.get(function_name)
    if not fn:
        raise ValueError(f"Unknown FHIR transform function: {function_name}")

    if function_name == "fhir_to_json_response":
        # The one function that takes an extra positional arg beyond a single dict.
        field = config.get("originalPatientIdField", "patientIdentifier")
        original_patient_id = resolve(f"$input.{field}", input_data)
        result = fn(input_data, original_patient_id)
    else:
        result = fn(input_data)

    if function_name == "validate_fhir_patient":
        is_valid, errors = result
        return {"is_valid": is_valid, "errors": errors}

    return result
