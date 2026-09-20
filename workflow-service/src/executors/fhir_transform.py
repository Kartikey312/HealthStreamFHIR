from typing import Dict, Any
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../'))
from shared import (
    to_fhir_eligibility_bundle, to_eligibility_response_json, validate_fhir_patient,
    fhir_to_json_request, json_to_fhir_response, json_to_fhir_claim,
    fhir_to_json_claim_response
)

from .context import ExecutionContext


FUNCTION_REGISTRY = {
    "to_fhir_eligibility_bundle": to_fhir_eligibility_bundle,
    "to_eligibility_response_json": to_eligibility_response_json,
    "validate_fhir_patient": validate_fhir_patient,
    "fhir_to_json_request": fhir_to_json_request,
    "json_to_fhir_response": json_to_fhir_response,
    "json_to_fhir_claim": json_to_fhir_claim,
    "fhir_to_json_claim_response": fhir_to_json_claim_response,
}


async def execute(config: Dict[str, Any], input_data: Dict[str, Any], ctx: ExecutionContext) -> Dict[str, Any]:
    function_name = config.get("function")
    fn = FUNCTION_REGISTRY.get(function_name)
    if not fn:
        raise ValueError(f"Unknown FHIR transform function: {function_name}")

    result = fn(input_data)

    if function_name == "validate_fhir_patient":
        is_valid, errors = result
        return {"is_valid": is_valid, "errors": errors}

    return result
