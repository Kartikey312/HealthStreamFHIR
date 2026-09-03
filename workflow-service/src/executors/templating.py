"""
Minimal, non-Turing-complete template resolver for node config values.
"$input.a.b" resolves to a dotted-path lookup into the upstream node's output;
anything else is treated as a literal. This is the one bounded exception to
"no code/expression node in v1" - just enough to make Kafka keys and DB
params usable.
"""
from typing import Any, Dict, Optional


def resolve(value: Any, input_data: Dict[str, Any]) -> Any:
    # Config panel fields are free-text - tolerate accidental leading/trailing
    # whitespace from typing or pasting rather than silently treating
    # " $input.x" as a literal string.
    stripped = value.strip() if isinstance(value, str) else value
    if isinstance(stripped, str) and stripped.startswith("$input"):
        path = stripped[len("$input"):].lstrip(".")
        if not path:
            return input_data
        current: Any = input_data
        for part in path.split("."):
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
        return current
    return value


def resolve_dict(d: Optional[Dict[str, Any]], input_data: Dict[str, Any]) -> Dict[str, Any]:
    if not d:
        return {}
    return {k: resolve(v, input_data) for k, v in d.items()}
