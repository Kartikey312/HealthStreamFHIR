"""
Minimal, non-Turing-complete template resolver for node config values.
"$input.a.b" resolves to a dotted-path lookup into the upstream node's output;
anything else is treated as a literal. This is the one bounded exception to
"no code/expression node in v1" - just enough to make Kafka keys and DB
params usable.
"""
from typing import Any, Dict, Optional


def _walk(current: Any, path: str) -> Any:
    for part in path.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list) and part.isdigit() and int(part) < len(current):
            current = current[int(part)]
        else:
            return None
    return current


def resolve(value: Any, input_data: Dict[str, Any], outputs: Optional[Dict[str, Any]] = None) -> Any:
    """
    "$input.a.b" looks up a dotted path in the upstream node's output;
    "$node.<node_id>.a.b" does the same in an EARLIER node's output in this
    run (any node that has already finished, not just the previous one).
    List items are addressed by index ("claim.0.id"). Anything else is literal.
    """
    # Config panel fields are free-text - tolerate accidental leading/trailing
    # whitespace from typing or pasting rather than silently treating
    # " $input.x" as a literal string.
    stripped = value.strip() if isinstance(value, str) else value
    if isinstance(stripped, str) and stripped.startswith("$input"):
        path = stripped[len("$input"):].lstrip(".")
        return input_data if not path else _walk(input_data, path)
    if isinstance(stripped, str) and stripped.startswith("$node."):
        node_id, _, path = stripped[len("$node."):].partition(".")
        node_output = (outputs or {}).get(node_id)
        if node_output is None:
            return None
        return node_output if not path else _walk(node_output, path)
    return value


def resolve_dict(d: Optional[Dict[str, Any]], input_data: Dict[str, Any],
                 outputs: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not d:
        return {}
    return {k: resolve(v, input_data, outputs) for k, v in d.items()}


def resolve_url(url: str, input_data: Dict[str, Any], outputs: Optional[Dict[str, Any]] = None) -> str:
    """
    Resolve "$input..." / "$node.<id>..." path segments inside a URL, e.g.
    ".../requests/$node.post_request.body.correlation_id" -> ".../requests/<the id>".
    Only whole "/"-separated segments are substituted; the rest is literal.
    """
    return "/".join(
        str(resolve(part, input_data, outputs)) if part.strip().startswith(("$input", "$node.")) else part
        for part in url.split("/")
    )
