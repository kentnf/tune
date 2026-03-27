"""Plan validator — validates a typed plan JSON against the step type registry.

A typed plan is a list of step objects, each with:
  step_type, step_key, display_name, depends_on, params, supports_fan_out
"""
from __future__ import annotations

import logging

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Typed plan step schema (used in LLM prompt and validation)
# ---------------------------------------------------------------------------

TYPED_STEP_SCHEMA = {
    "type": "object",
    "properties": {
        "step_key":       {"type": "string"},
        "step_type":      {"type": "string"},
        "display_name":   {"type": "string"},
        "depends_on":     {"type": "array", "items": {"type": "string"}},
        "params":         {"type": "object"},
        "supports_fan_out": {"type": "boolean"},
        "dynamic_spec":   {"type": "object"},
    },
    "required": ["step_key", "step_type"],
}

TYPED_PLAN_SCHEMA = {
    "type": "object",
    "properties": {
        "short_name": {"type": "string"},
        "steps": {"type": "array", "items": TYPED_STEP_SCHEMA},
    },
    "required": ["short_name", "steps"],
}


def validate_plan(plan_steps: list[dict]) -> list[str]:
    """Validate typed plan steps against the registry. Returns list of error strings."""
    from tune.core.registry import get_step_type
    from tune.core.registry.dynamic_steps import materialize_dynamic_step_types

    errors: list[str] = []
    seen_keys: set[str] = set()

    errors.extend(materialize_dynamic_step_types(plan_steps))

    for i, step in enumerate(plan_steps):
        prefix = f"Step {i + 1} ('{step.get('step_key', '?')}')"

        # step_type must be in registry
        step_type = step.get("step_type", "")
        if not step_type:
            errors.append(f"{prefix}: missing step_type")
            continue

        defn = get_step_type(step_type)
        if defn is None:
            from tune.core.registry import all_step_types
            errors.append(
                f"{prefix}: unknown step_type '{step_type}'. "
                f"Valid types: {all_step_types()}"
            )
            continue

        # step_key must be unique
        key = step.get("step_key", "")
        if not key:
            errors.append(f"{prefix}: missing step_key")
        elif key in seen_keys:
            errors.append(f"{prefix}: duplicate step_key '{key}'")
        else:
            seen_keys.add(key)

        # depends_on must reference known step_keys (checked after all steps seen)
        # We do a second pass below for cross-reference validation.

        # Validate params against step type's params_schema
        params = step.get("params") or {}
        schema = defn.params_schema
        param_errors = _validate_params(params, schema, prefix)
        errors.extend(param_errors)

    # Second pass: validate depends_on references
    for step in plan_steps:
        for dep in step.get("depends_on") or []:
            if dep not in seen_keys:
                errors.append(
                    f"Step '{step.get('step_key')}' depends_on unknown key '{dep}'"
                )

    return errors


def _validate_params(params: dict, schema: dict, prefix: str) -> list[str]:
    """Minimal JSON Schema validation for params (type checks only)."""
    errors: list[str] = []
    props = schema.get("properties", {})
    required = schema.get("required", [])

    for req in required:
        if req not in params:
            errors.append(f"{prefix}: missing required param '{req}'")

    for key, value in params.items():
        if key not in props:
            continue  # Extra params are allowed
        expected_type = props[key].get("type")
        if expected_type and not _check_type(value, expected_type):
            errors.append(
                f"{prefix}: param '{key}' expected type {expected_type}, "
                f"got {type(value).__name__}"
            )
        enum = props[key].get("enum")
        if enum is not None and value not in enum:
            errors.append(
                f"{prefix}: param '{key}' value {value!r} not in allowed values {enum}"
            )

    return errors


def _check_type(value: object, type_name: str) -> bool:
    mapping = {
        "string": str,
        "integer": int,
        "number": (int, float),
        "boolean": bool,
        "array": list,
        "object": dict,
    }
    expected = mapping.get(type_name)
    if expected is None:
        return True  # Unknown type — skip
    return isinstance(value, expected)
