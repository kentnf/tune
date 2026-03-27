"""Plan compiler — validates, normalises, and enriches a typed plan.

Takes raw plan steps (from LLM or user) and produces compiled steps where:
  - All required fields are present (step_key, step_type, depends_on, params)
  - Param defaults are filled in from each step type's JSON-Schema
  - display_name is set from the registry if not provided
  - Each step carries a ``_compiled: True`` marker

The compiler runs AFTER the basic structural validator (validate_plan) and
adds two additional checks:

  1. Cycle detection — the depends_on graph must be a DAG.
  2. Output→input type compatibility — warns when a step depends on an upstream
     step whose output types don't overlap with the downstream step's required
     input types.  This is advisory only; runtime binding may still succeed via
     KnownPath or FileRun records.

Usage::

    from tune.core.workflow.plan_compiler import compile_plan

    result = compile_plan(raw_steps)
    if not result.ok:
        # result.errors contains blocking issues
        raise PlanError(result.errors)
    compiled_steps = result.compiled_steps
    # result.warnings contains non-blocking notices
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public result type
# ---------------------------------------------------------------------------


@dataclass
class CompileResult:
    ok: bool
    compiled_steps: list[dict] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def compile_plan(steps: list[dict]) -> CompileResult:
    """Validate, normalise, and compile typed plan steps.

    Returns a ``CompileResult``.  When ``ok=False`` the ``errors`` list
    explains why compilation failed and ``compiled_steps`` is empty.
    """
    if not steps:
        return CompileResult(ok=False, errors=["Plan has no steps"])

    errors: list[str] = []
    warnings: list[str] = []

    # --- Pass 1: basic structural validation (step_type in registry, unique
    #     step_key, params schema, depends_on references) ---
    from tune.core.workflow.plan_validator import validate_plan

    structural_errors = validate_plan(steps)
    errors.extend(structural_errors)

    # Don't continue to cycle/type checks if structure is broken
    if errors:
        return CompileResult(ok=False, errors=errors)

    # --- Pass 2: cycle detection ---
    cycle_errors = _detect_cycles(steps)
    errors.extend(cycle_errors)

    # --- Pass 3: output→input type compatibility (warnings only) ---
    compat_warnings = _check_type_compatibility(steps)
    warnings.extend(compat_warnings)

    if errors:
        return CompileResult(ok=False, errors=errors, warnings=warnings)

    # --- Compile: normalise and enrich each step ---
    compiled = [_compile_step(step) for step in steps]

    log.debug("compile_plan: compiled %d steps (%d warnings)", len(compiled), len(warnings))
    return CompileResult(ok=True, compiled_steps=compiled, warnings=warnings)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _detect_cycles(steps: list[dict]) -> list[str]:
    """Return error strings if the depends_on graph contains a cycle.

    Uses Kahn's algorithm (topological sort).  If the number of processed
    nodes is less than the total, a cycle exists.
    """
    key_map = {s.get("step_key", ""): s for s in steps if s.get("step_key")}

    # Explicit self-loop check (Kahn's algorithm would miss these)
    for step in steps:
        key = step.get("step_key", "")
        if key and key in (step.get("depends_on") or []):
            return [
                f"Plan contains a dependency cycle involving steps: ['{key}']. "
                "All steps must form a directed acyclic graph (DAG)."
            ]

    # in_degree[k] = number of steps that k depends on (within this plan)
    in_degree: dict[str, int] = {k: 0 for k in key_map}
    # adj[k] = list of step_keys that depend on k (reverse edges for Kahn)
    adj: dict[str, list[str]] = {k: [] for k in key_map}

    for step in steps:
        key = step.get("step_key", "")
        for dep in step.get("depends_on") or []:
            if dep in key_map and dep != key:
                adj[dep].append(key)
                in_degree[key] = in_degree.get(key, 0) + 1

    queue = [k for k, d in in_degree.items() if d == 0]
    processed = 0
    while queue:
        node = queue.pop(0)
        processed += 1
        for neighbor in adj.get(node, []):
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if processed < len(key_map):
        # Find which step_keys are still in the cycle (non-zero in-degree)
        cycle_keys = [k for k, d in in_degree.items() if d > 0]
        return [
            f"Plan contains a dependency cycle involving steps: {cycle_keys}. "
            "All steps must form a directed acyclic graph (DAG)."
        ]
    return []


def _check_type_compatibility(steps: list[dict]) -> list[str]:
    """Return advisory warnings for output→input type mismatches.

    For each depends_on edge, checks whether the upstream step's output file
    types overlap with the downstream step's required input file types.  Emits
    a warning when there is no overlap.

    This is non-blocking because runtime binding can satisfy slots via
    KnownPath or FileRun records independently of the DAG edges.
    """
    from tune.core.registry import get_step_type
    from tune.core.binding.resolver import _file_matches_types

    warnings: list[str] = []
    step_map = {s.get("step_key", ""): s for s in steps}

    for step in steps:
        step_key = step.get("step_key", "")
        defn = get_step_type(step.get("step_type", ""))
        if defn is None:
            continue

        for dep_key in step.get("depends_on") or []:
            dep_step = step_map.get(dep_key)
            if not dep_step:
                continue
            dep_defn = get_step_type(dep_step.get("step_type", ""))
            if dep_defn is None:
                continue

            # Gather all output types from the upstream step
            dep_output_types: set[str] = set()
            for out_slot in dep_defn.output_slots:
                dep_output_types.update(out_slot.file_types)

            # Gather all required input types from the downstream step
            required_input_types: set[str] = set()
            for in_slot in defn.input_slots:
                if in_slot.required:
                    required_input_types.update(in_slot.file_types)

            # Wildcards always match; skip check
            if (
                not dep_output_types
                or not required_input_types
                or "*" in dep_output_types
                or "*" in required_input_types
            ):
                continue

            # Check if any upstream output type satisfies any required input type
            has_overlap = any(
                _file_matches_types(f"dummy.{ot}", list(required_input_types))
                for ot in dep_output_types
            )
            if not has_overlap:
                warnings.append(
                    f"Step '{step_key}' depends on '{dep_key}': upstream output types "
                    f"{sorted(dep_output_types)} may not satisfy required input types "
                    f"{sorted(required_input_types)}. "
                    "Binding may still succeed via KnownPath or FileRun at runtime."
                )

    return warnings


def _compile_step(step: dict) -> dict:
    """Normalise a single step dict: fill defaults and add ``_compiled: True``."""
    from tune.core.registry import get_step_type

    defn = get_step_type(step.get("step_type", ""))

    compiled = dict(step)
    compiled.pop("dynamic_spec", None)

    # Ensure all expected fields are present
    compiled.setdefault("depends_on", [])
    compiled.setdefault("params", {})

    # Fill display_name from registry if the LLM omitted it
    if not compiled.get("display_name") and defn:
        compiled["display_name"] = defn.display_name

    # Fill param defaults declared in the step type's JSON-Schema
    if defn and defn.params_schema:
        props = defn.params_schema.get("properties", {})
        for param_name, param_schema in props.items():
            if param_name not in compiled["params"] and "default" in param_schema:
                compiled["params"][param_name] = param_schema["default"]

    # Mark as compiled — executor uses this to skip re-compilation
    compiled["_compiled"] = True

    return compiled
