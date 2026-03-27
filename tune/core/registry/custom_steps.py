"""Runtime loader for declarative custom step specifications."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

log = logging.getLogger(__name__)

STEP_SPEC_DIR = ".tune/step_specs"

_LAST_SIGNATURE: tuple[tuple[str, int, int], ...] | None = None


def _spec_paths(analysis_dir: Path | None) -> list[Path]:
    if analysis_dir is None:
        return []
    spec_dir = analysis_dir / STEP_SPEC_DIR
    if not spec_dir.exists():
        return []
    return sorted(
        path
        for path in spec_dir.rglob("*")
        if path.is_file() and path.suffix.lower() in {".yaml", ".yml"}
    )


def _compute_signature(paths: list[Path]) -> tuple[tuple[str, int, int], ...]:
    signature: list[tuple[str, int, int]] = []
    for path in paths:
        stat = path.stat()
        signature.append((path.name, stat.st_mtime_ns, stat.st_size))
    return tuple(signature)


def _iter_step_payloads(path: Path) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    with path.open() as handle:
        documents = list(yaml.safe_load_all(handle))
    for document in documents:
        if not document:
            continue
        if isinstance(document, list):
            for item in document:
                if isinstance(item, dict):
                    payloads.append(item)
        elif isinstance(document, dict):
            payloads.append(document)
    return payloads


def _coerce_slot(slot_payload: dict[str, Any], steps_module) -> Any:
    return steps_module.SlotDefinition(
        name=str(slot_payload.get("name") or "").strip(),
        description=str(slot_payload.get("description") or "").strip(),
        file_types=[str(item) for item in slot_payload.get("file_types") or []],
        required=bool(slot_payload.get("required", True)),
        multiple=bool(slot_payload.get("multiple", False)),
        from_upstream_dir=bool(slot_payload.get("from_upstream_dir", False)),
        artifact_role=slot_payload.get("artifact_role"),
        accepted_roles=[str(item) for item in slot_payload.get("accepted_roles") or []],
        artifact_scope=str(slot_payload.get("artifact_scope") or "job_global"),
    )


def _coerce_repair_policy(payload: dict[str, Any], steps_module) -> Any:
    return steps_module.RepairPolicy(
        max_l1_retries=int(payload.get("max_l1_retries", 2)),
        max_l2_retries=int(payload.get("max_l2_retries", 1)),
        allow_l2_llm=bool(payload.get("allow_l2_llm", True)),
        l3_escalate=bool(payload.get("l3_escalate", True)),
    )


def _coerce_safety_policy(payload: dict[str, Any], steps_module) -> Any:
    return steps_module.SafetyPolicy(
        require_authorization=bool(payload.get("require_authorization", True)),
        command_type=str(payload.get("command_type") or ""),
        safety_flags=[str(item) for item in payload.get("safety_flags") or []],
    )


def normalize_template(payload: dict[str, Any]) -> dict[str, Any]:
    template = payload.get("template")
    if not isinstance(template, dict):
        raise ValueError("missing template block")
    command = str(template.get("command") or "").strip()
    if not command:
        raise ValueError("template.command is required")
    output_bindings = template.get("output_bindings") or {}
    if not isinstance(output_bindings, dict):
        raise ValueError("template.output_bindings must be a mapping")
    env_vars = template.get("env_vars") or {}
    if not isinstance(env_vars, dict):
        raise ValueError("template.env_vars must be a mapping")
    return {
        "command": command,
        "output_bindings": {str(key): str(value) for key, value in output_bindings.items()},
        "env_vars": {str(key): str(value) for key, value in env_vars.items()},
        "renderer_version": int(template.get("renderer_version", 1)),
    }


def build_step_definition_from_payload(payload: dict[str, Any], steps_module) -> Any:
    step_type = str(payload.get("step_type") or "").strip()
    display_name = str(payload.get("display_name") or step_type).strip()
    if not step_type:
        raise ValueError("step_type is required")

    input_slots_payload = payload.get("input_slots") or []
    output_slots_payload = payload.get("output_slots") or []
    if not isinstance(input_slots_payload, list) or not isinstance(output_slots_payload, list):
        raise ValueError("input_slots and output_slots must be lists")

    return steps_module.StepTypeDefinition(
        step_type=step_type,
        display_name=display_name,
        input_slots=[
            _coerce_slot(slot_payload, steps_module)
            for slot_payload in input_slots_payload
            if isinstance(slot_payload, dict)
        ],
        output_slots=[
            _coerce_slot(slot_payload, steps_module)
            for slot_payload in output_slots_payload
            if isinstance(slot_payload, dict)
        ],
        params_schema=payload.get("params_schema") or {"type": "object", "properties": {}},
        fanout_mode=str(payload.get("fanout_mode") or steps_module.FanoutMode.NONE),
        repair_policy=_coerce_repair_policy(payload.get("repair_policy") or {}, steps_module),
        safety_policy=_coerce_safety_policy(payload.get("safety_policy") or {}, steps_module),
        renderer_spec=normalize_template(payload),
        pixi_packages=[str(item) for item in payload.get("pixi_packages") or []],
    )


def sync_runtime_custom_step_registry(analysis_dir: Path | None, steps_module) -> None:
    """Load declarative custom step specs from the analysis workspace."""
    global _LAST_SIGNATURE

    paths = _spec_paths(analysis_dir)
    signature = _compute_signature(paths)
    if signature == _LAST_SIGNATURE:
        return

    steps_module.reset_custom()

    for path in paths:
        try:
            for payload in _iter_step_payloads(path):
                defn = build_step_definition_from_payload(payload, steps_module)
                steps_module.register_custom(defn)
        except Exception as exc:
            log.warning("Failed to load custom step spec from %s: %s", path, exc)

    _LAST_SIGNATURE = signature
