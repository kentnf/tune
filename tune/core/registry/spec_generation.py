"""LLM-assisted generation of dynamic step specifications for unknown tools."""
from __future__ import annotations

import logging
from typing import Any

from tune.core.llm.gateway import LLMMessage, get_gateway

log = logging.getLogger(__name__)

_DYNAMIC_SPEC_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "specs": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "step_key": {"type": "string"},
                    "step_type": {"type": "string"},
                    "dynamic_spec": {
                        "type": "object",
                        "properties": {
                            "display_name": {"type": "string"},
                            "fanout_mode": {"type": "string"},
                            "pixi_packages": {"type": "array", "items": {"type": "string"}},
                            "input_slots": {"type": "array", "items": {"type": "object"}},
                            "output_slots": {"type": "array", "items": {"type": "object"}},
                            "params_schema": {"type": "object"},
                            "repair_policy": {"type": "object"},
                            "safety_policy": {"type": "object"},
                            "template": {"type": "object"},
                        },
                        "required": [
                            "pixi_packages",
                            "input_slots",
                            "output_slots",
                            "params_schema",
                            "safety_policy",
                            "template",
                        ],
                    },
                },
                "required": ["step_key", "step_type", "dynamic_spec"],
            },
        }
    },
    "required": ["specs"],
}

_SYSTEM_PROMPT = """You generate execution-ready dynamic bioinformatics step specs.

Goal:
- For each unknown step_type, produce a safe declarative dynamic_spec that Tune can execute.

Constraints:
- Only generate specs for real bioinformatics CLI tools.
- Prefer packages from bioconda/conda-forge that match the tool binary.
- Do not use Python/R scripts unless the tool itself is the intended executable.
- The command template must be a single shell command template using placeholders like:
  {read1} {read2} {output_dir} {threads} {index_dir} {annotation_gtf}
- Output paths must be concrete files inside {output_dir}.
- For file_types, use broad practical suffix categories already used by Tune such as:
  fastq, fastq.gz, fq, fq.gz, bam, sam, gtf, gff, gff3, fa, fasta, fna, txt, csv, tsv, *
- safety_policy.command_type should be the primary CLI binary name.
- safety_policy.safety_flags should usually include ["write_to_disk"].
- If the step is clearly per-sample, set fanout_mode to "per_sample"; otherwise "none".
- input_slots and output_slots must be minimal but sufficient.
- params_schema should include useful defaults for threads / library options when appropriate.

Do not explain anything. Return only structured JSON matching the schema."""


def _unknown_steps(steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    from tune.core.registry import get_step_type

    items: list[dict[str, Any]] = []
    for step in steps:
        step_type = str(step.get("step_type") or "").strip()
        if not step_type:
            continue
        if step.get("dynamic_spec"):
            continue
        if get_step_type(step_type) is not None:
            continue
        items.append(step)
    return items


def _build_request_payload(steps: list[dict[str, Any]], context_hint: str = "") -> str:
    from tune.core.registry import all_step_types

    unknown = _unknown_steps(steps)
    builtins = sorted(all_step_types())
    return (
        f"Known built-in step types:\n{builtins}\n\n"
        f"Context:\n{context_hint or 'No extra context.'}\n\n"
        f"Unknown steps that need dynamic specs:\n{unknown}\n\n"
        "Return one dynamic_spec per unknown step."
    )


async def augment_plan_with_dynamic_specs(
    steps: list[dict[str, Any]],
    *,
    context_hint: str = "",
) -> tuple[list[dict[str, Any]], list[str]]:
    """Fill missing dynamic_spec fields for unknown step types via the active LLM."""
    unknown = _unknown_steps(steps)
    if not unknown:
        return steps, []

    try:
        gateway = get_gateway()
    except Exception as exc:
        return steps, [f"Unable to generate dynamic tool specs: {exc}"]

    try:
        response = await gateway.structured_output(
            [LLMMessage("user", _build_request_payload(steps, context_hint=context_hint))],
            _DYNAMIC_SPEC_SCHEMA,
            system=_SYSTEM_PROMPT,
        )
    except Exception as exc:
        log.warning("augment_plan_with_dynamic_specs failed: %s", exc)
        return steps, [f"Dynamic step spec generation failed: {exc}"]

    specs = response.get("specs") if isinstance(response, dict) else None
    if not isinstance(specs, list):
        return steps, ["Dynamic step spec generation returned an invalid payload"]

    spec_map: dict[tuple[str, str], dict[str, Any]] = {}
    for item in specs:
        if not isinstance(item, dict):
            continue
        step_key = str(item.get("step_key") or "").strip()
        step_type = str(item.get("step_type") or "").strip()
        dynamic_spec = item.get("dynamic_spec")
        if not step_key or not step_type or not isinstance(dynamic_spec, dict):
            continue
        spec_map[(step_key, step_type)] = dynamic_spec

    issues: list[str] = []
    augmented_steps: list[dict[str, Any]] = []
    for step in steps:
        step_key = str(step.get("step_key") or "").strip()
        step_type = str(step.get("step_type") or "").strip()
        if step.get("dynamic_spec") or not step_key or not step_type:
            augmented_steps.append(step)
            continue
        dynamic_spec = spec_map.get((step_key, step_type))
        if dynamic_spec is None:
            from tune.core.registry import get_step_type

            if get_step_type(step_type) is None:
                issues.append(f"Step '{step_key}' is missing dynamic_spec for unknown step_type '{step_type}'")
            augmented_steps.append(step)
            continue
        patched = dict(step)
        patched["dynamic_spec"] = dynamic_spec
        augmented_steps.append(patched)

    return augmented_steps, issues
