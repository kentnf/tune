"""Abstract-plan to execution-plan orchestration helpers.

This module keeps the confirmed abstract plan separate from the concrete
execution objects used by the worker runtime:

- resolved_plan_json: confirmed Abstract Plan
- execution_ir_json:  orchestration semantics
- expanded_dag_json:  concrete nodes the worker executes
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import selectinload


@dataclass
class OrchestrationBundle:
    abstract_plan: Any
    execution_ir: dict[str, Any]
    expanded_dag: dict[str, Any]


def extract_plan_steps(plan_payload: Any) -> list[dict[str, Any]]:
    """Return step dicts from a plan payload."""
    if isinstance(plan_payload, dict):
        steps = plan_payload.get("steps") or []
        return [dict(step) for step in steps if isinstance(step, dict)]
    if isinstance(plan_payload, list):
        return [dict(step) for step in plan_payload if isinstance(step, dict)]
    return []


def replace_plan_steps(plan_payload: Any, steps: list[dict[str, Any]]) -> dict[str, Any] | list[dict[str, Any]]:
    """Return a payload with its steps replaced, preserving dict/list shape."""
    normalized_steps = [dict(step) for step in steps if isinstance(step, dict)]
    if isinstance(plan_payload, dict):
        payload = dict(plan_payload)
        payload["steps"] = normalized_steps
        return payload
    return normalized_steps


def extract_execution_nodes(plan_payload: Any) -> list[dict[str, Any]]:
    """Return executable nodes from an expanded DAG payload or a plain step list."""
    if isinstance(plan_payload, dict):
        nodes = plan_payload.get("nodes")
        if isinstance(nodes, list):
            return [dict(node) for node in nodes if isinstance(node, dict)]
    return extract_plan_steps(plan_payload)


def _step_key(step: dict[str, Any]) -> str:
    return str(step.get("step_key") or step.get("name") or "").strip()


def _step_display_name(step: dict[str, Any]) -> str:
    return str(
        step.get("display_name")
        or step.get("name")
        or step.get("step_key")
        or step.get("step_type")
        or "unknown_step"
    )


def _sanitize_fanout_token(value: str) -> str:
    token = re.sub(r"[^a-zA-Z0-9]+", "_", value).strip("_").lower()
    return token[:48] or "unit"


def _collect_per_sample_lineages(project_files: list[dict[str, Any]]) -> list[dict[str, Any]]:
    lineages: dict[str, dict[str, Any]] = {}

    for project_file in project_files:
        sample_id = str(project_file.get("linked_sample_id") or "").strip()
        experiment_id = str(project_file.get("linked_experiment_id") or "").strip()
        sample_name = str(project_file.get("sample_name") or "").strip()

        if experiment_id:
            lineage_key = f"exp_{_sanitize_fanout_token(experiment_id)}"
            display_label = sample_name or sample_id or experiment_id
            if sample_name and sample_id and sample_name != sample_id:
                display_label = sample_name
            display_label = f"{display_label}:{experiment_id[:8]}" if display_label else experiment_id[:8]
        elif sample_id:
            lineage_key = f"sample_{_sanitize_fanout_token(sample_id)}"
            display_label = sample_name or sample_id
        else:
            continue

        preferred_lineage = lineages.setdefault(
            lineage_key,
            {
                "lineage_key": lineage_key,
                "display_label": display_label or lineage_key,
                "preferred_lineage": {},
            },
        )["preferred_lineage"]
        if sample_id and not preferred_lineage.get("sample_id"):
            preferred_lineage["sample_id"] = sample_id
        if experiment_id and not preferred_lineage.get("experiment_id"):
            preferred_lineage["experiment_id"] = experiment_id
        if sample_name and not preferred_lineage.get("sample_name"):
            preferred_lineage["sample_name"] = sample_name

    return sorted(
        lineages.values(),
        key=lambda item: (
            item["preferred_lineage"].get("sample_name")
            or item["preferred_lineage"].get("sample_id")
            or item["preferred_lineage"].get("experiment_id")
            or item["lineage_key"],
            item["preferred_lineage"].get("experiment_id") or "",
            item["lineage_key"],
        ),
    )


def _infer_execution_kind(step_type: str) -> str:
    if step_type.startswith("qc."):
        return "qc"
    if step_type.startswith("trim."):
        return "transform"
    if step_type.startswith("align."):
        return "align"
    if step_type.startswith("quant."):
        return "quantify"
    if step_type.startswith("stats."):
        return "stats"
    if step_type.startswith("util."):
        return "prepare"
    return "task"


def _infer_input_semantics(step_type: str, params: dict[str, Any]) -> list[str]:
    paired_end = bool((params or {}).get("paired_end", False))
    if step_type == "qc.fastqc":
        return ["raw_paired_reads" if paired_end else "raw_reads"]
    if step_type == "trim.fastp":
        return ["raw_paired_reads" if paired_end else "raw_reads"]
    if step_type in {"align.hisat2", "align.star"}:
        return [
            "trimmed_paired_reads" if paired_end else "trimmed_reads",
            "aligner_index",
        ]
    if step_type == "quant.featurecounts":
        return ["aligned_bam_collection", "annotation_gtf"]
    if step_type == "stats.deseq2":
        return ["counts_matrix"]
    if step_type == "util.hisat2_build":
        return ["reference_fasta"]
    if step_type == "util.star_genome_generate":
        return ["reference_fasta", "annotation_gtf"]
    return []


def _infer_scope(step: dict[str, Any]) -> str:
    from tune.core.registry import get_step_type
    from tune.core.registry.steps import FanoutMode

    defn = get_step_type(step.get("step_type") or "")
    if defn and defn.fanout_mode == FanoutMode.PER_SAMPLE:
        return "per_sample"
    if step.get("step_type") in {"qc.multiqc", "quant.featurecounts", "stats.deseq2"}:
        return "aggregate"
    return "global"


def _infer_aggregation_mode(step: dict[str, Any]) -> str:
    step_type = step.get("step_type") or ""
    if step_type in {"qc.multiqc", "quant.featurecounts", "stats.deseq2"}:
        return "all_upstream"
    if _infer_scope(step) == "per_sample":
        return "same_lineage"
    return "none"


def build_execution_ir(steps: list[dict[str, Any]]) -> dict[str, Any]:
    ir_steps: list[dict[str, Any]] = []
    for step in steps:
        ir_steps.append(
            {
                "step_key": _step_key(step),
                "step_type": step.get("step_type") or "",
                "display_name": _step_display_name(step),
                "execution_kind": _infer_execution_kind(step.get("step_type") or ""),
                "scope": _infer_scope(step),
                "input_semantics": _infer_input_semantics(
                    step.get("step_type") or "",
                    step.get("params") or {},
                ),
                "aggregation_mode": _infer_aggregation_mode(step),
                "depends_on": list(step.get("depends_on") or []),
                "params": dict(step.get("params") or {}),
            }
        )

    return {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "steps": ir_steps,
    }


def build_expanded_dag(
    steps: list[dict[str, Any]],
    project_files: list[dict[str, Any]],
) -> dict[str, Any]:
    from tune.core.registry import get_step_type
    from tune.core.registry.steps import FanoutMode

    lineage_units = _collect_per_sample_lineages(project_files)
    expanded_nodes: list[dict[str, Any]] = []
    expanded_groups: list[dict[str, Any]] = []
    expanded_group_map: dict[str, list[str]] = {}
    expanded_lineage_map: dict[str, dict[str, str]] = {}

    for step in steps:
        origin_step_key = _step_key(step)
        if not origin_step_key:
            continue

        step_type = step.get("step_type") or ""
        defn = get_step_type(step_type)
        scope = _infer_scope(step)
        should_expand = bool(
            defn
            and defn.fanout_mode == FanoutMode.PER_SAMPLE
            and lineage_units
        )
        origin_depends_on = list(step.get("depends_on") or [])

        if should_expand:
            group_node_keys: list[str] = []
            lineage_key_map: dict[str, str] = {}
            for lineage_unit in lineage_units:
                expanded_step = dict(step)
                expanded_step["step_key"] = f"{origin_step_key}__{lineage_unit['lineage_key']}"
                expanded_step["display_name"] = (
                    f"{_step_display_name(step)} [{lineage_unit['display_label']}]"
                )
                expanded_step["depends_on"] = []
                expanded_step["_origin_step_key"] = origin_step_key
                expanded_step["_origin_depends_on"] = origin_depends_on
                expanded_step["_fanout_expanded"] = True
                expanded_step["_fanout_lineage_key"] = lineage_unit["lineage_key"]
                expanded_step["_preferred_lineage"] = dict(lineage_unit["preferred_lineage"])
                expanded_nodes.append(expanded_step)
                group_node_keys.append(expanded_step["step_key"])
                lineage_key_map[lineage_unit["lineage_key"]] = expanded_step["step_key"]

            expanded_group_map[origin_step_key] = group_node_keys
            expanded_lineage_map[origin_step_key] = lineage_key_map
            expanded_groups.append(
                {
                    "group_key": origin_step_key,
                    "scope": scope,
                    "origin_step_type": step_type,
                    "origin_display_name": _step_display_name(step),
                    "node_keys": group_node_keys,
                }
            )
        else:
            normalized_step = dict(step)
            normalized_step["_origin_step_key"] = origin_step_key
            normalized_step["_origin_depends_on"] = origin_depends_on
            expanded_nodes.append(normalized_step)
            expanded_group_map[origin_step_key] = [origin_step_key]
            expanded_groups.append(
                {
                    "group_key": origin_step_key,
                    "scope": scope,
                    "origin_step_type": step_type,
                    "origin_display_name": _step_display_name(step),
                    "node_keys": [origin_step_key],
                }
            )

    for step in expanded_nodes:
        origin_depends_on = list(step.get("_origin_depends_on") or step.get("depends_on") or [])
        lineage_key = step.get("_fanout_lineage_key")
        resolved_deps: list[str] = []

        for dep_key in origin_depends_on:
            dep_lineage_map = expanded_lineage_map.get(dep_key)
            if lineage_key and dep_lineage_map:
                matched_dep = dep_lineage_map.get(lineage_key)
                if matched_dep:
                    resolved_deps.append(matched_dep)
                    continue
            resolved_deps.extend(expanded_group_map.get(dep_key, [dep_key]))

        deduped_deps: list[str] = []
        seen: set[str] = set()
        for dep in resolved_deps:
            if dep and dep not in seen:
                deduped_deps.append(dep)
                seen.add(dep)
        step["depends_on"] = deduped_deps

    return {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "nodes": expanded_nodes,
        "groups": expanded_groups,
    }


def build_execution_payload(
    plan_payload: Any,
    project_files: list[dict[str, Any]],
) -> OrchestrationBundle:
    abstract_steps = extract_plan_steps(plan_payload)
    abstract_payload = replace_plan_steps(plan_payload, abstract_steps)
    execution_ir = build_execution_ir(abstract_steps)
    expanded_dag = build_expanded_dag(abstract_steps, project_files)
    return OrchestrationBundle(
        abstract_plan=abstract_payload,
        execution_ir=execution_ir,
        expanded_dag=expanded_dag,
    )


def summarize_expanded_dag_for_confirmation(expanded_dag: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Return a grouped, human-readable plan view for second confirmation."""
    if not isinstance(expanded_dag, dict):
        return []

    nodes = {
        str(node.get("step_key") or ""): node
        for node in expanded_dag.get("nodes", [])
        if isinstance(node, dict) and (node.get("step_key") or "")
    }
    groups = expanded_dag.get("groups") or []
    group_key_by_node_key: dict[str, str] = {}
    for group in groups:
        if not isinstance(group, dict):
            continue
        group_key = str(group.get("group_key") or "").strip()
        if not group_key:
            continue
        for node_key in group.get("node_keys") or []:
            normalized_node_key = str(node_key).strip()
            if normalized_node_key:
                group_key_by_node_key[normalized_node_key] = group_key

    summaries: list[dict[str, Any]] = []

    for group in groups:
        if not isinstance(group, dict):
            continue
        group_key = str(group.get("group_key") or "").strip()
        node_keys = [str(key) for key in group.get("node_keys") or [] if str(key).strip()]
        if not group_key or not node_keys:
            continue

        first_node = nodes.get(node_keys[0], {})
        raw_depends_on: list[str] = []
        for node_key in node_keys:
            node = nodes.get(node_key, {})
            raw_depends_on.extend(str(dep) for dep in (node.get("depends_on") or []))
        depends_on: list[str] = []
        seen_dep_keys: set[str] = set()
        for dep in raw_depends_on:
            normalized_dep = str(dep).strip()
            if not normalized_dep:
                continue
            collapsed_dep = group_key_by_node_key.get(normalized_dep, normalized_dep)
            if collapsed_dep not in seen_dep_keys:
                depends_on.append(collapsed_dep)
                seen_dep_keys.add(collapsed_dep)
        scope = str(group.get("scope") or "global")
        scope_label = {
            "global": "global x1",
            "aggregate": "aggregate x1",
            "per_sample": f"per_sample x{len(node_keys)}",
        }.get(scope, f"{scope} x{len(node_keys)}")
        description_parts = [scope_label]
        if depends_on:
            description_parts.append(f"depends_on={', '.join(depends_on)}")

        summaries.append(
            {
                "step_key": group_key,
                "step_type": group.get("origin_step_type") or first_node.get("step_type") or "",
                "display_name": group.get("origin_display_name") or first_node.get("display_name") or group_key,
                "description": " | ".join(description_parts),
                "depends_on": depends_on,
                "node_count": len(node_keys),
                "scope": scope,
            }
        )

    return summaries


def build_execution_bundle(
    steps: list[dict[str, Any]],
    project_files: list[dict[str, Any]],
) -> OrchestrationBundle:
    return build_execution_payload({"steps": steps}, project_files)


async def load_project_execution_files(session, project_id: str | None) -> list[dict[str, Any]]:
    if not project_id:
        return []

    from tune.core.models import Experiment, File, FileRun, Sample

    files = (
        await session.execute(
            select(File)
            .options(selectinload(File.enhanced_metadata))
            .where(File.project_id == project_id)
            .limit(500)
        )
    ).scalars().all()

    file_lineage: dict[str, dict[str, Any]] = {}
    file_ids = [f.id for f in files]
    if file_ids:
        lineage_rows = await session.execute(
            select(
                FileRun.file_id,
                FileRun.read_number,
                Experiment.id,
                Sample.id,
                Sample.sample_name,
                Experiment.library_layout,
                Experiment.library_strategy,
            )
            .join(Experiment, FileRun.experiment_id == Experiment.id)
            .join(Sample, Experiment.sample_id == Sample.id)
            .where(FileRun.file_id.in_(file_ids))
        )
        for row in lineage_rows.all():
            file_lineage[row[0]] = {
                "read_number": row[1],
                "linked_experiment_id": row[2],
                "linked_sample_id": row[3],
                "sample_name": row[4],
                "library_layout": row[5],
                "library_strategy": row[6],
            }

    return [
        {
            "id": f.id,
            "path": f.path,
            "filename": f.filename,
            "file_type": f.file_type,
            "metadata": {m.field_key: m.field_value for m in f.enhanced_metadata},
            **file_lineage.get(f.id, {}),
        }
        for f in files
    ]


async def materialize_job_execution_plan(
    session,
    job,
    plan_payload: Any,
) -> OrchestrationBundle:
    from tune.core.registry.spec_generation import augment_plan_with_dynamic_specs
    from tune.core.workflow.plan_compiler import compile_plan

    raw_steps = extract_plan_steps(plan_payload)
    raw_steps, dynamic_issues = await augment_plan_with_dynamic_specs(
        raw_steps,
        context_hint=(
            f"Goal: {getattr(job, 'goal', '') or ''}\n"
            f"Project ID: {getattr(job, 'project_id', '') or ''}"
        ),
    )
    if dynamic_issues:
        raise ValueError("; ".join(dynamic_issues) or "Failed to generate dynamic step specs")
    compile_result = compile_plan(raw_steps)
    if not compile_result.ok:
        raise ValueError("; ".join(compile_result.errors) or "Failed to compile confirmed plan")

    normalized_payload = replace_plan_steps(plan_payload, compile_result.compiled_steps)
    project_files = await load_project_execution_files(session, getattr(job, "project_id", None))
    bundle = build_execution_payload(normalized_payload, project_files)
    job.resolved_plan_json = bundle.abstract_plan
    job.execution_ir_json = bundle.execution_ir
    job.expanded_dag_json = bundle.expanded_dag
    return bundle
