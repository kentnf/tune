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
from tune.core.context.semantic_dossier import build_project_memory_summary


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
                    "_preflight_injected": bool(step.get("_preflight_injected")),
                    "_rr_injected": bool(step.get("_rr_injected")),
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
                    "_preflight_injected": bool(step.get("_preflight_injected")),
                    "_rr_injected": bool(step.get("_rr_injected")),
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


def _collect_confirmation_group_views(expanded_dag: dict[str, Any] | None) -> list[dict[str, Any]]:
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

    views: list[dict[str, Any]] = []
    for group in groups:
        if not isinstance(group, dict):
            continue
        group_key = str(group.get("group_key") or "").strip()
        node_keys = [str(key) for key in group.get("node_keys") or [] if str(key).strip()]
        if not group_key or not node_keys:
            continue

        group_nodes = [nodes.get(node_key, {}) for node_key in node_keys]
        first_node = group_nodes[0] if group_nodes else {}
        raw_depends_on: list[str] = []
        for node in group_nodes:
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

        views.append(
            {
                "group": group,
                "group_key": group_key,
                "node_keys": node_keys,
                "group_nodes": group_nodes,
                "first_node": first_node,
                "depends_on": depends_on,
                "scope": str(group.get("scope") or "global"),
                "step_type": group.get("origin_step_type") or first_node.get("step_type") or "",
                "display_name": group.get("origin_display_name") or first_node.get("display_name") or group_key,
            }
        )

    return views


def summarize_expanded_dag_for_confirmation(expanded_dag: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Return a grouped, human-readable plan view for second confirmation."""
    summaries: list[dict[str, Any]] = []

    for view in _collect_confirmation_group_views(expanded_dag):
        scope = view["scope"]
        node_keys = view["node_keys"]
        depends_on = view["depends_on"]
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
                "step_key": view["group_key"],
                "step_type": view["step_type"],
                "display_name": view["display_name"],
                "description": " | ".join(description_parts),
                "depends_on": depends_on,
                "node_count": len(node_keys),
                "scope": scope,
            }
        )

    return summaries


def summarize_execution_review_changes(expanded_dag: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Describe concrete orchestration changes between the abstract plan and expanded DAG."""
    changes: list[dict[str, Any]] = []

    for view in _collect_confirmation_group_views(expanded_dag):
        group = view["group"]
        node_keys = view["node_keys"]
        group_nodes = view["group_nodes"]
        group_key = view["group_key"]
        depends_on = view["depends_on"]
        scope = view["scope"]

        change_kinds: list[str] = []
        detail_parts: list[str] = []
        fan_out_mode: str | None = None
        aggregate_mode: str | None = None

        fan_out = any(bool(node.get("_fanout_expanded")) for node in group_nodes) or any(
            node_key != group_key for node_key in node_keys
        )
        if fan_out:
            change_kinds.append("fan_out")
            fan_out_mode = "per_sample" if scope == "per_sample" else "generic"
            if fan_out_mode == "per_sample":
                detail_parts.append(f"expanded into {len(node_keys)} per-sample execution node(s)")
            else:
                detail_parts.append(f"expanded into {len(node_keys)} execution node(s)")

        if scope == "aggregate":
            change_kinds.append("aggregate")
            aggregate_mode = "all_upstream"
            if depends_on:
                detail_parts.append(f"aggregates upstream outputs from {', '.join(depends_on)} into one execution step")
            else:
                detail_parts.append("aggregates upstream outputs into one execution step")

        auto_injected_reasons: list[str] = []
        auto_injected_cause: str | None = None
        if group.get("_preflight_injected") or any(bool(node.get("_preflight_injected")) for node in group_nodes):
            auto_injected_reasons.append("preflight")
        if group.get("_rr_injected") or any(bool(node.get("_rr_injected")) for node in group_nodes):
            auto_injected_reasons.append("resource_readiness")
        if auto_injected_reasons:
            change_kinds.append("auto_injected")
            if any(bool(node.get("_stale_rebuild")) for node in group_nodes):
                auto_injected_cause = "stale_derived_resource"
            elif "preflight" in auto_injected_reasons and view["step_type"] == "util.hisat2_build":
                auto_injected_cause = "missing_hisat2_index"
            elif "preflight" in auto_injected_reasons and view["step_type"] == "util.star_genome_generate":
                auto_injected_cause = "missing_star_genome"
            elif "resource_readiness" in auto_injected_reasons and view["step_type"] == "util.hisat2_build":
                auto_injected_cause = "derivable_hisat2_index"
            elif "resource_readiness" in auto_injected_reasons and view["step_type"] == "util.star_genome_generate":
                auto_injected_cause = "derivable_star_genome"

            cause_summary = {
                "missing_hisat2_index": "auto-injected to build a missing HISAT2 index from the registered reference FASTA",
                "missing_star_genome": "auto-injected to build a missing STAR genome index from registered reference resources",
                "derivable_hisat2_index": "auto-injected because the required HISAT2 index is derivable from registered reference resources",
                "derivable_star_genome": "auto-injected because the required STAR genome index is derivable from registered reference resources",
                "stale_derived_resource": "auto-injected to rebuild a stale derived reference resource before downstream execution",
            }.get(auto_injected_cause)
            detail_parts.append(cause_summary or f"auto-injected via {', '.join(auto_injected_reasons)}")

        if not change_kinds:
            continue

        changes.append(
            {
                "group_key": group_key,
                "step_type": view["step_type"],
                "display_name": view["display_name"],
                "change_kinds": change_kinds,
                "summary": " | ".join(detail_parts),
                "depends_on": depends_on,
                "node_count": len(node_keys),
                "scope": scope,
                "fan_out_mode": fan_out_mode,
                "aggregate_mode": aggregate_mode,
                "auto_injected_reasons": auto_injected_reasons,
                "auto_injected_cause": auto_injected_cause,
            }
        )

    return changes


def summarize_execution_plan_delta(
    plan_payload: Any,
    expanded_dag: dict[str, Any] | None,
) -> dict[str, Any]:
    """Compare the confirmed abstract plan with the grouped execution DAG."""
    abstract_steps = extract_plan_steps(plan_payload)
    abstract_step_keys = {
        _step_key(step)
        for step in abstract_steps
        if _step_key(step)
    }
    changes = summarize_execution_review_changes(expanded_dag)
    changes_by_group_key = {
        str(item.get("group_key") or ""): item
        for item in changes
        if str(item.get("group_key") or "").strip()
    }

    added_groups: list[dict[str, Any]] = []
    changed_groups: list[dict[str, Any]] = []
    unchanged_groups: list[dict[str, Any]] = []

    for view in _collect_confirmation_group_views(expanded_dag):
        group_key = view["group_key"]
        base_item = {
            "group_key": group_key,
            "display_name": view["display_name"],
            "step_type": view["step_type"],
        }
        if group_key not in abstract_step_keys:
            added_groups.append(base_item)
            continue
        if group_key in changes_by_group_key:
            changed_groups.append(
                {
                    **base_item,
                    "change_kinds": list(changes_by_group_key[group_key].get("change_kinds") or []),
                }
            )
            continue
        unchanged_groups.append(base_item)

    return {
        "abstract_step_count": len(abstract_step_keys),
        "execution_group_count": len(added_groups) + len(changed_groups) + len(unchanged_groups),
        "added_group_count": len(added_groups),
        "changed_group_count": len(changed_groups),
        "unchanged_group_count": len(unchanged_groups),
        "added_groups": added_groups,
        "changed_groups": changed_groups,
    }


def summarize_execution_ir_for_confirmation(execution_ir: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Return a readable execution-semantics view for second confirmation."""
    if not isinstance(execution_ir, dict):
        return []

    review_items: list[dict[str, Any]] = []
    for step in execution_ir.get("steps", []) or []:
        if not isinstance(step, dict):
            continue
        step_key = str(step.get("step_key") or "").strip()
        if not step_key:
            continue
        scope = str(step.get("scope") or "global")
        execution_kind = str(step.get("execution_kind") or "").strip()
        aggregation_mode = str(step.get("aggregation_mode") or "none").strip()
        input_semantics = [str(item) for item in (step.get("input_semantics") or []) if str(item).strip()]
        depends_on = [str(item) for item in (step.get("depends_on") or []) if str(item).strip()]

        description_parts = [scope]
        if execution_kind:
            description_parts.append(execution_kind)
        if input_semantics:
            description_parts.append(f"inputs={', '.join(input_semantics)}")
        if aggregation_mode and aggregation_mode != "none":
            description_parts.append(f"aggregate={aggregation_mode}")
        if depends_on:
            description_parts.append(f"depends_on={', '.join(depends_on)}")

        review_items.append(
            {
                "step_key": step_key,
                "step_type": step.get("step_type") or "",
                "display_name": step.get("display_name") or step_key,
                "description": " | ".join(description_parts),
                "scope": scope,
                "execution_kind": execution_kind,
                "aggregation_mode": aggregation_mode,
                "input_semantics": input_semantics,
                "depends_on": depends_on,
            }
        )

    return review_items


def summarize_execution_semantic_guardrails(
    execution_ir: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(execution_ir, dict):
        return None
    payload = execution_ir.get("semantic_guardrails")
    if not isinstance(payload, dict):
        return None

    ambiguity_reviews: list[dict[str, Any]] = []
    for item in payload.get("ambiguity_reviews") or []:
        if not isinstance(item, dict):
            continue
        ambiguity_reviews.append(
            {
                "step_key": str(item.get("step_key") or "").strip(),
                "step_type": str(item.get("step_type") or "").strip(),
                "display_name": str(
                    item.get("display_name")
                    or item.get("step_key")
                    or item.get("step_type")
                    or "unknown_step"
                ).strip(),
                "slot_name": str(item.get("slot_name") or "").strip(),
                "binding_key": str(item.get("binding_key") or "").strip(),
                "primary_path": item.get("primary_path"),
                "secondary_path": item.get("secondary_path"),
                "score_gap": item.get("score_gap"),
                "candidate_count": item.get("candidate_count"),
                "description": (
                    f"{item.get('slot_name') or 'input'} has close candidates "
                    f"'{item.get('primary_path')}' vs '{item.get('secondary_path')}'"
                    f" (gap={item.get('score_gap')}, total={item.get('candidate_count')})."
                ),
            }
        )

    memory_binding_reviews: list[dict[str, Any]] = []
    for item in payload.get("memory_binding_reviews") or []:
        if not isinstance(item, dict):
            continue
        memory_binding_reviews.append(
            {
                "step_key": str(item.get("step_key") or "").strip(),
                "step_type": str(item.get("step_type") or "").strip(),
                "display_name": str(
                    item.get("display_name")
                    or item.get("step_key")
                    or item.get("step_type")
                    or "unknown_step"
                ).strip(),
                "slot_name": str(item.get("slot_name") or "").strip(),
                "binding_key": str(item.get("binding_key") or "").strip(),
                "fact_key": str(item.get("fact_key") or "").strip(),
                "confirmed_path": item.get("confirmed_path"),
                "candidate_path": item.get("candidate_path"),
                "candidate_count": item.get("candidate_count"),
                "description": (
                    f"{item.get('slot_name') or 'input'} currently prefers '{item.get('candidate_path')}', "
                    f"but project memory previously confirmed '{item.get('confirmed_path')}'."
                ),
            }
        )

    result = {
        "ambiguity_count": len(ambiguity_reviews),
        "ambiguity_reviews": ambiguity_reviews,
        "memory_review_count": len(memory_binding_reviews),
        "memory_binding_reviews": memory_binding_reviews,
    }
    project_memory_summary = payload.get("project_memory_summary")
    if isinstance(project_memory_summary, dict) and project_memory_summary:
        result["project_memory_summary"] = build_project_memory_summary(
            stable_facts=[{}] * int(project_memory_summary.get("stable_fact_count", 0) or 0),
            memory_patterns=[{}] * int(project_memory_summary.get("memory_pattern_count", 0) or 0),
            memory_preferences=[{}] * int(project_memory_summary.get("memory_preference_count", 0) or 0),
            memory_links=(
                [{"entity_type": "resource_entity"}] * int(project_memory_summary.get("resource_link_count", 0) or 0)
                + [{"entity_type": "artifact_record"}] * int(project_memory_summary.get("artifact_link_count", 0) or 0)
                + [{"entity_type": "memory_episode"}] * int(project_memory_summary.get("runtime_link_count", 0) or 0)
            ),
            resource_binding_fact_count=int(project_memory_summary.get("resource_binding_fact_count", 0) or 0),
        )
    return result


def summarize_execution_decision_source(
    execution_ir: dict[str, Any] | None,
) -> str | None:
    if not isinstance(execution_ir, dict):
        return None
    semantic_guardrails = summarize_execution_semantic_guardrails(execution_ir) or {}
    if int(semantic_guardrails.get("ambiguity_count", 0) or 0) > 0:
        return "semantic_trace"
    if int(semantic_guardrails.get("memory_review_count", 0) or 0) > 0:
        return "structured_memory"
    steps = execution_ir.get("steps")
    if isinstance(steps, list) and steps:
        return "confirmation_gate"
    return None


def summarize_execution_confirmation_overview(
    *,
    plan_payload: Any,
    execution_ir: dict[str, Any] | None,
    expanded_dag: dict[str, Any] | None,
) -> dict[str, Any]:
    """Return a compact summary that compresses IR + delta + change signals."""
    ir_review = summarize_execution_ir_for_confirmation(execution_ir)
    delta = summarize_execution_plan_delta(plan_payload, expanded_dag)
    changes = summarize_execution_review_changes(expanded_dag)
    semantic_guardrails = summarize_execution_semantic_guardrails(execution_ir) or {}

    scope_counts = {"global": 0, "per_sample": 0, "aggregate": 0}
    for item in ir_review:
        scope = str(item.get("scope") or "").strip()
        if scope in scope_counts:
            scope_counts[scope] += 1

    change_kind_counts = {"fan_out": 0, "aggregate": 0, "auto_injected": 0}
    for item in changes:
        for kind in item.get("change_kinds") or []:
            normalized = str(kind).strip()
            if normalized in change_kind_counts:
                change_kind_counts[normalized] += 1

    return {
        "abstract_step_count": delta.get("abstract_step_count", 0),
        "execution_ir_step_count": len(ir_review),
        "execution_group_count": delta.get("execution_group_count", 0),
        "unchanged_group_count": delta.get("unchanged_group_count", 0),
        "changed_group_count": delta.get("changed_group_count", 0),
        "added_group_count": delta.get("added_group_count", 0),
        "per_sample_step_count": scope_counts["per_sample"],
        "aggregate_step_count": scope_counts["aggregate"],
        "global_step_count": scope_counts["global"],
        "fan_out_change_count": change_kind_counts["fan_out"],
        "aggregate_change_count": change_kind_counts["aggregate"],
        "auto_injected_change_count": change_kind_counts["auto_injected"],
        "ambiguity_review_count": int(semantic_guardrails.get("ambiguity_count", 0) or 0),
        "memory_review_count": int(semantic_guardrails.get("memory_review_count", 0) or 0),
    }


def build_execution_bundle(
    steps: list[dict[str, Any]],
    project_files: list[dict[str, Any]],
) -> OrchestrationBundle:
    return build_execution_payload({"steps": steps}, project_files)


async def _load_project_memory_guardrail_inputs(
    session,
    project_id: str | None,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    if not project_id:
        return {}, {}
    try:
        from tune.core.memory.project_memory import (
            query_project_memory_facts,
            query_project_memory_links,
            query_project_memory_patterns,
            query_project_memory_preferences,
        )

        facts = await query_project_memory_facts(session, project_id, limit=20)
        patterns = await query_project_memory_patterns(session, project_id, limit=10)
        preferences = await query_project_memory_preferences(session, project_id, limit=10)
        links = await query_project_memory_links(session, project_id, limit=20)
    except Exception:
        return {}, {}

    fact_by_binding_key: dict[str, dict[str, Any]] = {}
    for fact in facts:
        binding_key = str(fact.get("binding_key") or "").strip()
        path = str(fact.get("path") or "").strip()
        if binding_key and path and binding_key not in fact_by_binding_key:
            fact_by_binding_key[binding_key] = fact

    return (
        build_project_memory_summary(
            stable_facts=facts,
            memory_patterns=patterns,
            memory_preferences=preferences,
            memory_links=links,
            resource_binding_fact_count=len(fact_by_binding_key),
        ),
        fact_by_binding_key,
    )


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
    from tune.core.analysis.implementation_decision import extract_implementation_decisions
    from tune.core.registry.spec_generation import augment_plan_with_dynamic_specs
    from tune.core.workflow.plan_compiler import compile_plan_with_decisions

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
    compile_result = compile_plan_with_decisions(
        raw_steps,
        implementation_decisions=extract_implementation_decisions(plan_payload),
    )
    if not compile_result.ok:
        raise ValueError("; ".join(compile_result.errors) or "Failed to compile confirmed plan")

    normalized_payload = replace_plan_steps(plan_payload, compile_result.compiled_steps)
    project_files = await load_project_execution_files(session, getattr(job, "project_id", None))
    bundle = build_execution_payload(normalized_payload, project_files)
    semantic_guardrails = await analyze_execution_plan_semantic_guardrails(
        session,
        project_id=getattr(job, "project_id", None),
        steps=compile_result.compiled_steps,
        project_files=project_files,
    )
    if (
        semantic_guardrails.get("ambiguity_reviews")
        or semantic_guardrails.get("memory_binding_reviews")
    ):
        bundle.execution_ir["semantic_guardrails"] = semantic_guardrails
    job.resolved_plan_json = bundle.abstract_plan
    job.execution_ir_json = bundle.execution_ir
    job.expanded_dag_json = bundle.expanded_dag
    return bundle


async def analyze_execution_plan_semantic_guardrails(
    session,
    *,
    project_id: str | None,
    steps: list[dict[str, Any]],
    project_files: list[dict[str, Any]],
) -> dict[str, Any]:
    if not project_id or not steps:
        return {"ambiguity_reviews": []}

    from tune.core.binding.preflight import _slot_binding_key, _upstream_can_provide
    from tune.core.binding.resolver import load_registered_resource_bindings
    from tune.core.binding.semantic_retrieval import (
        retrieve_semantic_candidates,
        summarize_candidate_ambiguity,
    )
    from tune.core.registry import ensure_registry_loaded, get_step_type

    ensure_registry_loaded()
    kp_bindings = await load_registered_resource_bindings(project_id, session)
    ambiguity_reviews: list[dict[str, Any]] = []
    memory_binding_reviews: list[dict[str, Any]] = []
    project_memory_summary, stable_fact_by_binding_key = await _load_project_memory_guardrail_inputs(
        session,
        project_id,
    )

    for step in steps:
        step_key = _step_key(step)
        step_type = str(step.get("step_type") or "").strip()
        defn = get_step_type(step_type)
        if defn is None:
            continue
        preferred_lineage = dict(step.get("_preferred_lineage") or {}) or None
        dep_keys = list(step.get("depends_on") or [])

        for slot in defn.input_slots:
            if not slot.required or slot.multiple:
                continue
            if slot.name not in {"reference_fasta", "annotation_gtf", "index_prefix", "genome_dir"}:
                continue
            if _upstream_can_provide(step, slot, steps):
                continue

            candidates = await retrieve_semantic_candidates(
                job_id="",
                dep_keys=dep_keys,
                slot=slot,
                project_id=project_id,
                project_files=project_files,
                kp_bindings=kp_bindings,
                db=session,
                preferred_lineage=preferred_lineage,
            )
            ambiguity = summarize_candidate_ambiguity(candidates[:3])
            if not ambiguity:
                pass
            else:
                ambiguity_reviews.append(
                    {
                        "step_key": step_key,
                        "step_type": step_type,
                        "display_name": _step_display_name(step),
                        "slot_name": slot.name,
                        "binding_key": _slot_binding_key(slot.name, step_type=step_type),
                        **ambiguity,
                    }
                )

            binding_key = _slot_binding_key(slot.name, step_type=step_type)
            stable_fact = stable_fact_by_binding_key.get(binding_key)
            top_candidate = candidates[0] if candidates else {}
            candidate_path = str((top_candidate or {}).get("path") or "").strip()
            confirmed_path = str((stable_fact or {}).get("path") or "").strip()
            if stable_fact and confirmed_path and candidate_path and candidate_path != confirmed_path:
                memory_binding_reviews.append(
                    {
                        "step_key": step_key,
                        "step_type": step_type,
                        "display_name": _step_display_name(step),
                        "slot_name": slot.name,
                        "binding_key": binding_key,
                        "fact_key": str(stable_fact.get("fact_key") or "").strip(),
                        "confirmed_path": confirmed_path,
                        "candidate_path": candidate_path,
                        "candidate_count": len(candidates),
                    }
                )

    payload: dict[str, Any] = {
        "ambiguity_reviews": ambiguity_reviews,
        "memory_binding_reviews": memory_binding_reviews,
    }
    if project_memory_summary:
        payload["project_memory_summary"] = project_memory_summary
    return payload
