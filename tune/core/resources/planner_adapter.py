"""PlannerAdapter — injects prepare steps for derivable/stale aligner indices.

Called after generate_coarse_plan() returns steps and before presenting
the plan to the user.  This ensures the user sees the full plan including
any index-build steps they need to approve.

The injection logic here is the canonical source of truth.  The duplicate
logic in preflight.py (FASTA→index injection) is kept as a safety-net
fallback only and should not be extended.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional

from tune.core.resources.models import ReadinessIssue, ResourceGraph, ResourceNode

# Map aligner name → step type and display name
_ALIGNER_STEP_MAP: dict[str, dict] = {
    "hisat2": {
        "step_type": "util.hisat2_build",
        "display_name": "Build HISAT2 Index",
        "command_type": "hisat2-build",
        "output_subdir": "00_hisat2_build",
    },
    "star": {
        "step_type": "util.star_genome_generate",
        "display_name": "Generate STAR Genome",
        "command_type": "star-genome",
        "output_subdir": "00_star_genome",
    },
    "bwa": {
        "step_type": "util.bwa_index",
        "display_name": "Build BWA Index",
        "command_type": "bwa",
        "output_subdir": "00_bwa_index",
    },
    "bowtie2": {
        "step_type": "util.bowtie2_build",
        "display_name": "Build Bowtie2 Index",
        "command_type": "bowtie2-build",
        "output_subdir": "00_bowtie2_build",
    },
}


@dataclass
class PlanFeasibilityResult:
    ok: bool
    amended_plan: list[dict] = field(default_factory=list)
    issues: list[ReadinessIssue] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def inject_prepare_steps(
    plan: list[dict],
    resource_graph: ResourceGraph,
    output_dir: str = "",
) -> list[dict]:
    """Inspect the ResourceGraph for derivable/stale aligner index nodes and
    prepend the corresponding build steps to the plan.

    Idempotent: if a build step of the same step_type is already present,
    it is not inserted again.

    Returns the (possibly amended) plan list.
    """
    amended = list(plan)
    existing_step_types = {s.get("step_type", "") for s in plan}

    idx_node_ids = resource_graph.by_kind.get("aligner_index", [])
    ref_node_ids = resource_graph.by_kind.get("reference_fasta", [])
    ann_node_ids = resource_graph.by_kind.get("annotation_gtf", [])

    ref_node: Optional[ResourceNode] = (
        resource_graph.nodes.get(ref_node_ids[0]) if ref_node_ids else None
    )
    ann_node: Optional[ResourceNode] = (
        resource_graph.nodes.get(ann_node_ids[0]) if ann_node_ids else None
    )

    idx_nodes_by_aligner = {
        _aligner_from_node_id(nid): resource_graph.nodes[nid]
        for nid in idx_node_ids
        if nid in resource_graph.nodes
    }
    required_aligners = set(idx_nodes_by_aligner) | _aligners_required_by_plan(plan)
    inserted_count = 0  # number of steps prepended so far (for ordering)

    for aligner in sorted(required_aligners):
        idx_node = idx_nodes_by_aligner.get(aligner)
        if idx_node and idx_node.status == "ready":
            continue

        step_cfg = _ALIGNER_STEP_MAP.get(aligner)
        if not step_cfg:
            continue

        step_type = step_cfg["step_type"]
        if step_type in existing_step_types:
            # Already present — skip injection
            continue
        if not ref_node or not ref_node.resolved_path:
            # Without a concrete reference FASTA path, the build step itself
            # would be infeasible. Let validation surface the missing input.
            continue
        if idx_node and idx_node.status not in ("derivable", "stale"):
            continue

        build_out_dir = os.path.join(output_dir, step_cfg["output_subdir"]) if output_dir else ""
        step_key = f"_rr_{aligner}_build"

        # Determine what bindings the build step needs
        resolved_bindings: dict[str, str] = {}
        if ref_node and ref_node.resolved_path:
            resolved_bindings["reference_fasta"] = ref_node.resolved_path
        if aligner == "star" and ann_node and ann_node.resolved_path:
            resolved_bindings["annotation_gtf"] = ann_node.resolved_path

        build_step: dict = {
            "step_key": step_key,
            "step_type": step_type,
            "display_name": step_cfg["display_name"],
            "params": {"threads": 4},
            "depends_on": [],
            "_rr_injected": True,           # mark as resource-readiness injected
            "_output_dir": build_out_dir,
            "_resolved_bindings": resolved_bindings,
        }
        if idx_node and idx_node.status == "stale":
            build_step["_stale_rebuild"] = True

        # Update depends_on for downstream alignment steps
        for step in amended:
            step_t = step.get("step_type", "")
            if _step_uses_aligner(step_t, aligner):
                deps = list(step.get("depends_on") or [])
                if step_key not in deps:
                    deps.insert(0, step_key)
                step["depends_on"] = deps

        # Insert at the front (before any previous injected build step)
        amended.insert(inserted_count, build_step)
        existing_step_types.add(step_type)
        inserted_count += 1

    return amended


def enforce_planner_constraints(
    plan: list[dict],
    planner_context,
) -> PlanFeasibilityResult:
    """Rewrite and validate a plan against current resource readiness.

    This runs before the user confirms a plan so the plan shown in chat is
    already constrained by the current project state.
    """
    resource_graph = getattr(planner_context, "resource_graph", None)
    if resource_graph is None:
        return PlanFeasibilityResult(ok=True, amended_plan=list(plan))

    normalized_plan, warnings = _normalize_plan_aligners(plan, resource_graph)
    amended = inject_prepare_steps(normalized_plan, resource_graph)
    issues = validate_plan_feasibility(amended, planner_context)
    return PlanFeasibilityResult(
        ok=not issues,
        amended_plan=amended,
        issues=issues,
        warnings=warnings,
    )


def validate_plan_feasibility(
    plan: list[dict],
    planner_context,
) -> list[ReadinessIssue]:
    """Return blocking issues for any plan step that cannot be satisfied."""
    from tune.core.binding.preflight import _issue_for_missing_slot, _issue_for_unknown_step_type
    from tune.core.registry import get_step_type

    issues: list[ReadinessIssue] = []
    step_map = {step.get("step_key", ""): step for step in plan if step.get("step_key")}

    for step in plan:
        step_key = step.get("step_key", "")
        step_type = step.get("step_type", "")
        defn = get_step_type(step_type)
        if defn is None:
            issues.append(_issue_for_unknown_step_type(step_key, step_type))
            continue

        for slot in defn.input_slots:
            if not slot.required:
                continue
            if _slot_feasible_from_project_state(step, slot, planner_context):
                continue
            if _slot_feasible_from_upstream(step, slot, step_map):
                continue
            issues.append(_issue_for_missing_slot(step_key, step_type, slot.name))

    return issues


def _slot_feasible_from_project_state(step: dict, slot, planner_context) -> bool:
    from tune.core.binding.resolver import _file_matches_types

    resource_graph = getattr(planner_context, "resource_graph", None)
    files = list(getattr(planner_context, "files", []) or [])
    project_state = dict(getattr(planner_context, "project_state", {}) or {})
    summary = dict(project_state.get("summary") or {})
    step_type = step.get("step_type", "")

    if slot.name == "read1":
        return any(
            f.file_type in {"fastq", "fq"}
            and f.linked_experiment_id
            and f.read_number in {None, 1}
            for f in files
        )
    if slot.name == "read2":
        return any(
            f.file_type in {"fastq", "fq"}
            and f.linked_experiment_id
            and f.read_number == 2
            for f in files
        )
    if slot.name == "reads":
        return any(
            f.file_type in {"fastq", "fq"} and f.linked_experiment_id
            for f in files
        )
    if slot.name == "reference_fasta":
        return _resource_kind_ready(resource_graph, "reference_fasta") or any(
            _file_matches_types(f.path, slot.file_types) for f in files
        ) or bool(summary.get("has_reference_genome"))
    if slot.name == "annotation_gtf":
        known_path_keys = {
            str(item).strip()
            for item in list(summary.get("known_path_keys") or [])
            if str(item).strip()
        }
        resource_roles = {
            str(item).strip()
            for item in dict(summary.get("resource_role_counts") or {}).keys()
            if str(item).strip()
        }
        return _resource_kind_ready(resource_graph, "annotation_gtf") or any(
            _file_matches_types(f.path, slot.file_types) for f in files
        ) or bool({"annotation_gtf"} & known_path_keys) or bool(
            {"annotation_bundle"} & resource_roles
        )
    if slot.name == "index_prefix":
        aligner = _aligner_for_step_type(step_type)
        available_aligners = {
            str(item).strip()
            for item in list(summary.get("available_index_aligners") or [])
            if str(item).strip()
        }
        return _aligner_index_ready(resource_graph, aligner or "hisat2") or bool(
            aligner and aligner in available_aligners
        )
    if slot.name == "genome_dir":
        available_aligners = {
            str(item).strip()
            for item in list(summary.get("available_index_aligners") or [])
            if str(item).strip()
        }
        return _aligner_index_ready(resource_graph, "star") or "star" in available_aligners

    if slot.file_types == ["*"]:
        return False

    # Project files can satisfy generic typed slots like BAM/SAM inputs even
    # when they are not part of the core readiness graph.
    return any(_file_matches_types(f.path, slot.file_types) for f in files)


def _slot_feasible_from_upstream(step: dict, slot, step_map: dict[str, dict]) -> bool:
    from tune.core.binding.resolver import _file_matches_types
    from tune.core.registry import get_step_type

    if getattr(slot, "from_upstream_dir", False):
        return bool(step.get("depends_on"))

    visited: set[str] = set()
    queue: list[str] = list(step.get("depends_on") or [])

    while queue:
        dep_key = queue.pop(0)
        if dep_key in visited:
            continue
        visited.add(dep_key)

        dep_step = step_map.get(dep_key)
        if dep_step is None:
            continue
        dep_defn = get_step_type(dep_step.get("step_type", ""))
        if dep_defn is None:
            continue

        for out_slot in dep_defn.output_slots:
            if out_slot.name == slot.name:
                return True
            if slot.accepted_roles and out_slot.artifact_role in slot.accepted_roles:
                return True
            if out_slot.file_types == ["*"] or slot.file_types == ["*"]:
                continue
            for out_type in out_slot.file_types:
                if any(_file_matches_types(f"dummy.{out_type}", [in_type]) for in_type in slot.file_types):
                    return True

        queue.extend(dep_step.get("depends_on") or [])

    return False


def _resource_kind_ready(resource_graph: ResourceGraph | None, kind: str) -> bool:
    if resource_graph is None:
        return False
    for node_id in resource_graph.by_kind.get(kind, []):
        node = resource_graph.nodes.get(node_id)
        if node and node.status == "ready":
            return True
    return False


def _aligner_index_ready(resource_graph: ResourceGraph | None, aligner: str) -> bool:
    if resource_graph is None:
        return False
    for node_id in resource_graph.by_kind.get("aligner_index", []):
        node = resource_graph.nodes.get(node_id)
        if not node:
            continue
        if _aligner_from_node_id(node.id) == aligner and node.status == "ready":
            return True
    return False


def _aligner_from_node_id(node_id: str) -> str:
    parts = node_id.split(":")
    return parts[1] if len(parts) >= 2 else "hisat2"


def _step_uses_aligner(step_type: str, aligner: str) -> bool:
    """Return True if the step type uses the given aligner."""
    _map: dict[str, set[str]] = {
        "hisat2": {"align.hisat2"},
        "star": {"align.star"},
        "bwa": {"align.bwa"},
        "bowtie2": {"align.bowtie2"},
    }
    return step_type in _map.get(aligner, set())


def _aligner_for_step_type(step_type: str) -> str | None:
    for aligner in _ALIGNER_STEP_MAP:
        if _step_uses_aligner(step_type, aligner):
            return aligner
    return None


def _aligners_required_by_plan(plan: list[dict]) -> set[str]:
    required: set[str] = set()
    for step in plan:
        step_type = step.get("step_type", "")
        for aligner in _ALIGNER_STEP_MAP:
            if _step_uses_aligner(step_type, aligner):
                required.add(aligner)
    return required


def _preferred_aligners_from_graph(resource_graph: ResourceGraph | None) -> list[str]:
    if resource_graph is None:
        return []

    preferred: list[str] = []
    for node_id in resource_graph.by_kind.get("aligner_index", []):
        aligner = _aligner_from_node_id(node_id)
        if aligner not in preferred:
            preferred.append(aligner)
    return preferred


def _normalize_plan_aligners(
    plan: list[dict],
    resource_graph: ResourceGraph | None,
) -> tuple[list[dict], list[str]]:
    """Constrain aligner choices to the single aligner inferred by the resource graph."""
    preferred_aligners = _preferred_aligners_from_graph(resource_graph)
    if len(preferred_aligners) != 1:
        return list(plan), []

    preferred = preferred_aligners[0]
    if preferred != "hisat2":
        return list(plan), []

    warnings: list[str] = []
    normalized: list[dict] = []
    rewrote_star = False

    for step in plan:
        if step.get("step_type") != "align.star":
            normalized.append(dict(step))
            continue

        rewritten = dict(step)
        rewritten["step_type"] = "align.hisat2"
        params = dict(rewritten.get("params") or {})
        params.pop("two_pass_mode", None)
        rewritten["params"] = params
        rewritten["display_name"] = "HISAT2 align"
        normalized.append(rewritten)
        rewrote_star = True
        warnings.append(
            "Normalized align.star to align.hisat2 because the resource graph only inferred hisat2 for this project."
        )

    if rewrote_star and not any(step.get("step_type") == "align.star" for step in normalized):
        removed_step_keys: set[str] = set()
        pruned: list[dict] = []
        for step in normalized:
            if step.get("step_type") == "util.star_genome_generate" and step.get("_rr_injected"):
                step_key = str(step.get("step_key") or "")
                if step_key:
                    removed_step_keys.add(step_key)
                warnings.append(
                    "Removed injected util.star_genome_generate because no align.star steps remain after aligner normalization."
                )
                continue
            pruned.append(step)

        if removed_step_keys:
            for step in pruned:
                deps = list(step.get("depends_on") or [])
                filtered_deps = [dep for dep in deps if dep not in removed_step_keys]
                if filtered_deps != deps:
                    step["depends_on"] = filtered_deps
            normalized = pruned

    return normalized, warnings
