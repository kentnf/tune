"""Procrastinate task definitions."""
from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import procrastinate

from tune.workers.app import app

log = logging.getLogger(__name__)


def _step_display_name(step: dict) -> str:
    """Return a human-readable name for a step, supporting both plan formats."""
    return (step.get("name") or step.get("display_name") or
            step.get("step_key") or step.get("step_type") or "unknown_step")


def _plan_step_key(step: dict) -> str:
    """Return the stable logical key for a plan step."""
    return (step.get("step_key") or step.get("name") or "").strip()


async def _load_known_path_bindings(project_id: str) -> dict[str, str]:
    """Return {slot_name: path} for project resource bindings.

    Main source:
    - KnownPath for explicit reference / annotation choices
    - DerivedResource for built aligner indices
    Legacy index KnownPath entries remain compatibility fallback only.
    """
    if not project_id:
        return {}
    try:
        from tune.core.database import get_session_factory
        from tune.core.binding.resolver import load_registered_resource_bindings
        async with get_session_factory()() as session:
            return await load_registered_resource_bindings(project_id, session)
    except Exception:
        log.exception("_load_known_path_bindings failed for project %s", project_id)
        return {}

def _topological_sort(steps: list[dict]) -> list[dict]:
    """Sort steps in topological order based on depends_on using Kahn's algorithm.

    Returns steps unchanged if no step declares depends_on (backward-compatible).
    Raises ValueError on cycle.
    Supports both legacy (name) and typed plan (step_key) formats.
    """
    if not any(step.get("depends_on") for step in steps):
        return steps

    def _step_id(step: dict) -> str:
        return (step.get("name") or step.get("step_key") or "").lower()

    name_to_step = {_step_id(step): step for step in steps}
    in_degree: dict[str, int] = {name: 0 for name in name_to_step}
    adjacency: dict[str, list[str]] = {name: [] for name in name_to_step}

    for step in steps:
        step_lower = _step_id(step)
        for dep in step.get("depends_on") or []:
            dep_lower = dep.lower()
            if dep_lower in name_to_step:
                in_degree[step_lower] += 1
                adjacency[dep_lower].append(step_lower)

    queue = [name for name, deg in in_degree.items() if deg == 0]
    result: list[dict] = []

    while queue:
        node = queue.pop(0)
        result.append(name_to_step[node])
        for neighbor in adjacency[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(result) != len(steps):
        cycle_nodes = [name for name, deg in in_degree.items() if deg > 0]
        raise ValueError(f"circular dependency: {', '.join(cycle_nodes)}")

    return result


def _sanitize_fanout_token(value: str) -> str:
    token = re.sub(r"[^a-zA-Z0-9]+", "_", value).strip("_").lower()
    return token[:48] or "unit"


def _collect_per_sample_lineages(project_files: list[dict]) -> list[dict[str, Any]]:
    """Build deterministic lineage units for PER_SAMPLE expansion.

    Prefer experiment-level fan-out when experiment linkage exists, otherwise
    fall back to sample-level expansion. This keeps biological replicate runs
    distinct while still supporting projects that only have sample linkage.
    """
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


def _expand_plan_for_fanout(job_plan: list[dict], project_files: list[dict]) -> list[dict]:
    """Legacy helper kept for tests and binding code; delegates to orchestration."""
    from tune.core.orchestration import build_execution_payload, extract_execution_nodes

    if not job_plan or any(step.get("_fanout_expanded") for step in job_plan):
        return job_plan

    bundle = build_execution_payload({"steps": job_plan}, project_files)
    return extract_execution_nodes(bundle.expanded_dag)


async def _resolve_step_inputs(
    step: dict,
    step_dirs: dict[str, Path],
    project_files: list[dict],
    project_id: str,
) -> list[str]:
    """Resolve input file paths for a step.

    Priority order:
    1. Files from dependency step output directories filtered by tool's expected types.
    2. Project files filtered by expected types (fallback when no dep provides needed files).
    3. KnownPath records for the project (always appended).
    """
    from tune.core.analysis.tools import find_tool

    tool_def = find_tool(step.get("tool", ""))
    expected_types: list[str] = tool_def.file_types_in if tool_def else []

    resolved: list[str] = []

    # Priority 1: files from dependency step output directories
    for dep_name in step.get("depends_on") or []:
        dep_dir = step_dirs.get(dep_name.lower())
        if dep_dir and dep_dir.exists():
            if tool_def and getattr(tool_def, "input_mode", "files") == "dir":
                # Tool expects a directory path (e.g. MultiQC), not individual files
                resolved.append(str(dep_dir))
            else:
                for f in sorted(dep_dir.rglob("*")):
                    if f.is_file():
                        suffix = f.suffix.lstrip(".").lower()
                        if not expected_types or suffix in expected_types:
                            resolved.append(str(f))

    # Priority 2: fallback to project files when no dependency provided files
    if not resolved:
        for pf in project_files:
            ft = pf.get("file_type", "")
            if not expected_types or ft in expected_types:
                resolved.append(pf["path"])

    # Priority 3: always inject KnownPath records for the project
    if project_id:
        try:
            from tune.core.database import get_session_factory
            from tune.core.models import KnownPath
            from sqlalchemy import select
            async with get_session_factory()() as session:
                rows = (await session.execute(
                    select(KnownPath).where(KnownPath.project_id == project_id)
                )).scalars().all()
                for row in rows:
                    resolved.append(row.path)
        except Exception:
            pass  # Best-effort

    return resolved


async def _load_step_bindings(step_run_id: str) -> dict[str, Any]:
    """Load resolved InputBinding records for a step.

    Returns a binding map where duplicate slot names are preserved as lists so
    ``multiple=True`` slots can pass all matched inputs into the renderer.
    """
    try:
        from tune.core.database import get_session_factory
        from tune.core.models import InputBinding
        from sqlalchemy import select

        async with get_session_factory()() as session:
            rows = (await session.execute(
                select(InputBinding).where(
                    InputBinding.step_id == step_run_id,
                    InputBinding.status == "resolved",
                )
            )).scalars().all()

            bindings: dict[str, Any] = {}
            for row in rows:
                if not row.resolved_path:
                    continue
                existing = bindings.get(row.slot_name)
                if existing is None:
                    bindings[row.slot_name] = row.resolved_path
                elif isinstance(existing, list):
                    if row.resolved_path not in existing:
                        existing.append(row.resolved_path)
                elif existing != row.resolved_path:
                    bindings[row.slot_name] = [existing, row.resolved_path]
            return bindings
    except Exception:
        log.exception("_load_step_bindings: failed for step_run_id=%s", step_run_id)
    return {}


async def _ensure_step_run_records(job_id: str, steps: list[dict]) -> None:
    """Ensure each plan step has a persistent AnalysisStepRun id."""
    if not job_id or not steps:
        return
    try:
        from sqlalchemy import select

        from tune.core.database import get_session_factory
        from tune.core.models import AnalysisStepRun

        async with get_session_factory()() as session:
            rows = (await session.execute(
                select(AnalysisStepRun).where(AnalysisStepRun.job_id == job_id)
            )).scalars().all()
            by_key = {row.step_key: row for row in rows if getattr(row, "step_key", "")}
            dirty = False

            for step in steps:
                step_key = _plan_step_key(step)
                if not step_key:
                    continue

                step_type = step.get("step_type") or step.get("tool") or ""
                display_name = step.get("display_name") or step.get("name") or step_key
                depends_on = list(step.get("depends_on") or [])
                params_json = step.get("params") or {}

                record = by_key.get(step_key)
                if record is None:
                    record = AnalysisStepRun(
                        id=str(uuid.uuid4()),
                        job_id=job_id,
                        step_key=step_key,
                        step_type=step_type,
                        display_name=display_name,
                        status="pending",
                        depends_on=depends_on,
                        params_json=params_json,
                    )
                    session.add(record)
                    by_key[step_key] = record
                    dirty = True
                else:
                    if record.step_type != step_type:
                        record.step_type = step_type
                        dirty = True
                    if record.display_name != display_name:
                        record.display_name = display_name
                        dirty = True
                    if record.depends_on != depends_on:
                        record.depends_on = depends_on
                        dirty = True
                    if record.params_json != params_json:
                        record.params_json = params_json
                        dirty = True

                if step.get("_run_id") != record.id:
                    step["_run_id"] = record.id
                if step.get("run_id") != record.id:
                    step["run_id"] = record.id

            if dirty:
                await session.commit()
    except Exception:
        log.exception("_ensure_step_run_records failed for job %s", job_id)


async def _ensure_step_run_id(job_id: str, step: dict[str, Any]) -> str:
    """Ensure one step dict carries its persistent AnalysisStepRun id."""
    step_key = _plan_step_key(step)
    existing = step.get("_run_id") or step.get("run_id") or ""
    if existing or not job_id or not step_key:
        return existing

    try:
        from sqlalchemy import select

        from tune.core.database import get_session_factory
        from tune.core.models import AnalysisStepRun

        async with get_session_factory()() as session:
            row = (await session.execute(
                select(AnalysisStepRun).where(
                    AnalysisStepRun.job_id == job_id,
                    AnalysisStepRun.step_key == step_key,
                )
            )).scalar_one_or_none()
            if row is None:
                await _ensure_step_run_records(job_id, [step])
                row = (await session.execute(
                    select(AnalysisStepRun).where(
                        AnalysisStepRun.job_id == job_id,
                        AnalysisStepRun.step_key == step_key,
                    )
                )).scalar_one_or_none()
            if row is None:
                return ""
            step["_run_id"] = row.id
            step["run_id"] = row.id
            return row.id
    except Exception:
        log.exception("_ensure_step_run_id failed for job %s step %s", job_id, step_key)
        return ""


async def _set_step_run_status(step_run_id: str, new_status: str, *, force: bool = False) -> bool:
    """Persist a step status update, using the workflow helper when possible."""
    if not step_run_id:
        return False

    try:
        from sqlalchemy import select

        from tune.core.database import get_session_factory
        from tune.core.models import AnalysisStepRun
        from tune.core.workflow import transition_step

        async with get_session_factory()() as session:
            updated = await transition_step(step_run_id, new_status, session)
            if not updated and force:
                step = (
                    await session.execute(
                        select(AnalysisStepRun).where(AnalysisStepRun.id == step_run_id)
                    )
                ).scalar_one_or_none()
                if step is None:
                    return False
                step.status = new_status
                if new_status == "running" and getattr(step, "started_at", None) is None:
                    step.started_at = datetime.now(timezone.utc)
                if new_status in {"succeeded", "failed", "skipped"}:
                    step.finished_at = datetime.now(timezone.utc)
                updated = True
            if updated:
                await session.commit()
            return bool(updated)
    except Exception:
        log.exception("_set_step_run_status failed for step %s -> %s", step_run_id, new_status)
        return False


async def _set_job_current_step(job_id: str, step_run_id: str | None) -> bool:
    """Persist the currently active step for operator visibility and resume anchors."""
    if not job_id:
        return False

    try:
        from sqlalchemy import select

        from tune.core.database import get_session_factory
        from tune.core.models import AnalysisJob

        async with get_session_factory()() as session:
            job = (
                await session.execute(
                    select(AnalysisJob).where(AnalysisJob.id == job_id)
                )
            ).scalar_one_or_none()
            if job is None:
                return False
            job.current_step_id = step_run_id
            job.last_progress_at = datetime.now(timezone.utc)
            await session.commit()
            return True
    except Exception:
        log.exception("_set_job_current_step failed for job %s step %s", job_id, step_run_id)
        return False


def _load_resume_anchor(job) -> dict | None:
    payload = getattr(job, "pending_interaction_payload_json", None)
    if not isinstance(payload, dict):
        return None

    anchor = payload.get("resume_anchor")
    if not isinstance(anchor, dict):
        return None

    step_key = str(anchor.get("step_key") or "").strip()
    if not step_key:
        return None

    return {
        "type": "resume_anchor",
        "step_key": step_key,
        "mode": str(anchor.get("mode") or "step_reenter"),
        "reason": str(anchor.get("reason") or ""),
        "requested_by": str(anchor.get("requested_by") or ""),
    }


def _build_resolver_context_steps(
    job_plan: list[dict],
    target_step: dict,
    current_step_dir: Path,
    step_dirs: dict[str, Path],
) -> list[dict]:
    """Build the minimal resolver context for one target step and its ancestors."""
    target_key = _plan_step_key(target_step).lower()
    if not target_key:
        return []

    plan_map = {
        _plan_step_key(step).lower(): step
        for step in job_plan
        if _plan_step_key(step)
    }
    needed: set[str] = set()
    queue = [target_key]

    while queue:
        step_key = queue.pop(0)
        if step_key in needed:
            continue
        needed.add(step_key)
        step = plan_map.get(step_key)
        if step:
            queue.extend(dep.lower() for dep in (step.get("depends_on") or []))

    context_steps: list[dict] = []
    for step in job_plan:
        step_key = _plan_step_key(step)
        if not step_key or step_key.lower() not in needed:
            continue

        context_step = dict(step)
        context_step["step_key"] = step_key
        context_step["run_id"] = step.get("run_id") or step.get("_run_id")

        if step_key.lower() == target_key:
            context_step["output_dir"] = str(current_step_dir)
        else:
            dep_dir = step_dirs.get(step_key.lower())
            if dep_dir:
                context_step["output_dir"] = str(dep_dir)
        context_steps.append(context_step)

    return context_steps


async def _resolve_semantic_bindings_for_step(
    job_id: str,
    job_plan: list[dict],
    step: dict,
    current_step_dir: Path,
    step_dirs: dict[str, Path],
    project_files: list[dict],
) -> None:
    """Materialize InputBinding rows for the current step before rendering."""
    step_key = _plan_step_key(step)
    if not job_id or not step_key or not step.get("step_type"):
        return

    resolver_steps = _build_resolver_context_steps(
        job_plan=job_plan,
        target_step=step,
        current_step_dir=current_step_dir,
        step_dirs=step_dirs,
    )
    if not resolver_steps:
        return

    try:
        from tune.core.binding.resolver import resolve_bindings
        from tune.core.database import get_session_factory

        async with get_session_factory()() as session:
            unresolved = await resolve_bindings(
                job_id=job_id,
                steps=resolver_steps,
                project_files=project_files,
                db=session,
                target_step_keys={step_key},
            )
            await session.commit()
        if unresolved:
            log.debug(
                "_resolve_semantic_bindings_for_step: unresolved slots for job %s step %s: %s",
                job_id, step_key, unresolved,
            )
    except Exception:
        log.exception(
            "_resolve_semantic_bindings_for_step failed for job %s step %s",
            job_id,
            step_key,
        )


def _is_missing_binding_renderer_error(exc: Exception) -> bool:
    message = str(exc or "").lower()
    return "not bound" in message or "missing required binding" in message


async def _preview_semantic_bindings_for_step(
    job_id: str,
    job_plan: list[dict],
    step: dict,
    current_step_dir: Path,
    step_dirs: dict[str, Path],
    project_files: list[dict],
    project_id: str,
) -> dict[str, Any]:
    """Compute a best-effort binding preview without depending on DB writes.

    This is an execution-time safety net for resumed jobs: if persisted
    InputBinding rows are missing or stale, we still want typed renderers to use
    deterministic fact-layer bindings instead of falling back to the LLM.
    """
    step_key = _plan_step_key(step)
    step_type = step.get("step_type") or ""
    if not job_id or not step_key or not step_type:
        return {}

    try:
        from tune.core.binding.resolver import (
            _collect_transitive_dep_keys,
            _select_semantic_candidates,
            load_registered_resource_bindings,
        )
        from tune.core.database import get_session_factory
        from tune.core.registry import get_step_type

        defn = get_step_type(step_type)
        if defn is None:
            return {}

        context_steps = _build_resolver_context_steps(
            job_plan=job_plan,
            target_step=step,
            current_step_dir=current_step_dir,
            step_dirs=step_dirs,
        )
        steps_by_key = {
            _plan_step_key(context_step).lower(): context_step
            for context_step in context_steps
            if _plan_step_key(context_step)
        }
        transitive_dep_keys = _collect_transitive_dep_keys(step, steps_by_key)
        preferred_lineage = dict(step.get("_preferred_lineage") or {}) or None

        async with get_session_factory()() as session:
            kp_bindings = (
                await load_registered_resource_bindings(project_id, session)
                if project_id
                else {}
            )
            preview: dict[str, Any] = {}
            for slot in defn.input_slots:
                candidates = await _select_semantic_candidates(
                    job_id=job_id,
                    dep_keys=transitive_dep_keys,
                    slot=slot,
                    project_id=project_id or None,
                    project_files=project_files,
                    kp_bindings=kp_bindings,
                    db=session,
                    preferred_lineage=preferred_lineage,
                )
                if not candidates:
                    continue
                if getattr(slot, "multiple", False):
                    preview[slot.name] = [
                        candidate["file_path"]
                        for candidate in candidates
                        if candidate.get("file_path")
                    ]
                else:
                    preview[slot.name] = candidates[0]["file_path"]
            return preview
    except Exception:
        log.exception(
            "_preview_semantic_bindings_for_step failed for job %s step %s",
            job_id,
            step_key,
        )
        return {}


async def _persist_step_run_bindings(step_run_id: str, bindings: dict[str, str]) -> None:
    """Persist the renderer binding snapshot on the step run for inspection/resume."""
    if not step_run_id:
        return
    try:
        from sqlalchemy import select

        from tune.core.database import get_session_factory
        from tune.core.models import AnalysisStepRun

        async with get_session_factory()() as session:
            row = (await session.execute(
                select(AnalysisStepRun).where(AnalysisStepRun.id == step_run_id)
            )).scalar_one_or_none()
            if row is None:
                return
            snapshot = dict(bindings)
            if row.bindings_json != snapshot:
                row.bindings_json = snapshot
                await session.commit()
    except Exception:
        log.exception("_persist_step_run_bindings failed for step_run_id=%s", step_run_id)


def _resolve_command_type_for_step(step: dict, command: str) -> str:
    step_type = step.get("step_type") if isinstance(step, dict) else None
    if step_type:
        try:
            from tune.core.registry import get_step_type as _get_step_type

            defn = _get_step_type(step_type)
            if defn and defn.safety_policy.command_type:
                return defn.safety_policy.command_type
        except Exception:
            log.debug(
                "_resolve_command_type_for_step: registry lookup failed for step_type=%s",
                step_type,
                exc_info=True,
            )
    return get_command_type(command)


async def _infer_artifact_lineage(
    step_run_id: str,
    project_files: list[dict],
    session,
) -> dict[str, object] | None:
    """Infer sample / experiment lineage for artifacts produced by one step."""
    if not step_run_id:
        return None
    try:
        from tune.core.models import InputBinding
        from sqlalchemy import select

        rows = (await session.execute(
            select(InputBinding).where(
                InputBinding.step_id == step_run_id,
                InputBinding.status == "resolved",
            )
        )).scalars().all()
        if not rows:
            return None

        by_path = {pf.get("path"): pf for pf in project_files if pf.get("path")}
        sample_ids: set[str] = set()
        experiment_ids: set[str] = set()
        sample_names: set[str] = set()
        read_numbers: set[int] = set()

        for row in rows:
            match_metadata = getattr(row, "match_metadata_json", None) or {}
            lineage = match_metadata.get("lineage") or {}
            sample_id = lineage.get("sample_id")
            experiment_id = lineage.get("experiment_id")
            sample_name = lineage.get("sample_name")
            read_number = lineage.get("read_number")

            project_file = by_path.get(getattr(row, "resolved_path", None))
            if project_file:
                sample_id = sample_id or project_file.get("linked_sample_id")
                experiment_id = experiment_id or project_file.get("linked_experiment_id")
                sample_name = sample_name or project_file.get("sample_name")
                read_number = read_number or project_file.get("read_number")

            if sample_id:
                sample_ids.add(str(sample_id))
            if experiment_id:
                experiment_ids.add(str(experiment_id))
            if sample_name:
                sample_names.add(str(sample_name))
            if isinstance(read_number, int):
                read_numbers.add(read_number)

        lineage_result: dict[str, object] = {}
        if len(sample_ids) == 1:
            lineage_result["sample_id"] = next(iter(sample_ids))
        if len(experiment_ids) == 1:
            lineage_result["experiment_id"] = next(iter(experiment_ids))
        if len(sample_names) == 1:
            lineage_result["sample_name"] = next(iter(sample_names))
        if len(read_numbers) == 1:
            lineage_result["read_number"] = next(iter(read_numbers))
        return lineage_result or None
    except Exception:
        log.debug(
            "_infer_artifact_lineage: failed for step_run_id=%s",
            step_run_id,
            exc_info=True,
        )
        return None


def _binding_paths(binding_value: Any) -> list[str]:
    if isinstance(binding_value, list):
        return [str(item) for item in binding_value if item]
    if binding_value:
        return [str(binding_value)]
    return []


def _normalize_metadata_key(key: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(key or "").strip().lower()).strip("_")


def _normalize_metadata_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, tuple, set)):
        parts = [_normalize_metadata_value(item) for item in value]
        return ", ".join(part for part in parts if part)
    return str(value).strip()


def _metadata_lookup(metadata: dict[str, Any] | None, *keys: str) -> str:
    if not isinstance(metadata, dict) or not metadata:
        return ""
    normalized = {
        _normalize_metadata_key(raw_key): raw_value
        for raw_key, raw_value in metadata.items()
    }
    for key in keys:
        value = normalized.get(_normalize_metadata_key(key))
        normalized_value = _normalize_metadata_value(value)
        if normalized_value:
            return normalized_value
    return ""


def _fallback_sample_label(path_value: str, sample_name: str = "") -> str:
    if sample_name:
        return sample_name
    label = Path(path_value).name
    for suffix in (".bam", ".sam"):
        if label.endswith(suffix):
            label = label[: -len(suffix)]
    for suffix in ("_sorted", "Aligned.sortedByCoord.out"):
        if label.endswith(suffix):
            label = label[: -len(suffix)]
    return label.rstrip("_-") or Path(path_value).stem


_DESEQ2_FACTOR_SPECS: dict[str, list[tuple[str, tuple[str, ...]]]] = {
    "condition": [
        ("row", ("condition_label", "condition")),
        ("sample", ("condition",)),
        ("sample", ("group", "condition_group")),
        ("sample", ("treatment", "treatment_group")),
        ("sample", ("phenotype",)),
        ("sample", ("genotype", "cultivar", "variety")),
        ("experiment", ("condition",)),
        ("experiment", ("group", "condition_group")),
        ("experiment", ("treatment", "treatment_group")),
    ],
    "treatment": [
        ("sample", ("treatment", "treatment_group")),
        ("experiment", ("treatment", "treatment_group")),
    ],
    "group": [
        ("sample", ("group", "condition_group")),
        ("experiment", ("group", "condition_group")),
    ],
    "phenotype": [
        ("sample", ("phenotype",)),
        ("experiment", ("phenotype",)),
    ],
    "genotype": [
        ("sample", ("genotype", "cultivar", "variety")),
        ("experiment", ("genotype", "cultivar", "variety")),
    ],
    "strain": [
        ("sample", ("strain",)),
        ("experiment", ("strain",)),
    ],
    "cohort": [
        ("sample", ("cohort",)),
        ("experiment", ("cohort",)),
    ],
    "timepoint": [
        ("sample", ("timepoint", "time_point")),
        ("experiment", ("timepoint", "time_point")),
    ],
    "stage": [
        ("sample", ("stage", "development_stage", "dev_stage")),
        ("experiment", ("stage", "development_stage", "dev_stage")),
    ],
    "sex": [
        ("sample", ("sex",)),
        ("experiment", ("sex",)),
    ],
    "tissue": [
        ("sample", ("tissue",)),
        ("experiment", ("tissue",)),
    ],
    "batch": [
        ("experiment", ("batch", "batch_id")),
        ("sample", ("batch", "batch_id")),
    ],
    "replicate": [
        ("row", ("replicate_label", "replicate")),
        ("experiment", ("replicate", "replicate_id", "replicate_number", "biological_replicate", "bio_replicate", "rep")),
        ("sample", ("replicate", "replicate_id", "replicate_number", "biological_replicate", "bio_replicate", "rep")),
    ],
}

_DESEQ2_SIDECAR_FACTOR_FIELDS = [
    "condition",
    "treatment",
    "group",
    "phenotype",
    "genotype",
    "strain",
    "cohort",
    "timepoint",
    "stage",
    "sex",
    "tissue",
    "batch",
    "replicate",
]


def _resolve_deseq2_factor_value(row: dict[str, Any], factor_name: str) -> str:
    normalized_factor = _normalize_metadata_key(factor_name)
    if normalized_factor == "sample_name":
        return _normalize_metadata_value(row.get("sample_name"))
    if normalized_factor == "library_strategy":
        return _normalize_metadata_value(row.get("library_strategy"))
    if normalized_factor == "library_layout":
        return _normalize_metadata_value(row.get("library_layout"))
    if normalized_factor == "experiment_id":
        return _normalize_metadata_value(row.get("experiment_id"))
    if normalized_factor == "sample_id":
        return _normalize_metadata_value(row.get("sample_id"))

    for source_kind, candidate_keys in _DESEQ2_FACTOR_SPECS.get(normalized_factor, []):
        if source_kind == "row":
            for key in candidate_keys:
                value = _normalize_metadata_value(row.get(key))
                if value:
                    return value
        elif source_kind == "sample":
            value = _metadata_lookup(row.get("sample_attrs") or {}, *candidate_keys)
            if value:
                return value
        elif source_kind == "experiment":
            value = _metadata_lookup(row.get("experiment_attrs") or {}, *candidate_keys)
            if value:
                return value
    return ""


def _sidecar_factor_values(row: dict[str, Any]) -> dict[str, str]:
    values = {
        field_name: _resolve_deseq2_factor_value(row, field_name)
        for field_name in _DESEQ2_SIDECAR_FACTOR_FIELDS
    }
    values["sample_name"] = _normalize_metadata_value(row.get("sample_name"))
    values["library_strategy"] = _normalize_metadata_value(row.get("library_strategy"))
    values["library_layout"] = _normalize_metadata_value(row.get("library_layout"))
    values["experiment_id"] = _normalize_metadata_value(row.get("experiment_id"))
    values["sample_id"] = _normalize_metadata_value(row.get("sample_id"))
    return values


def _choose_condition_labels(rows: list[dict[str, Any]]) -> tuple[list[str], str]:
    from collections import Counter

    candidate_fields = [
        "condition",
        "group",
        "treatment",
        "phenotype",
        "genotype",
        "strain",
        "cohort",
        "timepoint",
        "stage",
        "sex",
        "tissue",
        "sample_name",
    ]
    candidate_scores = {
        "condition": 320,
        "group": 310,
        "treatment": 300,
        "phenotype": 290,
        "genotype": 280,
        "strain": 270,
        "cohort": 260,
        "timepoint": 250,
        "stage": 240,
        "sex": 220,
        "tissue": 210,
        "sample_name": 140,
    }

    best_labels: list[str] | None = None
    best_source = ""
    best_score: int | None = None

    for field_name in candidate_fields:
        labels = [_resolve_deseq2_factor_value(row, field_name) for row in rows]
        if not all(labels):
            continue

        counts = Counter(labels)
        if len(counts) < 2:
            continue

        repeated_groups = sum(1 for count in counts.values() if count >= 2)
        singleton_groups = sum(1 for count in counts.values() if count == 1)
        if repeated_groups == 0 and len(rows) > 2:
            continue

        score = candidate_scores[field_name] + repeated_groups * 25 - singleton_groups * 6 + min(counts.values()) * 3
        if best_score is None or score > best_score:
            best_score = score
            best_labels = labels
            best_source = field_name if field_name != "sample_name" else "sample_name"

    if best_labels is not None:
        source_name = best_source if best_source == "sample_name" else f"factor.{best_source}"
        return best_labels, source_name

    fallback_labels = [
        _fallback_sample_label(str(row.get("bam_path") or ""), _normalize_metadata_value(row.get("sample_name")))
        for row in rows
    ]
    return fallback_labels, "fallback.sample_label"




def _derive_replicate_label(row: dict[str, Any]) -> str:
    value = _resolve_deseq2_factor_value(row, "replicate")
    if value:
        return value
    experiment_id = _normalize_metadata_value(row.get("experiment_id"))
    if experiment_id:
        return experiment_id[:8]
    return _fallback_sample_label(str(row.get("bam_path") or ""))


async def _write_featurecounts_sample_metadata_sidecar(
    *,
    step_run_id: str,
    step_bindings: dict[str, Any],
    project_files: list[dict],
    step_dir: Path,
) -> str | None:
    aligned_bams = _binding_paths(step_bindings.get("aligned_bam"))
    counts_path = step_dir / "counts.txt"
    if not step_run_id or not aligned_bams or not counts_path.exists():
        return None

    import csv
    import json
    from sqlalchemy import select
    from tune.core.database import get_session_factory
    from tune.core.models import Experiment, InputBinding, Sample

    project_file_by_path = {
        str(project_file.get("path")): project_file
        for project_file in project_files
        if project_file.get("path")
    }

    async with get_session_factory()() as session:
        binding_rows = (await session.execute(
            select(InputBinding).where(
                InputBinding.step_id == step_run_id,
                InputBinding.slot_name == "aligned_bam",
                InputBinding.status == "resolved",
            )
        )).scalars().all()

        binding_rows_by_path: dict[str, list[Any]] = {}
        experiment_ids: set[str] = set()
        sample_ids: set[str] = set()

        for binding in binding_rows:
            resolved_path = str(getattr(binding, "resolved_path", "") or "").strip()
            if not resolved_path:
                continue
            binding_rows_by_path.setdefault(resolved_path, []).append(binding)
            match_metadata = getattr(binding, "match_metadata_json", None) or {}
            lineage = dict(match_metadata.get("lineage") or {})
            project_file = project_file_by_path.get(resolved_path) or {}
            experiment_id = _normalize_metadata_value(lineage.get("experiment_id") or project_file.get("linked_experiment_id"))
            sample_id = _normalize_metadata_value(lineage.get("sample_id") or project_file.get("linked_sample_id"))
            if experiment_id:
                experiment_ids.add(experiment_id)
            if sample_id:
                sample_ids.add(sample_id)

        experiment_map: dict[str, dict[str, Any]] = {}
        sample_map: dict[str, dict[str, Any]] = {}

        if experiment_ids:
            exp_rows = (await session.execute(
                select(Experiment, Sample)
                .join(Sample, Experiment.sample_id == Sample.id)
                .where(Experiment.id.in_(sorted(experiment_ids)))
            )).all()
            for experiment, sample in exp_rows:
                experiment_map[str(experiment.id)] = {
                    "sample_id": str(experiment.sample_id),
                    "library_strategy": experiment.library_strategy or "",
                    "library_layout": experiment.library_layout or "",
                    "attrs": dict(experiment.attrs or {}),
                }
                sample_map[str(sample.id)] = {
                    "sample_name": sample.sample_name or "",
                    "attrs": dict(sample.attrs or {}),
                }

        if sample_ids - sample_map.keys():
            sample_rows = (await session.execute(
                select(Sample).where(Sample.id.in_(sorted(sample_ids - sample_map.keys())))
            )).scalars().all()
            for sample in sample_rows:
                sample_map[str(sample.id)] = {
                    "sample_name": sample.sample_name or "",
                    "attrs": dict(sample.attrs or {}),
                }

    rows: list[dict[str, Any]] = []
    for bam_path in aligned_bams:
        binding_candidates = binding_rows_by_path.get(bam_path) or []
        binding = binding_candidates.pop(0) if binding_candidates else None
        match_metadata = getattr(binding, "match_metadata_json", None) or {}
        lineage = dict(match_metadata.get("lineage") or {})
        project_file = project_file_by_path.get(bam_path) or {}

        experiment_id = _normalize_metadata_value(lineage.get("experiment_id") or project_file.get("linked_experiment_id"))
        sample_id = _normalize_metadata_value(lineage.get("sample_id") or project_file.get("linked_sample_id"))
        experiment_info = experiment_map.get(experiment_id, {})
        if not sample_id:
            sample_id = _normalize_metadata_value(experiment_info.get("sample_id"))
        sample_info = sample_map.get(sample_id, {})

        sample_name = _normalize_metadata_value(
            lineage.get("sample_name")
            or project_file.get("sample_name")
            or sample_info.get("sample_name")
        )
        sample_attrs = dict(sample_info.get("attrs") or {})
        experiment_attrs = dict(experiment_info.get("attrs") or {})
        library_strategy = _normalize_metadata_value(
            experiment_info.get("library_strategy") or project_file.get("library_strategy")
        )
        library_layout = _normalize_metadata_value(
            experiment_info.get("library_layout") or project_file.get("library_layout")
        )
        read_number = _normalize_metadata_value(
            lineage.get("read_number") or project_file.get("read_number")
        )

        rows.append({
            "count_column": bam_path,
            "bam_path": bam_path,
            "bam_basename": Path(bam_path).name,
            "sample_id": sample_id,
            "sample_name": sample_name,
            "experiment_id": experiment_id,
            "library_strategy": library_strategy,
            "library_layout": library_layout,
            "read_number": read_number,
            "sample_attrs": sample_attrs,
            "experiment_attrs": experiment_attrs,
        })

    if not rows:
        return None

    condition_labels, condition_source = _choose_condition_labels(rows)
    for row, condition_label in zip(rows, condition_labels):
        row["sample_name"] = row["sample_name"] or _fallback_sample_label(
            str(row.get("bam_path") or "")
        )
        row["condition_label"] = condition_label
        row["condition_source"] = condition_source
        row["replicate_label"] = _derive_replicate_label(row)
        row["condition"] = condition_label
        row["replicate"] = row["replicate_label"]
        row.update(_sidecar_factor_values(row))
        row["condition"] = row.get("condition") or condition_label
        row["replicate"] = row.get("replicate") or row["replicate_label"]

    sidecar_path = step_dir / "counts.sample_metadata.tsv"
    fieldnames = [
        "count_column",
        "bam_path",
        "bam_basename",
        "sample_id",
        "sample_name",
        "experiment_id",
        "library_strategy",
        "library_layout",
        "read_number",
        "replicate_label",
        "condition_label",
        "condition_source",
        *_DESEQ2_SIDECAR_FACTOR_FIELDS,
        "sample_attrs_json",
        "experiment_attrs_json",
    ]
    with sidecar_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="	")
        writer.writeheader()
        for row in rows:
            payload = {
                "count_column": row["count_column"],
                "bam_path": row["bam_path"],
                "bam_basename": row["bam_basename"],
                "sample_id": row["sample_id"],
                "sample_name": row["sample_name"],
                "experiment_id": row["experiment_id"],
                "library_strategy": row["library_strategy"],
                "library_layout": row["library_layout"],
                "read_number": row["read_number"],
                "replicate_label": row["replicate_label"],
                "condition_label": row["condition_label"],
                "condition_source": row["condition_source"],
                "sample_attrs_json": json.dumps(row.get("sample_attrs") or {}, sort_keys=True, ensure_ascii=True),
                "experiment_attrs_json": json.dumps(row.get("experiment_attrs") or {}, sort_keys=True, ensure_ascii=True),
            }
            for field_name in _DESEQ2_SIDECAR_FACTOR_FIELDS:
                payload[field_name] = row.get(field_name, "")
            writer.writerow(payload)
    return str(sidecar_path)


async def _load_pending_decision(job) -> dict | None:
    """Load the pending auth or repair decision from DB for a suspended job.

    Returns a resume context dict, or None if the decision is not yet available.

    Auth context:   {"type": "auth",   "step_key": str, "authorized": bool, "command": str}
    Repair context: {"type": "repair", "step_key": str, "repair_command": str, "should_continue": bool}
    """
    from tune.core.database import get_session_factory
    from tune.core.models import CommandAuthorizationRequest, RepairRequest
    from sqlalchemy import select

    step_key = job.pending_step_key or ""

    def _effective_auth_command(req) -> str:
        return (
            getattr(req, "effective_command", None)
            or getattr(req, "current_command_text", None)
            or getattr(req, "command_text", "")
        )

    if job.pending_auth_request_id:
        try:
            async with get_session_factory()() as session:
                req = (await session.execute(
                    select(CommandAuthorizationRequest).where(
                        CommandAuthorizationRequest.id == job.pending_auth_request_id
                    )
                )).scalar_one_or_none()
                if req is None or req.status == "pending":
                    return None  # Decision not yet made
                return {
                    "type": "auth",
                    "step_key": step_key,
                    "authorized": req.status == "approved",
                    "command": _effective_auth_command(req),
                }
        except Exception:
            log.exception("_load_pending_decision: failed to load auth request for job %s", job.id)
            return None

    if job.pending_repair_request_id:
        try:
            async with get_session_factory()() as session:
                req = (await session.execute(
                    select(RepairRequest).where(
                        RepairRequest.id == job.pending_repair_request_id
                    )
                )).scalar_one_or_none()
                if req is None or req.status == "pending":
                    return None  # Decision not yet made
                resolution = req.human_resolution_json or {}
                return {
                    "type": "repair",
                    "step_key": step_key,
                    "repair_command": resolution.get("command", ""),
                    "should_continue": resolution.get("should_continue", False),
                    # Phase 6: include original stderr so RepairMemory can be written
                    # after a human-provided command succeeds.
                    "stderr": req.stderr_excerpt or "",
                }
        except Exception:
            log.exception("_load_pending_decision: failed to load repair request for job %s", job.id)
            return None

    return None


@app.task(queue="analysis")
async def resume_job_task(job_id: str) -> None:
    """Resume a job that was suspended waiting for authorization or repair.

    Authorization / repair resumes must preserve the pending request fields so
    run_analysis_task() can reload the resolved decision. Only plain
    "interrupted" jobs without a pending human decision need to be re-queued.
    """
    from sqlalchemy import select
    from tune.core.database import get_session_factory
    from tune.core.models import AnalysisJob
    from tune.core.workflow import transition_job

    async with get_session_factory()() as session:
        job = (await session.execute(
            select(AnalysisJob).where(AnalysisJob.id == job_id)
        )).scalar_one_or_none()
        if not job:
            log.warning("resume_job_task: job %s not found", job_id)
            return
        has_pending_decision = bool(job.pending_auth_request_id or job.pending_repair_request_id)
        if not has_pending_decision and job.status not in (
            "waiting_for_authorization",
            "waiting_for_repair",
            "interrupted",
        ):
            log.warning(
                "resume_job_task: job %s has unexpected status %s, skipping",
                job_id, job.status,
            )
            return
        ok = True
        if not has_pending_decision and job.status == "interrupted":
            ok = await transition_job(job_id, "queued", session)
            if ok:
                await session.commit()

    if ok:
        from tune.workers.defer import defer_async_with_fallback

        await defer_async_with_fallback(run_analysis_task, job_id=job_id)


@app.task(queue="analysis")
async def prepare_environment_task(job_id: str) -> None:
    """Build the Pixi environment for a job before execution starts.

    Collects pixi_packages from all step types in the plan, checks the
    env cache by hash, and installs if needed. On success updates
    env_status='ready' and env_spec_hash, then re-queues run_analysis_task.
    On failure sets env_status='failed'.
    """
    import os
    import shutil
    import sys
    import platform as _platform
    from pathlib import Path
    from sqlalchemy import select
    from tune.core.database import get_session_factory
    from tune.core.models import AnalysisJob
    from tune.core.env_planner import build_env_spec, check_env_cache, write_env_cache, format_env_spec_summary
    from tune.core.analysis.executor import PixiEnv, make_output_dir
    from tune.core.config import get_config

    cfg = get_config()

    async with get_session_factory()() as session:
        job = (await session.execute(
            select(AnalysisJob).where(AnalysisJob.id == job_id)
        )).scalar_one_or_none()
        if not job:
            log.warning("prepare_environment_task: job %s not found", job_id)
            return

        if not job.output_dir:
            project_name = "default"
            if job.project_id:
                from tune.core.models import Project

                project = (await session.execute(
                    select(Project).where(Project.id == job.project_id)
                )).scalar_one_or_none()
                if project:
                    project_name = project.name
            job.output_dir = str(make_output_dir(cfg.analysis_dir, project_name, job.name))

        steps = job.plan or []
        job.env_status = "preparing"
        await session.commit()

    # Compute the env spec from the plan
    env_spec = build_env_spec(steps)
    log.info("prepare_environment_task: job %s %s",
             job_id, format_env_spec_summary(env_spec))

    # Surface conflicts as warnings (non-blocking — pixi resolves them or fails below)
    for conflict in env_spec.conflicts:
        log.warning("prepare_environment_task: %s (job %s)", conflict, job_id)

    # Determine the env dir (shared per-project, same as PixiEnv logic)
    if job.output_dir:
        run_dir = Path(job.output_dir)
    else:
        run_dir = cfg.analysis_dir / "tmp" / job_id
    env_dir = run_dir.parent / ".pixi-env"
    origin_file = env_dir / ".env_origin_path"
    current_origin = str(env_dir.resolve())

    if env_dir.exists():
        stored_origin = origin_file.read_text().strip() if origin_file.exists() else None
        if stored_origin != current_origin:
            log.info(
                "prepare_environment_task: rebuilding stale pixi env for job %s env_dir=%s stored_origin=%s",
                job_id,
                env_dir,
                stored_origin,
            )
            shutil.rmtree(env_dir, ignore_errors=True)

    # Cache check
    if check_env_cache(env_dir, env_spec.hash):
        log.info("prepare_environment_task: env cache hit for job %s (hash=%s)", job_id, env_spec.hash)
        async with get_session_factory()() as session:
            job = (await session.execute(
                select(AnalysisJob).where(AnalysisJob.id == job_id)
            )).scalar_one_or_none()
            if job:
                job.env_status = "ready"
                job.env_spec_hash = env_spec.hash
                await session.commit()
        from tune.workers.defer import defer_async_with_fallback

        await defer_async_with_fallback(run_analysis_task, job_id=job_id)
        return

    # Install packages via pixi — Phase 7: use bulk_install (single pixi add call)
    # instead of N sequential pixi add calls.  Falls back to sequential on failure.
    pixi = PixiEnv(run_dir, cfg.pixi_path, project_id=job.project_id or "")
    pixi.init_toml()

    success = await pixi.bulk_install(env_spec.packages)

    async with get_session_factory()() as session:
        job = (await session.execute(
            select(AnalysisJob).where(AnalysisJob.id == job_id)
        )).scalar_one_or_none()
        if not job:
            return
        if success:
            job.env_status = "ready"
            job.env_spec_hash = env_spec.hash
            job.error_message = None
            write_env_cache(env_dir, env_spec.hash)
            log.info("prepare_environment_task: env ready for job %s", job_id)
        else:
            job.env_status = "failed"
            failed_packages = list(getattr(pixi, "last_failed_packages", []) or [])
            install_error = str(getattr(pixi, "last_install_error", "") or "").strip()
            if not install_error and failed_packages:
                install_error = (
                    "Pixi environment preparation failed for package(s): "
                    + ", ".join(failed_packages)
                )
            elif not install_error:
                install_error = "Pixi environment preparation failed."
            job.error_message = install_error
            log.error(
                "prepare_environment_task: env failed for job %s packages=%s detail=%s",
                job_id,
                failed_packages,
                install_error,
            )
        await session.commit()

    if success:
        from tune.workers.defer import defer_async_with_fallback

        await defer_async_with_fallback(run_analysis_task, job_id=job_id)


async def _summarize_resource_sync_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    total_changes = sum(int(item.get("changes") or 0) for item in results)
    total_resource_entities = sum(int(item.get("resource_entity_count") or 0) for item in results)
    return {
        "project_count": len(results),
        "total_changes": total_changes,
        "total_resource_entities": total_resource_entities,
        "projects": results,
    }


async def _mark_scan_file_processed_and_maybe_trigger_sync() -> None:
    from tune.core.database import get_session_factory
    from tune.core.models import ScanState
    from sqlalchemy import select

    should_trigger = False
    async with get_session_factory()() as session:
        state = (await session.execute(
            select(ScanState).with_for_update()
        )).scalar_one_or_none()
        if not state or state.status not in {"running", "syncing_resources"}:
            return

        total_discovered = int(state.total_discovered or 0)
        total_processed = int(state.total_processed or 0)
        state.total_processed = min(total_processed + 1, total_discovered)

        if total_discovered > 0 and state.total_processed >= total_discovered:
            if state.resource_sync_status not in {"queued", "running", "completed"}:
                state.resource_sync_status = "queued"
                state.status = "syncing_resources"
                should_trigger = True
        await session.commit()

    if should_trigger:
        await post_scan_resource_sync_task.defer_async()


@app.task(queue="scan")
async def post_scan_resource_sync_task() -> None:
    from datetime import datetime, timezone

    from tune.core.database import get_session_factory
    from tune.core.models import ScanState
    from tune.core.resources.entities import sync_all_projects_resource_entities
    from sqlalchemy import select

    async with get_session_factory()() as session:
        state = (await session.execute(
            select(ScanState).with_for_update()
        )).scalar_one_or_none()
        if not state:
            return
        state.status = "syncing_resources"
        state.resource_sync_status = "running"
        await session.commit()

    try:
        async with get_session_factory()() as session:
            results = await sync_all_projects_resource_entities(session)
            summary = await _summarize_resource_sync_results(results)
            state = (await session.execute(
                select(ScanState).with_for_update()
            )).scalar_one_or_none()
            if state:
                state.status = "complete"
                state.resource_sync_status = "completed"
                state.resource_sync_summary_json = summary
                state.completed_at = datetime.now(tz=timezone.utc)
                await session.commit()
    except Exception as exc:
        log.exception("post_scan_resource_sync_task failed")
        async with get_session_factory()() as session:
            state = (await session.execute(
                select(ScanState).with_for_update()
            )).scalar_one_or_none()
            if state:
                state.status = "complete"
                state.resource_sync_status = "failed"
                state.resource_sync_summary_json = {"error": str(exc)}
                state.completed_at = datetime.now(tz=timezone.utc)
                await session.commit()


@app.task(queue="scan", retry=procrastinate.RetryStrategy(max_attempts=3))
async def scan_file_task(path: str) -> None:
    """Extract base metadata for a single file and upsert into the database."""
    from tune.core.database import get_session_factory
    from tune.core.models import File
    from tune.core.scanner.detector import detect_file_type
    from tune.core.scanner.extractor import extract_base_metadata
    from sqlalchemy import select

    file_path = Path(path)
    if not file_path.exists() or not file_path.is_file():
        log.warning("scan_file_task: path does not exist: %s", path)
        await _mark_scan_file_processed_and_maybe_trigger_sync()
        return

    file_type = detect_file_type(file_path)
    meta = extract_base_metadata(file_path, file_type)

    is_new = False
    async with get_session_factory()() as session:
        existing = (
            await session.execute(
                select(File)
                .where(File.path == path)
                .order_by(File.discovered_at.desc(), File.id.desc())
                .limit(1)
            )
        ).scalars().first()
        if existing:
            existing.size_bytes = meta["size_bytes"]
            existing.mtime = meta["mtime"]
            existing.md5 = meta["md5"]
            existing.preview = meta["preview"]
        else:
            is_new = True
            dup = None
            if meta["md5"]:
                dup = (
                    await session.execute(
                        select(File)
                        .where(File.md5 == meta["md5"])
                        .order_by(File.duplicate_of.is_not(None), File.discovered_at.asc(), File.id.asc())
                        .limit(1)
                    )
                ).scalars().first()
            file_rec = File(
                id=str(uuid.uuid4()),
                duplicate_of=dup.id if dup else None,
                **meta,
            )
            session.add(file_rec)
        await session.commit()

    # Broadcast new-file discovery event if this was a new unassigned file
    if is_new:
        try:
            from tune.api.ws import broadcast_global_chat_event
            await broadcast_global_chat_event({
                "type": "new_files_discovered",
                "count": 1,
                "types": {file_type: 1},
            })
        except Exception:
            pass  # Broadcasting is best-effort

    await _mark_scan_file_processed_and_maybe_trigger_sync()




@app.task(queue="analysis")
async def run_analysis_task(job_id: str) -> None:
    """Execute a full analysis job: plan → execute steps → handle errors.

    Phase 1 resume: when the job status is waiting_for_authorization or
    waiting_for_repair, this function is re-invoked by defer_async after the
    user makes a decision.  It reads the cached decision from DB, skips already-
    completed steps, and continues from the suspended step.
    """
    import asyncio
    import os
    import re as _re
    import shlex as _shlex
    from datetime import datetime, timezone
    from pathlib import Path as _Path

    from sqlalchemy import select

    from tune.api.ws import (
        AuthorizationPendingError,
        broadcast_job_event,
        broadcast_thread_chat_event,
        request_authorization,
    )
    from tune.core.analysis.executor import (
        PixiEnv,
        append_command_log,
        check_input_files,
        get_command_type,
        init_commands_log,
        make_output_dir,
        monitor_resources,
        run_subprocess,
        write_inputs_json,
    )
    from tune.core.analysis.planner import generate_fine_command
    from tune.core.config import get_config
    from tune.core.database import get_session_factory
    from tune.core.models import AnalysisJob, Experiment, File, FileRun, Project, Sample
    from tune.core.orchestration import (
        build_execution_bundle,
        extract_execution_nodes,
        extract_plan_steps,
        replace_plan_steps,
    )
    from sqlalchemy.orm import selectinload

    # Pipeline-v2: renderer registry (LEGACY_RENDERER=true falls back to LLM)
    import os as _os
    _use_legacy_renderer = _os.environ.get("LEGACY_RENDERER", "").lower() == "true"
    if not _use_legacy_renderer:
        from tune.core.renderer import render_step, RendererError as _RendererError

    cfg = get_config()

    # Phase 1: resume context — populated when re-invoked after auth/repair pause
    _resume_ctx: dict | None = None

    async with get_session_factory()() as session:
        job = (await session.execute(
            select(AnalysisJob).where(AnalysisJob.id == job_id)
        )).scalar_one_or_none()
        if not job or not (job.plan or job.resolved_plan_json or getattr(job, "expanded_dag_json", None)):
            return

        # --- Phase 1 resume detection ---
        _resume_anchor_ctx = _load_resume_anchor(job)
        _is_resume = (
            job.status in ("waiting_for_authorization", "waiting_for_repair")
            or bool(job.pending_auth_request_id)
            or bool(job.pending_repair_request_id)
            or bool(_resume_anchor_ctx)
        )
        if _is_resume:
            if job.pending_auth_request_id or job.pending_repair_request_id:
                _resume_ctx = await _load_pending_decision(job)
                if _resume_ctx is None:
                    # Decision not yet available — ws handler will re-defer when ready
                    log.warning(
                        "run_analysis_task: job %s resumed but pending decision not available "
                        "(status=%s, auth_req=%s, repair_req=%s)",
                        job_id, job.status,
                        job.pending_auth_request_id, job.pending_repair_request_id,
                    )
                    return
            else:
                _resume_ctx = _resume_anchor_ctx
            # Clear pending fields — they'll be re-set if we pause again
            job.pending_auth_request_id = None
            job.pending_repair_request_id = None
            job.pending_step_key = None
            job.pending_interaction_type = None
            job.pending_interaction_payload_json = None
            log.info(
                "run_analysis_task: resuming job %s after %s (step_key=%s)",
                job_id, _resume_ctx["type"], _resume_ctx.get("step_key", ""),
            )

        # Phase 6: if env not ready, delegate to prepare_environment_task which
        # will call run_analysis_task again once the environment is prepared.
        if job.env_status != "ready":
            log.info(
                "run_analysis_task: env not ready (status=%s) for job %s — "
                "delegating to prepare_environment_task",
                job.env_status, job_id,
            )
            await session.commit()
            from tune.workers.defer import defer_async_with_fallback

            await defer_async_with_fallback(prepare_environment_task, job_id=job_id)
            return

        job_name = job.name  # save before session closes
        abstract_plan_payload = job.resolved_plan_json or job.plan
        abstract_plan = extract_plan_steps(abstract_plan_payload)
        if not abstract_plan:
            abstract_plan = extract_plan_steps(job.plan)
        job_plan = extract_execution_nodes(getattr(job, "expanded_dag_json", None))
        if not job_plan:
            job_plan = [dict(step) for step in abstract_plan]
        job_language = job.language or "en"
        job_thread_id = job.thread_id

        job.status = "running"
        job.last_progress_at = datetime.now(tz=timezone.utc)
        if not job.started_at:  # preserve original start time on resume
            job.started_at = datetime.now(tz=timezone.utc)
        await session.commit()

        # Look up human-readable project name for directory naming
        project_name = "default"
        if job.project_id:
            proj = (await session.execute(
                select(Project).where(Project.id == job.project_id)
            )).scalar_one_or_none()
            if proj:
                project_name = proj.name

        # Fetch project files so commands can reference real paths
        project_files: list[dict] = []
        if job.project_id:
            files = (await session.execute(
                select(File)
                .options(selectinload(File.enhanced_metadata))
                .where(File.project_id == job.project_id)
                .limit(200)
            )).scalars().all()
            file_lineage: dict[str, dict[str, object | None]] = {}
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
            project_files = [
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

        # On resume reuse the existing output directory; on fresh start create one
        if job.output_dir:
            out_dir = _Path(job.output_dir)
            log.debug("run_analysis_task: reusing output_dir %s for job %s", out_dir, job_id)
        else:
            out_dir = make_output_dir(cfg.analysis_dir, project_name, job.name)
            job.output_dir = str(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        await session.commit()

    async def _broadcast_job_chat(event: dict[str, object]) -> bool:
        if job_thread_id:
            await broadcast_thread_chat_event(job_thread_id, event)
            return True
        log.warning(
            "run_analysis_task: dropping job-scoped chat event type=%s for job %s because thread_id is missing",
            event.get("type"),
            job_id,
        )
        return False

    # ------------------------------------------------------------------
    # PHASE 2: Compile gate — ensure plan is compiled before execution.
    # Plans created before Phase 2 (no _compiled marker) are compiled here
    # so the executor can rely on normalised fields and filled-in defaults.
    # ------------------------------------------------------------------
    if abstract_plan and not all(s.get("_compiled") for s in abstract_plan):
        try:
            from tune.core.workflow.plan_compiler import compile_plan as _compile_plan
            _cresult = _compile_plan(abstract_plan)
            if _cresult.ok:
                abstract_plan = _cresult.compiled_steps
                log.info(
                    "run_analysis_task: compiled uncompiled plan for job %s (%d steps, %d warnings)",
                    job_id, len(abstract_plan), len(_cresult.warnings),
                )
            else:
                log.warning(
                    "run_analysis_task: plan compilation failed for job %s: %s — proceeding with uncompiled plan",
                    job_id, _cresult.errors,
                )
        except Exception:
            log.exception("run_analysis_task: compile gate raised for job %s — proceeding", job_id)

    # ------------------------------------------------------------------
    # READINESS CHECK PHASE (new semantic resource check before preflight)
    # Skipped on resume — already resolved in original run.
    # ------------------------------------------------------------------
    if not _is_resume:
        try:
            from tune.core.context.builder import PlannerContextBuilder
            from tune.core.context.models import ContextScope
            from tune.core.resources.graph_builder import ResourceGraphBuilder
            from tune.core.resources.readiness import ReadinessChecker
            from tune.core.resources.planner_adapter import enforce_planner_constraints

            async with get_session_factory()() as _rc_session:
                planner_ctx = await PlannerContextBuilder(_rc_session).build(
                    ContextScope(project_id=job.project_id or "")
                )

            if planner_ctx.resource_graph:
                # Graph was already built in PlannerContextBuilder — reuse it
                rr_graph = planner_ctx.resource_graph
            else:
                rr_graph = None

            if rr_graph is not None:
                rr_report = ReadinessChecker().check(abstract_plan, rr_graph)

                # Persist graph snapshot on the job record
                try:
                    import json as _json
                    _graph_nodes = {
                        nid: {
                            "id": n.id, "kind": n.kind, "status": n.status,
                            "label": n.label, "resolved_path": n.resolved_path,
                            "source_type": n.source_type,
                        }
                        for nid, n in rr_graph.nodes.items()
                    }
                    _graph_json = _json.dumps({"nodes": _graph_nodes, "by_kind": rr_graph.by_kind})
                    async with get_session_factory()() as _gj_sess:
                        _gj_job = (
                            await _gj_sess.execute(
                                select(AnalysisJob).where(AnalysisJob.id == job_id)
                            )
                        ).scalar_one_or_none()
                        if _gj_job:
                            _gj_job.resource_graph_json = _graph_json
                            await _gj_sess.commit()
                except Exception:
                    log.debug("run_analysis_task: failed to persist resource_graph_json for job %s", job_id)

                if not rr_report.ok:
                    blocking = [i for i in rr_report.issues if i.severity == "blocking"]
                    log.warning(
                        "run_analysis_task: ReadinessChecker blocking issues for job %s: %s",
                        job_id,
                        [i.title for i in blocking],
                    )
                    async with get_session_factory()() as _rr_sess:
                        _rr_job = (
                            await _rr_sess.execute(
                                select(AnalysisJob).where(AnalysisJob.id == job_id)
                            )
                        ).scalar_one_or_none()
                        if _rr_job:
                            _rr_job.status = "resource_clarification_required"
                            _rr_job.error_message = "; ".join(i.title for i in blocking)
                            await _rr_sess.commit()
                    await _broadcast_job_chat({
                        "type": "resource_clarification_required",
                        "job_id": job_id,
                        "issues": [
                            {"kind": i.kind, "title": i.title, "description": i.description}
                            for i in blocking
                        ],
                    })
                    try:
                        from tune.api.ws import start_resource_clarification_chat
                        await start_resource_clarification_chat(
                            job_id=job_id,
                            project_id=job.project_id or "",
                            issues=blocking,
                            language=job_language,
                            thread_id=job_thread_id,
                        )
                    except Exception:
                        log.exception(
                            "start_resource_clarification_chat failed for job %s", job_id
                        )
                    return
                else:
                    # Re-apply planner constraints at execution time so historical
                    # jobs also inherit current fact-layer aligner constraints and
                    # injected prepare steps.
                    constrained = enforce_planner_constraints(abstract_plan, planner_ctx)
                    if constrained.warnings:
                        log.info(
                            "run_analysis_task: planner constraint warnings for job %s: %s",
                            job_id,
                            constrained.warnings,
                        )
                    if constrained.amended_plan != abstract_plan:
                        abstract_plan = constrained.amended_plan
                        log.info(
                            "run_analysis_task: planner constraints amended plan for job %s",
                            job_id,
                        )
        except Exception:
            log.exception(
                "run_analysis_task: ReadinessChecker raised for job %s — continuing with preflight",
                job_id,
            )

    # ------------------------------------------------------------------
    # PRE-FLIGHT PHASE (skipped on resume — already done in original run)
    # ------------------------------------------------------------------
    _preflight_bindings: dict[str, dict] = {}
    if _is_resume:
        log.debug("run_analysis_task: skipping pre-flight for resumed job %s", job_id)
    else:
        try:
            from tune.core.registry import ensure_registry_loaded
            from tune.core.renderer import ensure_renderers_loaded
            from tune.core.binding.preflight import run_preflight

            ensure_registry_loaded()
            ensure_renderers_loaded()
            async with get_session_factory()() as pf_session:
                preflight = await run_preflight(
                    plan=abstract_plan,
                    project_id=job.project_id or "",
                    job_id=job_id,
                    output_dir=str(out_dir),
                    db=pf_session,
                )
            if not preflight.ok:
                log.warning(
                    "run_analysis_task: pre-flight failed for job %s: %s",
                    job_id, [issue.title for issue in preflight.issues],
                )
                async with get_session_factory()() as session:
                    job = (await session.execute(
                        select(AnalysisJob).where(AnalysisJob.id == job_id)
                    )).scalar_one_or_none()
                    if job:
                        job.status = "resource_clarification_required"
                        job.error_message = "; ".join(issue.title for issue in preflight.issues)
                        await session.commit()
                await _broadcast_job_chat({
                    "type": "resource_clarification_required",
                    "job_id": job_id,
                    "issues": [
                        {
                            "kind": getattr(issue, "kind", ""),
                            "title": getattr(issue, "title", ""),
                            "description": getattr(issue, "description", ""),
                        }
                        for issue in preflight.issues
                    ],
                    "warnings": [
                        {
                            "kind": getattr(issue, "kind", ""),
                            "title": getattr(issue, "title", ""),
                            "description": getattr(issue, "description", ""),
                        }
                        for issue in preflight.warnings
                    ],
                })
                try:
                    from tune.api.ws import start_resource_clarification_chat
                    await start_resource_clarification_chat(
                        job_id=job_id,
                        project_id=job.project_id or "",
                        issues=preflight.issues,
                        language=job_language,
                        thread_id=job_thread_id,
                    )
                except Exception:
                    log.exception("start_resource_clarification_chat failed for preflight job %s", job_id)
                return
            # Use potentially amended plan (may have hisat2_build injected)
            abstract_plan = preflight.amended_plan
            _preflight_bindings = preflight.resolved_bindings
            if preflight.warnings:
                log.info(
                    "run_analysis_task: pre-flight warnings for job %s: %s",
                    job_id,
                    [issue.title for issue in preflight.warnings],
                )
            # Persist the amended plan so resume can reconstruct the same DAG
            try:
                async with get_session_factory()() as _pf_save:
                    _pf_job = (await _pf_save.execute(
                        select(AnalysisJob).where(AnalysisJob.id == job_id)
                    )).scalar_one_or_none()
                    if _pf_job:
                        _pf_job.resolved_plan_json = replace_plan_steps(
                            _pf_job.resolved_plan_json or _pf_job.plan,
                            abstract_plan,
                        )
                        await _pf_save.commit()
            except Exception:
                log.debug("run_analysis_task: failed to persist resolved_plan_json for job %s", job_id)
        except Exception:
            log.exception("run_analysis_task: pre-flight check raised unexpected error for job %s — proceeding", job_id)
            _preflight_bindings = {}

    if not getattr(job, "expanded_dag_json", None) or not _is_resume:
        try:
            orchestration_bundle = build_execution_bundle(abstract_plan, project_files)
            job_plan = extract_execution_nodes(orchestration_bundle.expanded_dag)
            async with get_session_factory()() as _dag_save:
                _dag_job = (
                    await _dag_save.execute(
                        select(AnalysisJob).where(AnalysisJob.id == job_id)
                    )
                ).scalar_one_or_none()
                if _dag_job:
                    _dag_job.resolved_plan_json = replace_plan_steps(
                        _dag_job.resolved_plan_json or _dag_job.plan,
                        abstract_plan,
                    )
                    _dag_job.execution_ir_json = orchestration_bundle.execution_ir
                    _dag_job.expanded_dag_json = orchestration_bundle.expanded_dag
                    await _dag_save.commit()
        except Exception:
            log.exception(
                "run_analysis_task: failed to materialize expanded DAG for job %s",
                job_id,
            )
            job_plan = [dict(step) for step in abstract_plan]

    write_inputs_json(out_dir, project_files)
    cmd_log = init_commands_log(out_dir)

    pixi = PixiEnv(out_dir, cfg.pixi_path, project_id=job.project_id or "")
    pixi.init_toml()

    # W1: Monitor CPU/memory for the duration of the job.
    monitor_task = asyncio.create_task(monitor_resources(job_id, os.getpid()))

    final_status = "completed"
    error_msg = None

    # Step output directory registry: populated after each successful step
    step_dirs: dict[str, Path] = {}

    try:
        # Sort steps topologically before execution (backward-compatible: unchanged if no depends_on)
        job_plan = _topological_sort(job_plan)
        await _ensure_step_run_records(job_id, job_plan)

        # Phase 1: resume target — skip steps before this key (they already completed)
        _resume_step_key: str | None = (_resume_ctx or {}).get("step_key") or None

        for i, step in enumerate(job_plan):
            # W6: Create a numbered subdirectory for each step's outputs.
            # Support both legacy (name) and typed plan (step_key/display_name) formats.
            _raw_name = (step.get("name") or step.get("step_key") or
                         step.get("display_name") or f"step_{i + 1}")
            step_name = _re.sub(r"[^\w-]", "_", _raw_name)[:30].lower()
            step_dir = out_dir / f"{i + 1:02d}_{step_name}"

            # Phase 1: skip steps that completed before the job was paused
            _step_key = step.get("step_key", "") or step.get("name", "")
            if _resume_step_key:
                if _step_key != _resume_step_key:
                    # Register the dir in step_dirs so BFS scans still work for later steps
                    step_dir.mkdir(parents=True, exist_ok=True)
                    step_dirs[step_name] = step_dir
                    if _step_key:
                        step_dirs[_step_key.lower()] = step_dir
                    log.debug(
                        "run_analysis_task: skipping step '%s' (completed before pause)", _step_key
                    )
                    continue
                # Found the resume target — clear flag, proceed with full execution
                _resume_step_key = None

            step_dir.mkdir(parents=True, exist_ok=True)
            step_run_id = await _ensure_step_run_id(job_id, step)
            await _set_job_current_step(job_id, step_run_id or None)

            # Install required packages into the Pixi environment.
            # Phase 7: when env_status=="ready" the environment was already
            # fully prepared by prepare_environment_task (all packages installed).
            # Skip the per-step ensure_package calls in that case — they are
            # only needed as a safety-net when running without pre-flight.
            _env_is_ready = (job.env_status == "ready")
            step_type = step.get("step_type")
            if not _env_is_ready:
                if step_type and not _use_legacy_renderer:
                    from tune.core.registry import get_step_type as _get_step_type
                    step_defn = _get_step_type(step_type)
                    if step_defn:
                        for pkg in step_defn.pixi_packages:
                            await pixi.ensure_package(pkg)
                elif _use_legacy_renderer:
                    tool_name = step.get("tool", "")
                    if tool_name:
                        from tune.core.analysis.tools import find_tool
                        tool_def = find_tool(tool_name)
                        if tool_def:
                            await pixi.ensure_package(tool_def.pixi_package)
            else:
                step_type = step.get("step_type")

            # Generate the exact shell command for this step (outputs go into step_dir).
            resolved_inputs = await _resolve_step_inputs(step, step_dirs, project_files, job.project_id or "")
            await _resolve_semantic_bindings_for_step(
                job_id=job_id,
                job_plan=job_plan,
                step=step,
                current_step_dir=step_dir,
                step_dirs=step_dirs,
                project_files=project_files,
            )

            # Phase 1: on repair resume, inject the user's command and skip auth
            _repair_resume_active = (
                _resume_ctx is not None
                and _resume_ctx.get("type") == "repair"
                and not _resume_ctx.get("_applied")
            )

            # Pipeline-v2: try renderer first, fall back to LLM if unavailable or LEGACY_RENDERER set
            command = None
            rendered = None           # Phase 5: track the RenderedCommand object throughout
            renderer_outputs: list[str] = []
            step_bindings: dict[str, Any] = {}
            _llm_fallback_used = False  # Phase 5: audit flag
            if not _use_legacy_renderer and step.get("step_type"):
                try:
                    if not step_run_id:
                        log.warning(
                            "run_analysis_task: missing step run id before render "
                            "(job=%s step_key=%s step_type=%s fanout=%s)",
                            job_id,
                            _step_key,
                            step.get("step_type"),
                            bool(step.get("_fanout_expanded")),
                        )

                    # Priority: semantic InputBinding DB records → pre-flight resolved bindings →
                    # injected step bindings → fresh KnownPath overlay
                    step_bindings = await _load_step_bindings(step_run_id)

                    # For preflight-injected steps, use the embedded resolved bindings
                    if not step_bindings and step.get("_preflight_injected"):
                        step_bindings = dict(step.get("_resolved_bindings") or {})

                    # Use pre-flight resolved bindings (covers FileRun Tier 2 reads),
                    # but exclude index_prefix and genome_dir — they will be set freshly
                    # via KnownPath after the build steps auto-register their outputs.
                    if not step_bindings:
                        step_key_local = step.get("step_key", "")
                        step_bindings = dict(_preflight_bindings.get(step_key_local, {}))
                        if not step_bindings and not step.get("_fanout_expanded"):
                            origin_step_key = step.get("_origin_step_key") or step_key_local
                            step_bindings = dict(_preflight_bindings.get(origin_step_key, {}))
                        step_bindings.pop("index_prefix", None)  # re-populated from hisat2_index KnownPath
                        step_bindings.pop("genome_dir", None)    # re-populated from star_genome_dir KnownPath

                    # Fresh KnownPath overlay (fills index_prefix and annotation_gtf)
                    kp_bindings = await _load_known_path_bindings(job.project_id or "")
                    for slot, path in kp_bindings.items():
                        if slot not in step_bindings:
                            step_bindings[slot] = path

                    # Execution-time deterministic fallback: if persisted bindings are
                    # missing, recompute a fact-layer preview directly from the current
                    # plan context before considering any LLM path.
                    preview_bindings = await _preview_semantic_bindings_for_step(
                        job_id=job_id,
                        job_plan=job_plan,
                        step=step,
                        current_step_dir=step_dir,
                        step_dirs=step_dirs,
                        project_files=project_files,
                        project_id=job.project_id or "",
                    )
                    for slot, value in preview_bindings.items():
                        if step.get("_fanout_expanded") or slot not in step_bindings:
                            step_bindings[slot] = value

                    log.info(
                        "run_analysis_task: renderer context job=%s step_key=%s step_type=%s "
                        "run_id=%s fanout=%s lineage=%s bindings=%s preview=%s",
                        job_id,
                        _step_key,
                        step.get("step_type"),
                        step_run_id or "",
                        bool(step.get("_fanout_expanded")),
                        step.get("_preferred_lineage") or {},
                        step_bindings,
                        preview_bindings,
                    )

                    # Scan upstream step output dirs for missing input slot bindings.
                    # Uses BFS over the full transitive dep chain so indirect ancestors
                    # (e.g. sort_samtools two levels above featurecounts) are also searched.
                    _step_type_str = step.get("step_type", "")
                    if _step_type_str:
                        from tune.core.registry import get_step_type as _gst
                        _defn = _gst(_step_type_str)
                        if _defn:
                            # Build a simple step_key → step map from job_plan for BFS
                            _plan_map = {s.get("step_key", "").lower(): s for s in job_plan}
                            for _slot in _defn.input_slots:
                                if _slot.name not in step_bindings:
                                    # from_upstream_dir slots (e.g. multiqc's input_dir)
                                    # should be bound to the first available upstream dep
                                    # output directory, not scanned for individual files.
                                    if getattr(_slot, "from_upstream_dir", False):
                                        for _dep_key_fud in [k.lower() for k in (step.get("depends_on") or [])]:
                                            _dep_dir_fud = step_dirs.get(_dep_key_fud)
                                            if _dep_dir_fud and _dep_dir_fud.exists():
                                                step_bindings[_slot.name] = str(_dep_dir_fud)
                                                break
                                        continue
                                    # Skip other wildcard slots — they must be resolved via
                                    # KnownPath or explicit InputBinding, not BFS file scan.
                                    if _slot.file_types == ["*"]:
                                        continue
                                    # BFS over all transitive dependencies
                                    _visited: set[str] = set()
                                    _bfs_queue: list[str] = [
                                        k.lower() for k in (step.get("depends_on") or [])
                                    ]
                                    _collected_candidates: list[str] = []
                                    while _bfs_queue and _slot.name not in step_bindings:
                                        _dep_key = _bfs_queue.pop(0)
                                        if _dep_key in _visited:
                                            continue
                                        _visited.add(_dep_key)
                                        _dep_dir = step_dirs.get(_dep_key)
                                        if _dep_dir and _dep_dir.exists():
                                            _candidates = sorted([
                                                str(_f) for _f in _dep_dir.rglob("*")
                                                if _f.is_file()
                                                and (
                                                    _slot.file_types == ["*"]
                                                    or _f.suffix.lstrip(".").lower() in _slot.file_types
                                                )
                                            ])
                                            if _candidates:
                                                if getattr(_slot, "multiple", False):
                                                    for _candidate in _candidates:
                                                        if _candidate not in _collected_candidates:
                                                            _collected_candidates.append(_candidate)
                                                else:
                                                    step_bindings[_slot.name] = _candidates[0]
                                        # Enqueue this dep's own deps
                                        _dep_step = _plan_map.get(_dep_key)
                                        if _dep_step:
                                            _bfs_queue.extend(
                                                k.lower() for k in (_dep_step.get("depends_on") or [])
                                            )
                                    if getattr(_slot, "multiple", False) and _collected_candidates:
                                        step_bindings[_slot.name] = _collected_candidates

                    await _persist_step_run_bindings(step_run_id, step_bindings)

                    rendered = render_step(
                        step["step_type"],
                        step.get("params") or {},
                        step_bindings,
                        str(step_dir),
                    )
                    await _set_step_run_status(step_run_id, "ready")
                    command = rendered.command_text
                    renderer_outputs = rendered.expected_outputs
                    log.info("Renderer produced command for step '%s': %s", _step_display_name(step), command[:100])
                except _RendererError as re:
                    if _is_missing_binding_renderer_error(re):
                        await _set_step_run_status(step_run_id, "binding_missing", force=True)
                        log.error(
                            "Renderer missing bindings for step '%s' (step_type=%s): %s. "
                            "Typed execution will NOT fall back to the LLM.",
                            _step_display_name(step),
                            step["step_type"],
                            re,
                        )
                        final_status = "failed"
                        error_msg = (
                            f"Step '{_step_display_name(step)}' is missing deterministic input "
                            f"bindings: {re}"
                        )
                        break
                    if step.get("_fanout_expanded"):
                        await _set_step_run_status(step_run_id, "failed", force=True)
                        log.error(
                            "Renderer failed for fanout step '%s' (step_type=%s): %s. "
                            "LLM fallback is disabled for fanout-expanded typed steps.",
                            _step_display_name(step),
                            step["step_type"],
                            re,
                        )
                        final_status = "failed"
                        error_msg = (
                            f"Step '{_step_display_name(step)}' renderer failed for fanout step "
                            f"'{step['step_type']}': {re}"
                        )
                        break
                    # Phase 5: respect repair_policy.allow_l2_llm before using LLM fallback.
                    # Deterministic steps (allow_l2_llm=False, e.g. util.hisat2_build) fail
                    # immediately; other steps may fall back with an audit warning.
                    from tune.core.registry import get_step_type as _gst_p5
                    from tune.core.registry.steps import RepairPolicy as _RP_p5
                    _p5_defn = _gst_p5(step["step_type"])
                    _p5_policy = _p5_defn.repair_policy if _p5_defn else _RP_p5()
                    if _p5_policy.allow_l2_llm:
                        log.warning(
                            "Renderer failed for step '%s' (step_type=%s): %s — "
                            "LLM fallback permitted by repair_policy (allow_l2_llm=True)",
                            _step_display_name(step), step["step_type"], re,
                        )
                        _llm_fallback_used = True
                    else:
                        await _set_step_run_status(step_run_id, "failed", force=True)
                        log.error(
                            "Renderer failed for step '%s' (step_type=%s): %s — "
                            "LLM fallback DISABLED by repair_policy (allow_l2_llm=False). "
                            "Failing step.",
                            _step_display_name(step), step["step_type"], re,
                        )
                        final_status = "failed"
                        error_msg = (
                            f"Step '{_step_display_name(step)}' renderer failed and LLM "
                            f"fallback is disabled for step_type '{step['step_type']}': {re}"
                        )
                        break
                except Exception as _re_exc:
                    # Phase 5: unexpected renderer exceptions do NOT silently trigger LLM
                    # fallback — they indicate a code bug and should surface immediately.
                    await _set_step_run_status(step_run_id, "failed", force=True)
                    log.exception(
                        "Unexpected renderer error for step '%s' (step_type=%s) — "
                        "NOT falling back to LLM",
                        _step_display_name(step), step["step_type"],
                    )
                    final_status = "failed"
                    error_msg = (
                        f"Unexpected renderer error for step '{_step_display_name(step)}': "
                        f"{_re_exc}"
                    )
                    break

            if command is None:
                # Phase 5: LLM fallback — only reached for:
                #   (a) Old plan format without step_type  — backwards compat
                #   (b) LEGACY_RENDERER=true               — explicit override
                #   (c) RendererError + allow_l2_llm=True  — policy-permitted
                _llm_fallback_used = True
                _fallback_reason = (
                    "renderer_error" if step.get("step_type") and not _use_legacy_renderer
                    else ("legacy_renderer_mode" if _use_legacy_renderer else "no_step_type")
                )
                log.warning(
                    "LLM fallback: generating command for step '%s' "
                    "(step_type=%s, reason=%s, job=%s)",
                    _step_display_name(step), step.get("step_type", "none"),
                    _fallback_reason, job_id,
                )
                command = await generate_fine_command(step, project_files, str(step_dir), {}, project_id=job.project_id or "", resolved_inputs=resolved_inputs, language=job_language)
                from tune.core.renderer import RenderedCommand as _RC_p5
                rendered = _RC_p5(command_text=command, llm_fallback_used=True)
                await _set_step_run_status(step_run_id, "ready")
                await broadcast_job_event(job_id, {
                    "type": "llm_fallback",
                    "step": _step_display_name(step),
                    "step_type": step.get("step_type", ""),
                    "reason": _fallback_reason,
                })

            # Input file pre-check: verify files exist and are non-empty before running.
            # Pass step_dir and renderer_outputs so output paths are excluded from the check.
            input_problems = check_input_files(
                command, project_files,
                output_dir=str(step_dir),
                known_outputs=renderer_outputs,
            )
            if input_problems:
                await _set_step_run_status(step_run_id, "failed", force=True)
                problem_msg = "; ".join(input_problems)
                with open(cmd_log, "a") as _f:
                    _f.write(f"\n## [INPUT_FILE_ERROR] Step: {_step_display_name(step)}\n{problem_msg}\n")
                await broadcast_job_event(job_id, {
                    "type": "input_file_error",
                    "step": _step_display_name(step),
                    "error": problem_msg,
                })
                await _broadcast_job_chat({
                    "type": "input_file_error",
                    "step": _step_display_name(step),
                    "job_id": job_id,
                    "error": problem_msg,
                })
                final_status = "failed"
                error_msg = f"Input file error on step '{_step_display_name(step)}': {problem_msg}"
                break

            # Command authorization sandbox.
            # Phase 1: on auth resume for this exact step, use the cached decision.
            # Otherwise request fresh authorization (raises AuthorizationPendingError to pause).
            _auth_resume_active = (
                _resume_ctx is not None
                and _resume_ctx.get("type") == "auth"
                and not _resume_ctx.get("_applied")
            )
            if _repair_resume_active:
                # Repair resume: skip rendering and auth entirely — use the user's fix command
                if not _resume_ctx["should_continue"]:
                    await broadcast_job_event(job_id, {
                        "type": "error_escalation",
                        "step": _step_display_name(step),
                        "error": "Repair cancelled by user.",
                        "fix_hint": "user_cancelled",
                    })
                    final_status = "failed"
                    error_msg = f"Step '{_step_display_name(step)}' repair was cancelled by user."
                    _resume_ctx["_applied"] = True
                    break
                # Phase 6: capture original command before override so RepairMemory
                # can record the (failed_command → human_fix) pair after success.
                _original_command_before_repair = command
                command = _resume_ctx["repair_command"] or command
                _resume_ctx["_applied"] = True
                authorized = True  # User explicitly provided the command
            elif _auth_resume_active:
                # Auth resume: use the cached decision for this step
                authorized = _resume_ctx["authorized"]
                # Use the command from the auth request (same as renderer output)
                command = _resume_ctx["command"] or command
                _resume_ctx["_applied"] = True
                if authorized:
                    command_type = _resolve_command_type_for_step(step, command)
                    from tune.api.ws import _authorized_types
                    _authorized_types.setdefault(job_id, set()).add(command_type)
            else:
                command_type = _resolve_command_type_for_step(step, command)
                try:
                    await _set_step_run_status(step_run_id, "awaiting_authorization")
                    command, authorized = await request_authorization(
                        job_id, command, command_type,
                        step=step, language=job_language,
                        step_id=step_run_id or step.get("_run_id") or step.get("run_id"),
                        thread_id=job_thread_id,
                    )
                except AuthorizationPendingError:
                    # Job transitioned to waiting_for_authorization by request_authorization().
                    # pending_auth_request_id and pending_step_key already saved to DB.
                    # Return — will be resumed by defer_async when user decides.
                    await _set_step_run_status(step_run_id, "awaiting_authorization", force=True)
                    log.info(
                        "run_analysis_task: job %s paused for authorization at step '%s'",
                        job_id, _step_key,
                    )
                    return
            if not authorized:
                await _set_step_run_status(step_run_id, "failed", force=True)
                await broadcast_job_event(job_id, {
                    "type": "error_escalation",
                    "step": _step_display_name(step),
                    "error": "Command rejected by user.",
                    "fix_hint": "",
                })
                final_status = "failed"
                error_msg = f"Step '{_step_display_name(step)}' was rejected by user."
                break

            # Execute with structured repair engine: L1 rules → L2 constrained LLM → L3 human.
            succeeded = False
            human_assisted = False
            attempt_history: list[dict] = []
            while True:
                await _set_step_run_status(step_run_id, "running", force=True)
                pixi_command = (
                    f"{_shlex.quote(pixi.pixi_path)} run "
                    f"--manifest-path {_shlex.quote(str(pixi.env_dir / 'pixi.toml'))} "
                    f"bash -c {_shlex.quote(command)}"
                )
                result = await run_subprocess(pixi_command, cwd=step_dir, job_id=job_id)
                append_command_log(cmd_log, command, result)
                if result.success:
                    succeeded = True
                    break

                attempt_history.append({
                    "command": command,
                    "stderr": result.stderr,
                    "repair_level": 0,
                })

                # Phase 6: if the previous attempt used a memory-recalled command and it
                # failed again, increment its failure_count so it gets deprioritised.
                if attempt_history and attempt_history[-1].get("from_memory"):
                    _prev_mem_id = attempt_history[-1].get("memory_id")
                    if _prev_mem_id:
                        try:
                            from tune.core.repair.memory import increment_memory_failure
                            await increment_memory_failure(_prev_mem_id)
                        except Exception:
                            pass

                # Repair engine: try Tier-0 memory, L1 rules, L2 constrained LLM, L3 human
                from tune.core.repair import attempt_repair, RepairAction
                step_run_id = step.get("_run_id") or ""
                repair = await attempt_repair(
                    job_id=job_id,
                    step_id=step_run_id or None,
                    command=command,
                    stderr=result.stderr,
                    output_dir=str(step_dir),
                    attempt_history=attempt_history,
                    step_type=step.get("step_type"),
                    project_id=job.project_id or "",
                )

                if repair.action in (RepairAction.APPLIED_RULE, RepairAction.LLM_REPAIRED,
                                     RepairAction.MEMORY_RECALLED):
                    await _set_step_run_status(step_run_id, "repairable_failed", force=True)
                    if repair.action == RepairAction.MEMORY_RECALLED:
                        level = 0
                        attempt_history[-1]["from_memory"] = True
                        attempt_history[-1]["memory_id"] = repair.memory_id
                        log.info("Repair engine Tier-0 memory recalled for step '%s' (memory=%s)",
                                 _step_display_name(step), repair.memory_id)
                    elif repair.action == RepairAction.APPLIED_RULE:
                        level = 1
                        log.info("Repair engine L1 rule '%s' applied for step '%s'",
                                 repair.rule_applied, _step_display_name(step))
                    else:
                        level = 2
                        log.info("Repair engine L2 LLM repair applied for step '%s'",
                                 _step_display_name(step))
                    attempt_history[-1]["repair_level"] = level
                    command = repair.repaired_command
                    # continue the while loop to retry with repaired command

                elif repair.action == RepairAction.ESCALATED:
                    await _set_step_run_status(step_run_id, "waiting_for_human_repair", force=True)
                    # Level-3: RepairRequest already in DB (created by escalate_to_human()).
                    # Phase 1 DB-poll: save pending state to job, activate repair UI, then return.
                    # The job will be re-deferred by write_repair_resolution() when user decides.
                    req_id = repair.escalation_repair_request_id
                    from tune.api.ws import activate_error_recovery as _activate_repair

                    await broadcast_job_event(job_id, {
                        "type": "human_repair_required",
                        "step": _step_display_name(step),
                        "repair_request_id": req_id,
                        "command": command,
                        "stderr": result.stderr[-500:],
                    })

                    # Persist the resume context so the next invocation knows where to start
                    try:
                        from tune.core.database import get_session_factory as _gsf
                        from tune.core.models import AnalysisJob as _AJ
                        from sqlalchemy import select as _sel
                        async with _gsf()() as _sess:
                            _jb = (await _sess.execute(
                                _sel(_AJ).where(_AJ.id == job_id)
                            )).scalar_one_or_none()
                            if _jb:
                                _jb.pending_repair_request_id = req_id
                                _jb.pending_step_key = _step_key
                            await _sess.commit()
                    except Exception:
                        log.exception(
                            "run_analysis_task: failed to save pending_repair_request_id for job %s",
                            job_id,
                        )

                    # Activate session state and broadcast for chat-based resolution
                    await _activate_repair(
                        job_id=job_id,
                        repair_request_id=req_id or "",
                        step_name=_step_display_name(step),
                        command=command,
                        stderr=result.stderr,
                        attempt_history=attempt_history,
                        language=job_language,
                        thread_id=job_thread_id,
                    )

                    log.info(
                        "run_analysis_task: job %s paused for human repair at step '%s' (req=%s)",
                        job_id, _step_key, req_id,
                    )
                    return  # Task exits; will be resumed via defer_async

                else:
                    # No repair possible
                    await _set_step_run_status(step_run_id, "failed", force=True)
                    final_status = "failed"
                    error_msg = f"Step '{_step_display_name(step)}' failed — all repair options exhausted"
                    break

            if not succeeded:
                await _set_step_run_status(step_run_id, "failed", force=True)
                if final_status != "failed":
                    final_status = "failed"
                    error_msg = f"Step '{_step_display_name(step)}' failed — repair exhausted"
                break

            # W4: After a successful step, record its output dir and broadcast result files.
            if succeeded:
                await _set_step_run_status(step_run_id, "succeeded", force=True)
                step_dirs[_step_display_name(step).lower()] = step_dir
                # Also register under step_key so depends_on lookups work for typed plans
                _sk = step.get("step_key", "").lower()
                if _sk:
                    step_dirs[_sk] = step_dir

                # Phase 6: write RepairMemory when a human-provided command succeeds.
                # This persists the fix pattern for future Tier-0 automatic reuse.
                if _repair_resume_active and _resume_ctx and _resume_ctx.get("repair_command"):
                    _repair_stderr = _resume_ctx.get("stderr", "")
                    _repair_cmd = _resume_ctx["repair_command"]
                    _orig_cmd = locals().get("_original_command_before_repair", "") or ""
                    if _repair_stderr and step.get("step_type"):
                        try:
                            from tune.core.repair.memory import write_repair_memory as _wrm
                            _mem_id = await _wrm(
                                step_type=step["step_type"],
                                original_command=_orig_cmd,
                                repair_command=_repair_cmd,
                                stderr=_repair_stderr,
                                project_id=job.project_id or "",
                                context_fingerprint=(
                                    rendered.command_fingerprint
                                    if rendered and hasattr(rendered, "command_fingerprint")
                                    else ""
                                ),
                            )
                            if _mem_id:
                                log.info(
                                    "Phase 6: RepairMemory written (id=%s) for step '%s'",
                                    _mem_id, _step_display_name(step),
                                )
                        except Exception:
                            log.exception(
                                "Phase 6: failed to write RepairMemory for job %s step '%s'",
                                job_id, _step_display_name(step),
                            )

                if step.get("step_type") == "quant.featurecounts":
                    try:
                        sidecar_path = await _write_featurecounts_sample_metadata_sidecar(
                            step_run_id=step.get("_run_id", "") or step.get("run_id", ""),
                            step_bindings=step_bindings,
                            project_files=project_files,
                            step_dir=step_dir,
                        )
                        if sidecar_path:
                            log.debug(
                                "run_analysis_task: wrote featureCounts sample metadata sidecar for step '%s': %s",
                                _sk or _step_display_name(step),
                                sidecar_path,
                            )
                    except Exception:
                        log.exception(
                            "run_analysis_task: failed to write featureCounts sample metadata sidecar for step '%s'",
                            _sk or _step_display_name(step),
                        )

                # Phase 4: Write ArtifactRecord for each expected renderer output that
                # exists on disk.  Downstream steps will query this table (Tier 1a)
                # instead of BFS-scanning directories.
                if renderer_outputs and step.get("step_type") and _sk:
                    try:
                        from tune.core.binding.artifacts import write_artifact_records as _write_arts
                        from tune.core.database import get_session_factory as _sf4
                        async with _sf4()() as _art_sess:
                            _art_meta = {}
                            _artifact_lineage = await _infer_artifact_lineage(
                                step.get("_run_id", ""),
                                project_files,
                                _art_sess,
                            )
                            if rendered is not None and hasattr(rendered, "command_fingerprint"):
                                _art_meta["command_fingerprint"] = rendered.command_fingerprint
                                _art_meta["template_type"] = rendered.template_type
                            if _artifact_lineage:
                                _art_meta["lineage"] = _artifact_lineage
                            _n = await _write_arts(
                                job_id=job_id,
                                step_key=_sk,
                                step_type=step["step_type"],
                                renderer_outputs=renderer_outputs,
                                db=_art_sess,
                                step_run_id=step.get("_run_id"),
                                sample_name=(_artifact_lineage or {}).get("sample_name") if _artifact_lineage else None,
                                metadata=_art_meta or None,
                            )
                            await _art_sess.commit()
                        log.debug(
                            "run_analysis_task: wrote %d artifact record(s) for step '%s'",
                            _n, _sk,
                        )
                    except Exception:
                        log.exception(
                            "run_analysis_task: failed to write artifact records for step '%s'",
                            _sk,
                        )

                # After util.hisat2_build: persist the built index in DerivedResourceCache.
                # KnownPath is no longer the primary index source.
                if step.get("step_type") == "util.hisat2_build" and job.project_id:
                    import glob as _glob
                    ht2_matches = _glob.glob(str(step_dir / "hisat2_index" / "genome") + "*.1.ht2")
                    if ht2_matches:
                        built_prefix = str(step_dir / "hisat2_index" / "genome")
                        # Also write to DerivedResourceCache for staleness tracking
                        try:
                            from tune.core.resources.cache import DerivedResourceCache
                            from tune.core.resources.entities import sync_derived_resource_entity
                            from tune.core.resources.models import ResourceNode
                            from tune.core.binding.resolver import load_registered_resource_bindings
                            from tune.core.database import get_session_factory as _sf2
                            _ref_path: str | None = None
                            _rb = step.get("_resolved_bindings") or {}
                            _ref_path = _rb.get("reference_fasta")
                            if not _ref_path:
                                async with _sf2()() as _ksess:
                                    _kp_map = await load_registered_resource_bindings(job.project_id, _ksess)
                                    _ref_path = _kp_map.get("reference_fasta")
                            idx_node = ResourceNode(
                                id=f"aligner_index:hisat2:{(job.project_id or '')[:8]}",
                                kind="aligner_index",
                                status="ready",
                                label="HISAT2 index",
                                resolved_path=built_prefix,
                                source_type="auto_derived",
                            )
                            async with _sf2()() as _dc_sess:
                                await DerivedResourceCache().put(
                                    project_id=job.project_id,
                                    node=idx_node,
                                    derived_from_path=_ref_path or "",
                                    aligner="hisat2",
                                    db=_dc_sess,
                                )
                                await sync_derived_resource_entity(
                                    _dc_sess,
                                    project_id=job.project_id,
                                    aligner="hisat2",
                                    derived_path=built_prefix,
                                    derived_from_path=_ref_path,
                                )
                                await _dc_sess.commit()
                            log.info("run_analysis_task: DerivedResourceCache updated for hisat2 index")
                        except Exception:
                            log.exception("run_analysis_task: DerivedResourceCache.put failed for hisat2")

                # After util.star_genome_generate: persist the built genome dir in
                # DerivedResourceCache. KnownPath is no longer the primary index source.
                if step.get("step_type") == "util.star_genome_generate" and job.project_id:
                    built_genome_dir = str(step_dir / "star_genome")
                    sa_file = os.path.join(built_genome_dir, "SA")
                    if os.path.exists(sa_file):
                        # Also write to DerivedResourceCache for staleness tracking
                        try:
                            from tune.core.resources.cache import DerivedResourceCache
                            from tune.core.resources.entities import sync_derived_resource_entity
                            from tune.core.resources.models import ResourceNode
                            from tune.core.binding.resolver import load_registered_resource_bindings
                            from tune.core.database import get_session_factory as _sf2
                            _ref_path_star: str | None = None
                            _rb_star = step.get("_resolved_bindings") or {}
                            _ref_path_star = _rb_star.get("reference_fasta")
                            if not _ref_path_star:
                                async with _sf2()() as _ksess:
                                    _kp_map = await load_registered_resource_bindings(job.project_id, _ksess)
                                    _ref_path_star = _kp_map.get("reference_fasta")
                            star_node = ResourceNode(
                                id=f"aligner_index:star:{(job.project_id or '')[:8]}",
                                kind="aligner_index",
                                status="ready",
                                label="STAR genome",
                                resolved_path=built_genome_dir,
                                source_type="auto_derived",
                            )
                            async with _sf2()() as _dc_sess:
                                await DerivedResourceCache().put(
                                    project_id=job.project_id,
                                    node=star_node,
                                    derived_from_path=_ref_path_star or "",
                                    aligner="star",
                                    db=_dc_sess,
                                )
                                await sync_derived_resource_entity(
                                    _dc_sess,
                                    project_id=job.project_id,
                                    aligner="star",
                                    derived_path=built_genome_dir,
                                    derived_from_path=_ref_path_star,
                                )
                                await _dc_sess.commit()
                            log.info("run_analysis_task: DerivedResourceCache updated for star genome")
                        except Exception:
                            log.exception("run_analysis_task: DerivedResourceCache.put failed for star")

                # Write ProjectExecutionEvent if this step required error recovery
                if attempt_history and job.project_id:
                    try:
                        from tune.core.database import get_session_factory as _sf
                        from tune.core.memory.project_memory import write_execution_event
                        async with _sf()() as _sess:
                            await write_execution_event(
                                _sess,
                                project_id=job.project_id,
                                event_type="error_resolved",
                                description=f"Step '{_step_display_name(step)}' failed with: {attempt_history[-1]['stderr'][:200]}",
                                resolution=f"Resolved using command: {command}",
                                user_contributed=human_assisted,
                            )
                    except Exception:
                        pass  # Memory write is best-effort

                # After human-assisted recovery, offer to save to GlobalMemory
                if human_assisted:
                    await _broadcast_job_chat({
                        "type": "suggest_memory_save",
                        "job_id": job_id,
                        "trigger_suggestion": attempt_history[-1]["stderr"][:200] if attempt_history else "",
                        "approach_suggestion": f"Resolved by: {command}",
                    })
                    human_assisted = False  # Reset for next step

                for result_file in sorted(step_dir.rglob("*")):
                    if result_file.is_file() and result_file.suffix.lower() in (".png", ".csv", ".html"):
                        await _broadcast_job_chat({
                            "type": "analysis_result",
                            "kind": result_file.suffix.lstrip(".").lower(),
                            "path": str(result_file),
                            "filename": result_file.name,
                            "step": _step_display_name(step),
                            "job_id": job_id,
                        })

    except Exception as exc:
        log.exception("Unhandled error in run_analysis_task job_id=%s", job_id)
        final_status = "failed"
        error_msg = f"Internal error: {exc}"

    finally:
        # W1: Stop the resource monitor.
        monitor_task.cancel()
        try:
            await monitor_task
        except (asyncio.CancelledError, Exception):
            pass  # monitor task errors must never prevent the job-status update below

        _job_is_paused = False
        async with get_session_factory()() as session:
            job = (await session.execute(
                select(AnalysisJob).where(AnalysisJob.id == job_id)
            )).scalar_one_or_none()
            if job:
                # Do NOT overwrite a pause state that was already committed to DB by
                # request_authorization() or activate_error_recovery().  Those functions
                # transition the job to "waiting_for_authorization" / "waiting_for_repair"
                # and then the worker returns — the finally block must not clobber that.
                #
                # Belt-and-suspenders: also check pending_auth/repair fields as a backup —
                # if transition_job() somehow did not update the status column but the auth
                # request WAS committed, the presence of pending_auth_request_id is a
                # reliable indicator that the job should NOT be marked completed.
                _pause_states = {
                    "waiting_for_authorization",
                    "waiting_for_repair",
                    "resource_clarification_required",
                }
                _job_is_paused = (
                    job.status in _pause_states
                    or bool(job.pending_auth_request_id)
                    or bool(job.pending_repair_request_id)
                )
                log.info(
                    "run_analysis_task FINALLY: job %s status_in_db='%s' final_status='%s' "
                    "pending_auth=%s pending_repair=%s is_paused=%s",
                    job_id, job.status, final_status,
                    job.pending_auth_request_id, job.pending_repair_request_id, _job_is_paused,
                )
                if not _job_is_paused:
                    job_thread_id = job.thread_id or job_thread_id
                    job.status = final_status
                    job.ended_at = datetime.now(tz=timezone.utc)
                    job.last_progress_at = datetime.now(tz=timezone.utc)
                    job.error_message = error_msg
                    if final_status == "completed":
                        job.current_step_id = None
                else:
                    log.info(
                        "run_analysis_task FINALLY: job %s is paused (status='%s', "
                        "pending_auth=%s) — NOT overwriting with final_status='%s'",
                        job_id, job.status, job.pending_auth_request_id, final_status,
                    )
                await session.commit()

        # Only broadcast completion events when the job actually ended (not paused for auth/repair).
        # Broadcasting "analysis_complete" while paused causes the UI to show "Completed" even
        # though the job is waiting for user authorization.
        if not _job_is_paused:
            await broadcast_job_event(job_id, {"type": "status", "status": final_status})

            # Broadcast completion summary to all chat sessions
            await _broadcast_job_chat({
                "type": "analysis_complete",
                "job_id": job_id,
                "job_name": job_name,
                "status": final_status,
                "steps_total": len(job_plan),
                "output_dir": job.output_dir if job else None,
                "error": error_msg,
            })
            try:
                from tune.api.ws import broadcast_project_task_event

                await broadcast_project_task_event(job_id, reason="completed")
            except Exception:
                log.exception("run_analysis_task: failed to broadcast project task event for job %s", job_id)

            # Trigger passive narrative update for the project
            try:
                from tune.core.database import get_session_factory
                from tune.core.models import AnalysisJob
                from sqlalchemy import select as _select
                from tune.core.analysis.engine import _update_project_narrative

                async with get_session_factory()() as _sess:
                    _job = (await _sess.execute(_select(AnalysisJob).where(AnalysisJob.id == job_id))).scalar_one_or_none()
                    if _job and _job.project_id:
                        asyncio.create_task(_update_project_narrative(
                            _job.project_id,
                            {
                                "event": f"Analysis job '{_job.name}' {final_status}",
                                "goal": _job.goal or "",
                                "output_dir": _job.output_dir or "",
                                "error": error_msg or "",
                            },
                        ))
            except Exception:
                pass  # Narrative update is best-effort

            # Offer to extract as a reusable Skill template after a successful run (pipeline-v2).
            if final_status == "completed":
                await _broadcast_job_chat({
                    "type": "offer_skill_extraction",
                    "job_id": job_id,
                    "job_name": job_name,
                })


@app.task(queue="scan")
async def full_scan_task(data_dir: str) -> None:
    """Recursively scan data_dir, queueing scan_file_task for each file."""
    from tune.core.database import get_session_factory
    from tune.core.models import ScanState
    from sqlalchemy import select
    from datetime import datetime, timezone

    root = Path(data_dir)
    all_files = [p for p in root.rglob("*") if p.is_file()]
    total = len(all_files)

    async with get_session_factory()() as session:
        state = (await session.execute(select(ScanState))).scalar_one_or_none()
        if not state:
            state = ScanState(
                total_discovered=total,
                total_processed=0,
                status="running",
                resource_sync_status="pending",
                resource_sync_summary_json=None,
                started_at=datetime.now(tz=timezone.utc),
                completed_at=None,
            )
            session.add(state)
        else:
            state.total_discovered = total
            state.total_processed = 0
            state.last_scanned_path = None
            state.status = "running"
            state.resource_sync_status = "pending"
            state.resource_sync_summary_json = None
            state.started_at = datetime.now(tz=timezone.utc)
            state.completed_at = None
        await session.commit()

    for file_path in all_files:
        await scan_file_task.defer_async(path=str(file_path))

    if total == 0:
        async with get_session_factory()() as session:
            state = (await session.execute(select(ScanState))).scalar_one_or_none()
            if state:
                state.status = "syncing_resources"
                state.resource_sync_status = "queued"
                await session.commit()
        await post_scan_resource_sync_task.defer_async()
