"""Jobs API routes."""
from __future__ import annotations

import json
import logging
import mimetypes
import re
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from tune.core.database import get_session
from tune.core.job_output_paths import build_output_dir_path, derive_run_dirs_from_artifact_paths
from tune.core.models import AnalysisJob, KnownPath, ResourceEntity, ResourceFile
from tune.core.orchestration import (
    extract_plan_steps,
    summarize_execution_confirmation_overview,
    summarize_execution_plan_delta,
    summarize_execution_ir_for_confirmation,
    summarize_execution_review_changes,
    summarize_expanded_dag_for_confirmation,
)
from tune.core.runtime.watchdog import (
    STALL_PROGRESS_THRESHOLD_SECONDS,
    get_job_progress_reference,
    get_job_stall_age_seconds,
    is_job_stalled,
)

router = APIRouter()
log = logging.getLogger(__name__)
_WATCHDOG_AUTO_RECOVERY_RE = re.compile(
    r"^\[watchdog\] Auto-(?:normalized|applied) (?P<issue_kind>[a-z_]+) via (?P<safe_action>[a-z_]+) "
    r"\(status=(?P<resulting_status>[^,]+), pending=(?P<pending_types>[^)]+)\)\.$"
)
_RUN_DIR_NAME_RE = re.compile(r"^\d{8}_\d{6}_.+")
_PIXI_INSTALL_ERROR_RE = re.compile(
    r"Pixi install failed for package\(s\) \[(?P<packages>[^\]]*)\](?:: (?P<detail>.*))?$",
    re.IGNORECASE,
)
_PIXI_MISSING_PACKAGE_RE = re.compile(
    r"PackagesNotFoundError|No package named|Could not find package|No candidates were found for",
    re.IGNORECASE,
)
_RESOURCE_DECISION_TARGETS: dict[str, dict[str, str]] = {
    "reference": {"key": "reference_fasta", "file_role": "reference_fasta"},
    "reference_bundle": {"key": "reference_fasta", "file_role": "reference_fasta"},
    "reference_fasta": {"key": "reference_fasta", "file_role": "reference_fasta"},
    "annotation": {"key": "annotation_gtf", "file_role": "annotation_gtf"},
    "annotation_bundle": {"key": "annotation_gtf", "file_role": "annotation_gtf"},
    "annotation_gtf": {"key": "annotation_gtf", "file_role": "annotation_gtf"},
}


class JobCreate(BaseModel):
    thread_id: str | None = None
    project_id: str | None = None
    name: str
    goal: str
    plan: list | None = None


class SupervisorSafeActionRequest(BaseModel):
    safe_action: str


def _project_output_roots(
    analysis_dir: Path,
    *,
    project_name: str | None,
    project_dir: str | None,
) -> list[Path]:
    roots: list[Path] = []
    for value in (project_name, project_dir):
        if not value:
            continue
        candidate = (analysis_dir / value).resolve()
        if candidate not in roots:
            roots.append(candidate)
    if not roots:
        roots.append((analysis_dir / "default").resolve())
    return roots


def _candidate_job_output_dirs(
    analysis_dir: Path,
    *,
    project_name: str | None,
    project_dir: str | None,
    job_name: str,
    created_at: datetime | None,
    explicit_output_dir: str | None,
    artifact_paths: list[str],
) -> list[Path]:
    candidates: list[Path] = []
    seen: set[Path] = set()

    def _push(path: Path | None) -> None:
        if path is None:
            return
        resolved = path.resolve()
        if resolved in seen:
            return
        seen.add(resolved)
        candidates.append(resolved)

    if explicit_output_dir:
        _push(Path(explicit_output_dir))

    for root in _project_output_roots(
        analysis_dir,
        project_name=project_name,
        project_dir=project_dir,
    ):
        for run_dir in derive_run_dirs_from_artifact_paths(root, artifact_paths):
            if _RUN_DIR_NAME_RE.match(run_dir.name):
                _push(run_dir)

    if created_at:
        for project_root in _project_output_roots(
            analysis_dir,
            project_name=project_name,
            project_dir=project_dir,
        ):
            expected = build_output_dir_path(
                analysis_dir,
                project_root.name,
                job_name,
                created_at=created_at,
            )
            if expected.exists():
                _push(expected)

    return candidates


async def _collect_job_output_cleanup_targets(
    session: AsyncSession,
    job: AnalysisJob,
) -> list[Path]:
    from tune.core.config import get_config
    from tune.core.models import ArtifactRecord, Project

    project_name: str | None = None
    project_dir: str | None = None
    if job.project_id:
        project = (
            await session.execute(select(Project).where(Project.id == job.project_id))
        ).scalar_one_or_none()
        if project:
            project_name = getattr(project, "name", None)
            project_dir = getattr(project, "project_dir", None)

    artifact_paths = list(
        (
            await session.execute(
                select(ArtifactRecord.file_path).where(ArtifactRecord.job_id == job.id)
            )
        ).scalars().all()
    )
    return _candidate_job_output_dirs(
        get_config().analysis_dir.resolve(),
        project_name=project_name,
        project_dir=project_dir,
        job_name=job.name,
        created_at=job.created_at,
        explicit_output_dir=job.output_dir,
        artifact_paths=artifact_paths,
    )


def _delete_job_output_dirs(output_dirs: list[Path], analysis_dir: Path) -> list[str]:
    import shutil

    deleted: list[str] = []
    resolved_analysis_dir = analysis_dir.resolve()
    for output_dir in output_dirs:
        try:
            output_dir.relative_to(resolved_analysis_dir)
        except ValueError:
            log.warning(
                "delete_job_output_dirs: path %s is outside analysis_dir %s, skipping",
                output_dir,
                resolved_analysis_dir,
            )
            continue
        if not output_dir.exists():
            continue
        shutil.rmtree(output_dir)
        deleted.append(str(output_dir))
    return deleted


def _build_environment_failure_diagnostic(job: AnalysisJob) -> dict | None:
    from tune.core.env_planner.planner import candidate_package_specs, build_env_spec, normalize_package_spec

    env_status = str(getattr(job, "env_status", "") or "").strip().lower()
    if env_status != "failed":
        return None

    raw_error = str(getattr(job, "error_message", "") or "").strip()
    failed_packages: list[str] = []
    detail = raw_error
    match = _PIXI_INSTALL_ERROR_RE.match(raw_error)
    if match:
        failed_packages = [
            item.strip()
            for item in str(match.group("packages") or "").split(",")
            if item.strip()
        ]
        parsed_detail = str(match.group("detail") or "").strip()
        if parsed_detail:
            detail = parsed_detail

    combined_text = "\n".join(item for item in [raw_error, detail] if item).strip()
    failure_kind = "missing_package" if _PIXI_MISSING_PACKAGE_RE.search(combined_text) else "install_failed"
    retryable = failure_kind == "missing_package"
    package_candidates: dict[str, list[str]] = {}
    if failure_kind == "missing_package":
        for package in failed_packages:
            candidates = [
                candidate
                for candidate in candidate_package_specs(package)
                if str(candidate).strip()
            ]
            if candidates:
                package_candidates[package] = candidates
    implicated_steps: list[dict[str, Any]] = []
    plan_steps = extract_plan_steps(getattr(job, "resolved_plan_json", None) or getattr(job, "plan", None))
    if plan_steps and failed_packages:
        try:
            env_spec = build_env_spec(plan_steps)
            step_meta = {
                str(step.get("step_key") or ""): {
                    "step_type": str(step.get("step_type") or "").strip() or None,
                    "display_name": str(
                        step.get("display_name")
                        or step.get("name")
                        or step.get("step_key")
                        or step.get("step_type")
                        or ""
                    ).strip() or None,
                }
                for step in plan_steps
                if str(step.get("step_key") or "").strip()
            }
            for step_key, packages in (env_spec.step_package_map or {}).items():
                matched_failed: list[str] = []
                for failed_package in failed_packages:
                    normalized_failed = normalize_package_spec(failed_package)
                    candidate_set = set(package_candidates.get(failed_package) or candidate_package_specs(failed_package))
                    candidate_set.add(normalized_failed)
                    if any(pkg in candidate_set for pkg in (packages or [])):
                        matched_failed.append(failed_package)
                if matched_failed:
                    meta = step_meta.get(step_key, {})
                    implicated_steps.append(
                        {
                            "step_key": step_key,
                            "step_type": meta.get("step_type"),
                            "display_name": meta.get("display_name"),
                            "packages": list(packages or []),
                            "matched_failed_packages": matched_failed,
                        }
                    )
        except Exception:
            log.exception("Failed to infer implicated steps for environment preparation failure")

    diagnostic = {
        "kind": "environment_prepare_failed",
        "env_status": env_status,
        "stage": "pixi_install",
        "failure_kind": failure_kind,
        "retryable": retryable,
        "failed_packages": failed_packages,
        "detail": detail or "Pixi environment preparation failed.",
    }
    if package_candidates:
        diagnostic["package_candidates"] = package_candidates
    if implicated_steps:
        diagnostic["implicated_steps"] = implicated_steps
    if raw_error:
        diagnostic["error_message"] = raw_error
    return diagnostic


async def _get_effective_job_state(session: AsyncSession, job: AnalysisJob) -> dict:
    from tune.core.models import CommandAuthorizationRequest, RepairRequest

    status = job.status
    error_message = job.error_message
    pending_type = job.pending_interaction_type
    pending_payload = job.pending_interaction_payload_json
    runtime_diagnostics: list[dict] = []

    environment_failure = _build_environment_failure_diagnostic(job)
    if environment_failure is not None:
        runtime_diagnostics.append(environment_failure)

    if job.status == "awaiting_plan_confirmation":
        execution_summary = _serialize_execution_plan(job)["summary"]
        if execution_summary["has_execution_ir"] and execution_summary["has_expanded_dag"]:
            pending_type = pending_type or "execution_confirmation"
            pending_payload = pending_payload or {
                "phase": "execution",
                "prompt_text": "Execution graph is ready for final confirmation.",
                "execution_plan_summary": execution_summary,
            }
            if not error_message:
                error_message = pending_payload["prompt_text"]
        else:
            pending_type = pending_type or "plan_confirmation"
            pending_payload = pending_payload or {
                "phase": "abstract",
                "prompt_text": "Abstract analysis plan is waiting for confirmation.",
            }
            if not error_message:
                error_message = pending_payload["prompt_text"]

    if job.pending_auth_request_id:
        auth_req = (
            await session.execute(
                select(CommandAuthorizationRequest).where(
                    CommandAuthorizationRequest.id == job.pending_auth_request_id
                )
            )
        ).scalar_one_or_none()
        if auth_req and auth_req.status == "pending":
            status = "waiting_for_authorization"
            pending_type = "authorization"
            pending_payload = {
                "auth_request_id": auth_req.id,
                "step_key": job.pending_step_key,
                "command_type": auth_req.command_template_type,
                "command": auth_req.current_command_text or auth_req.command_text,
                "prompt_text": (
                    f"Waiting for command authorization before continuing step "
                    f"'{job.pending_step_key or 'unknown'}'."
                ),
                "issues": [
                    {
                        "title": "Pending command authorization",
                        "description": auth_req.command_template_type or "command review required",
                    }
                ],
            }
            if not error_message:
                error_message = pending_payload["prompt_text"]
        else:
            runtime_diagnostics.append(
                {
                    "kind": "orphan_pending_request" if auth_req is None else "resolved_pending_request",
                    "request_type": "authorization",
                    "request_id": job.pending_auth_request_id,
                    "request_status": getattr(auth_req, "status", None),
                    "resolved_at": getattr(auth_req, "resolved_at", None),
                }
            )

    if job.pending_repair_request_id:
        repair_req = (
            await session.execute(
                select(RepairRequest).where(
                    RepairRequest.id == job.pending_repair_request_id
                )
            )
        ).scalar_one_or_none()
        if repair_req and repair_req.status == "pending":
            status = "waiting_for_repair"
            pending_type = "repair"
            pending_payload = {
                "repair_request_id": repair_req.id,
                "step_key": job.pending_step_key,
                "failed_command": repair_req.failed_command,
                "stderr_excerpt": repair_req.stderr_excerpt,
                "prompt_text": (
                    f"Waiting for human repair input before continuing step "
                    f"'{job.pending_step_key or 'unknown'}'."
                ),
            }
            if not error_message:
                error_message = pending_payload["prompt_text"]
        else:
            runtime_diagnostics.append(
                {
                    "kind": "orphan_pending_request" if repair_req is None else "resolved_pending_request",
                    "request_type": "repair",
                    "request_id": job.pending_repair_request_id,
                    "request_status": getattr(repair_req, "status", None),
                    "resolved_at": getattr(repair_req, "resolved_at", None),
                }
            )

    return {
        "status": status,
        "error_message": error_message,
        "pending_interaction_type": pending_type,
        "pending_interaction_payload": pending_payload,
        "runtime_diagnostics": runtime_diagnostics,
    }


def _serialize_binding(binding) -> dict:
    return {
        "id": binding.id,
        "slot_name": binding.slot_name,
        "source_type": binding.source_type,
        "source_ref": binding.source_ref,
        "resolved_path": binding.resolved_path,
        "status": binding.status,
    }


def _serialize_binding_detail(binding, step_run=None) -> dict:
    payload = _serialize_binding(binding)
    payload["match_metadata"] = binding.match_metadata_json
    if step_run is not None:
        payload["step"] = {
            "id": step_run.id,
            "step_key": step_run.step_key,
            "step_type": step_run.step_type,
            "display_name": step_run.display_name,
            "status": step_run.status,
        }
    return payload


def _serialize_execution_plan(job: AnalysisJob) -> dict:
    abstract_plan = getattr(job, "resolved_plan_json", None) or getattr(job, "plan", None)
    execution_ir = getattr(job, "execution_ir_json", None)
    expanded_dag = getattr(job, "expanded_dag_json", None)
    nodes = (
        expanded_dag.get("nodes", [])
        if isinstance(expanded_dag, dict)
        else []
    )
    groups = (
        expanded_dag.get("groups", [])
        if isinstance(expanded_dag, dict)
        else []
    )
    return {
        "abstract_plan": abstract_plan,
        "execution_ir": execution_ir,
        "expanded_dag": expanded_dag,
        "review_overview": summarize_execution_confirmation_overview(
            plan_payload=abstract_plan,
            execution_ir=execution_ir,
            expanded_dag=expanded_dag,
        ),
        "review_ir": summarize_execution_ir_for_confirmation(execution_ir),
        "review_delta": summarize_execution_plan_delta(abstract_plan, expanded_dag),
        "review_changes": summarize_execution_review_changes(expanded_dag),
        "summary": {
            "has_execution_ir": bool(execution_ir),
            "has_expanded_dag": bool(expanded_dag),
            "node_count": len(nodes),
            "group_count": len(groups),
        },
    }


def _serialize_confirmation_details(job: AnalysisJob, pending_type: str | None) -> dict:
    phase = None
    review_plan: list[dict] = []
    execution_summary = None
    execution_overview = None
    execution_ir_review = None
    execution_changes = None
    execution_delta = None

    if pending_type == "execution_confirmation":
        phase = "execution"
        review_plan = summarize_expanded_dag_for_confirmation(getattr(job, "expanded_dag_json", None))
        execution_payload = _serialize_execution_plan(job)
        execution_summary = execution_payload["summary"]
        execution_overview = execution_payload["review_overview"]
        execution_ir_review = execution_payload["review_ir"]
        execution_changes = execution_payload["review_changes"]
        execution_delta = execution_payload["review_delta"]
    elif pending_type == "plan_confirmation":
        phase = "abstract"
        review_plan = extract_plan_steps(
            getattr(job, "resolved_plan_json", None) or getattr(job, "plan", None)
        )

    return {
        "confirmation_phase": phase,
        "confirmation_plan": review_plan,
        "execution_plan_summary": execution_summary,
        "execution_confirmation_overview": execution_overview,
        "execution_ir_review": execution_ir_review,
        "execution_plan_delta": execution_delta,
        "execution_plan_changes": execution_changes,
    }


def _infer_rollback_level(incident_type: str) -> str:
    return {
        "authorization": "step",
        "repair": "step",
        "binding": "step",
        "binding_required": "step",
        "stalled": "step",
        "resume_failed": "step",
        "orphan_pending_request": "step",
        "job_status_mismatch": "step",
        "execution_confirmation": "dag",
        "resource_clarification": "execution_ir",
        "interrupted": "step",
        "failed": "step",
        "plan_confirmation": "abstract_plan",
    }.get(incident_type, "step")


def _infer_failure_layer(incident_type: str) -> str:
    return {
        "plan_confirmation": "abstract_plan",
        "execution_confirmation": "expanded_dag",
        "binding": "resource_binding",
        "resource_clarification": "resource_binding",
        "binding_required": "resource_binding",
        "authorization": "step_execution",
        "repair": "step_execution",
        "stalled": "step_execution",
        "resume_failed": "step_execution",
        "orphan_pending_request": "step_execution",
        "job_status_mismatch": "step_execution",
        "interrupted": "step_execution",
        "failed": "step_execution",
    }.get(incident_type, "step_execution")


def _requires_reconfirmation(rollback_level: str) -> bool:
    return rollback_level in {"abstract_plan", "execution_ir", "dag"}


def _recommendation_fields_from_incident(incident: dict) -> tuple[str, str, str]:
    rollback_target = {
        "execution_confirmation": "execution_confirmation_gate",
        "plan_confirmation": "abstract_plan_gate",
        "authorization": "authorization_request",
        "repair": incident.get("current_step_key") or "failed_step",
        "binding": "binding_resolution",
        "resource_clarification": "resource_clarification_gate",
        "binding_required": "binding_resolution",
        "stalled": incident.get("current_step_key") or "job_runtime",
        "resume_failed": incident.get("current_step_key") or "resume_chain",
        "orphan_pending_request": incident.get("current_step_key") or "pending_request_record",
        "job_status_mismatch": "job_state_normalization",
        "interrupted": incident.get("current_step_key") or "job_queue",
        "failed": incident.get("current_step_key") or "failed_step",
    }.get(incident["incident_type"], "job_detail")
    diagnosis = {
        "execution_confirmation": "Execution is intentionally blocked at the second confirmation gate.",
        "plan_confirmation": "Execution is intentionally blocked at the abstract plan confirmation gate.",
        "authorization": "Worker execution is paused for a user authorization decision.",
        "repair": "The worker hit a command failure and needs an explicit repair choice before it can continue.",
        "binding": "The binding layer could not resolve all required inputs deterministically.",
        "resource_clarification": "The runtime could not safely continue because a required resource is ambiguous or missing.",
        "binding_required": "The binding layer could not resolve all required inputs deterministically.",
        "stalled": "The worker is still marked running, but no runtime progress heartbeat has been observed recently.",
        "resume_failed": "A human decision was already resolved, but the worker resume chain did not complete cleanly.",
        "orphan_pending_request": "The job still points at a pending request record that no longer exists.",
        "job_status_mismatch": "The job reached a terminal state, but stale pending request metadata is still attached.",
        "interrupted": "The job stopped before reaching a terminal state and should be resumed from persisted state.",
        "failed": "The job exited in a failed state and needs diagnosis before retry.",
    }.get(incident["incident_type"], incident["summary"])
    if incident.get("incident_type") == "failed":
        env_failure = _extract_environment_failure_signal(incident)
        if env_failure is not None:
            stage = str(env_failure.get("stage") or "environment preparation").strip()
            failed_packages = [
                str(pkg).strip()
                for pkg in (env_failure.get("failed_packages") or [])
                if str(pkg).strip()
            ]
            package_text = ", ".join(failed_packages)
            failure_kind = str(env_failure.get("failure_kind") or "").strip()
            package_candidates = {
                str(package).strip(): [
                    str(candidate).strip()
                    for candidate in candidates
                    if str(candidate).strip()
                ]
                for package, candidates in (env_failure.get("package_candidates") or {}).items()
                if str(package).strip()
            }
            implicated_steps = [
                item
                for item in (env_failure.get("implicated_steps") or [])
                if isinstance(item, dict)
            ]
            if failure_kind == "missing_package" and package_text:
                candidate_fragments = []
                for package, candidates in package_candidates.items():
                    alternatives = [candidate for candidate in candidates if candidate != package]
                    if alternatives:
                        candidate_fragments.append(f"{package} -> {', '.join(alternatives[:2])}")
                candidate_note = (
                    f" Suggested package candidates: {'; '.join(candidate_fragments)}."
                    if candidate_fragments else ""
                )
                implicated_note = ""
                if implicated_steps:
                    implicated_labels = [
                        str(item.get("display_name") or item.get("step_key") or "").strip()
                        for item in implicated_steps[:3]
                        if str(item.get("display_name") or item.get("step_key") or "").strip()
                    ]
                    if implicated_labels:
                        implicated_note = f" Check these step definitions first: {', '.join(implicated_labels)}."
                diagnosis = (
                    f"Environment preparation failed during {stage} because Pixi could not resolve "
                    f"required package(s) [{package_text}] before execution started. Check step-to-package "
                    f"mapping and dynamic spec pixi_packages.{implicated_note}{candidate_note}"
                )
            elif failure_kind == "missing_package":
                diagnosis = (
                    f"Environment preparation failed during {stage} because Pixi could not resolve "
                    "a required package before execution started. Check step-to-package mapping and dynamic "
                    "spec pixi_packages."
                )
            else:
                diagnosis = (
                    f"Environment preparation failed during {stage} before execution started, "
                    "and the installer error needs manual diagnosis before retry."
                )
    rollback_level = _infer_rollback_level(incident["incident_type"])
    return rollback_target, rollback_level, diagnosis


def _resolve_recommendation_rollback(
    incident: dict,
    dossier: dict | None,
    rollback_target: str,
    rollback_level: str,
    diagnosis: str,
) -> tuple[str, str, str]:
    if not dossier:
        return rollback_target, rollback_level, diagnosis

    rollback_hint = dossier.get("rollback_hint") or {}
    suggested_level = str(rollback_hint.get("suggested_level") or "").strip()
    if not suggested_level:
        return rollback_target, rollback_level, diagnosis

    incident_type = str(incident.get("incident_type") or "").strip()
    current_step_key = (
        str(incident.get("current_step_key") or "").strip()
        or str(((dossier.get("current_step") or {}).get("step_key")) or "").strip()
    )

    if suggested_level == "abstract_plan":
        rollback_target = "abstract_plan_gate"
    elif suggested_level == "execution_ir":
        rollback_target = (
            "resource_clarification_gate"
            if incident_type == "resource_clarification"
            else (current_step_key or "execution_semantics")
        )
    elif suggested_level == "dag":
        rollback_target = (
            "execution_confirmation_gate"
            if incident_type == "execution_confirmation"
            else (current_step_key or "expanded_dag")
        )

    rollback_level = suggested_level

    reason = str(rollback_hint.get("reason") or "").strip()
    if reason and reason.lower() not in diagnosis.lower():
        diagnosis = f"{diagnosis} Rollback hint: {reason}"

    return rollback_target, rollback_level, diagnosis


def _build_supervisor_rollback_context(
    job: AnalysisJob,
    effective: dict,
    incident: dict,
    *,
    current_step=None,
) -> dict:
    confirmation = _serialize_confirmation_details(job, effective.get("pending_interaction_type"))
    dossier = {
        "job_id": getattr(job, "id", None),
        "incident_type": incident.get("incident_type"),
        "current_step": {
            "step_key": getattr(current_step, "step_key", None),
        },
        "execution_confirmation_overview": confirmation.get("execution_confirmation_overview"),
        "execution_plan_delta": confirmation.get("execution_plan_delta"),
    }
    dossier["rollback_hint"] = _build_dossier_rollback_hint(dossier)
    return dossier


def _resolve_supervisor_incident_controls(
    incident: dict,
    *,
    dossier: dict | None = None,
) -> tuple[str, str, str, str | None]:
    rollback_target, rollback_level, diagnosis = _recommendation_fields_from_incident(incident)
    rollback_target, rollback_level, diagnosis = _resolve_recommendation_rollback(
        incident,
        dossier,
        rollback_target,
        rollback_level,
        diagnosis,
    )
    safe_action = _infer_safe_supervisor_action(incident, rollback_level)
    return rollback_target, rollback_level, diagnosis, safe_action


def _extract_environment_failure_signal(incident: dict) -> dict | None:
    for item in (incident.get("runtime_diagnostics") or []):
        if not isinstance(item, dict):
            continue
        if str(item.get("kind") or "").strip() == "environment_prepare_failed":
            return item
    return None


def _build_environment_failure_memory_fragment(incident: dict) -> str | None:
    env_failure = _extract_environment_failure_signal(incident)
    if env_failure is None:
        return None

    parts: list[str] = []
    failure_kind = str(env_failure.get("failure_kind") or "").strip()
    if failure_kind:
        parts.append(f"env_failure={failure_kind}")
    failed_packages = [
        str(pkg).strip()
        for pkg in (env_failure.get("failed_packages") or [])
        if str(pkg).strip()
    ]
    if failed_packages:
        parts.append("env_packages=" + ",".join(failed_packages))
    implicated_steps = [
        str(item.get("step_key") or "").strip()
        for item in (env_failure.get("implicated_steps") or [])
        if isinstance(item, dict) and str(item.get("step_key") or "").strip()
    ]
    if implicated_steps:
        parts.append("env_steps=" + ",".join(implicated_steps[:3]))
    return "; ".join(parts) if parts else None


def _build_safe_action_eligibility(incident: dict) -> dict | None:
    incident_type = str(incident.get("incident_type") or "").strip()
    if incident_type != "resume_failed":
        return None

    retryable_statuses = [
        "interrupted",
        "waiting_for_authorization",
        "waiting_for_repair",
    ]
    job_status = str(incident.get("job_status") or "unknown").strip() or "unknown"
    status_retryable = job_status in set(retryable_statuses)

    runtime_diagnostics = [
        item for item in (incident.get("runtime_diagnostics") or [])
        if isinstance(item, dict)
    ]
    resolved_pending_types = sorted(
        {
            str(item.get("request_type") or "").strip()
            for item in runtime_diagnostics
            if str(item.get("kind") or "").strip() == "resolved_pending_request"
            and str(item.get("request_type") or "").strip()
        }
    )
    if runtime_diagnostics:
        has_resolved_pending_signal = any(
            str(item.get("kind") or "").strip() == "resolved_pending_request"
            for item in runtime_diagnostics
        )
    else:
        # A synthesized resume_failed incident already implies a resolved decision signal.
        has_resolved_pending_signal = True

    has_pending_request_reference = bool(
        incident.get("pending_auth_request_id") or incident.get("pending_repair_request_id")
    )
    pending_reference_types = sorted(
        filter(
            None,
            [
                "authorization" if incident.get("pending_auth_request_id") else "",
                "repair" if incident.get("pending_repair_request_id") else "",
            ],
        )
    )

    blocking_reasons: list[str] = []
    if not status_retryable:
        blocking_reasons.append("job_status_not_retryable")
    if not has_pending_request_reference:
        blocking_reasons.append("pending_request_reference_missing")
    if not has_resolved_pending_signal:
        blocking_reasons.append("resolved_pending_signal_missing")
    if (
        resolved_pending_types
        and pending_reference_types
        and not set(resolved_pending_types).intersection(pending_reference_types)
    ):
        blocking_reasons.append("pending_request_type_mismatch")

    return {
        "eligible": not blocking_reasons,
        "current_job_status": job_status,
        "retryable_job_statuses": retryable_statuses,
        "has_resolved_pending_signal": has_resolved_pending_signal,
        "has_pending_request_reference": has_pending_request_reference,
        "resolved_pending_types": resolved_pending_types,
        "pending_reference_types": pending_reference_types,
        "blocking_reasons": blocking_reasons,
    }


def _describe_resume_retry_blockers(
    eligibility: dict | None,
    *,
    job_status: str,
) -> list[str]:
    blockers: list[str] = []
    if not eligibility:
        return blockers
    if "job_status_not_retryable" in (eligibility.get("blocking_reasons") or []):
        blockers.append(f"the job is in non-retryable status '{job_status}'")
    if "pending_request_reference_missing" in (eligibility.get("blocking_reasons") or []):
        blockers.append("no pending authorization/repair reference is still attached")
    if "resolved_pending_signal_missing" in (eligibility.get("blocking_reasons") or []):
        blockers.append("no resolved pending-decision signal is present")
    if "pending_request_type_mismatch" in (eligibility.get("blocking_reasons") or []):
        blockers.append("the resolved decision type does not match the remaining pending request reference")
    return blockers


def _infer_safe_supervisor_action(incident: dict, rollback_level: str) -> str | None:
    if incident.get("incident_type") == "plan_confirmation" and rollback_level == "abstract_plan":
        return "revalidate_abstract_plan"
    if incident.get("incident_type") == "resource_clarification" and rollback_level == "execution_ir":
        return "refresh_execution_plan"
    if incident.get("incident_type") == "execution_confirmation" and rollback_level == "dag":
        return "refresh_execution_graph"
    if incident.get("incident_type") == "resume_failed":
        eligibility = _build_safe_action_eligibility(incident)
        if rollback_level == "step" and eligibility and eligibility.get("eligible"):
            return "retry_resume_chain"
    if incident.get("incident_type") == "orphan_pending_request":
        return "normalize_orphan_pending_state"
    if incident.get("incident_type") == "job_status_mismatch":
        return "normalize_terminal_state"
    if incident.get("owner") != "system":
        return None
    if rollback_level != "step":
        return None
    if (
        incident.get("incident_type") == "failed"
        and _extract_environment_failure_signal(incident) is not None
    ):
        return None
    if incident.get("incident_type") in {"failed", "binding_required", "interrupted"}:
        return "step_reenter"
    return None


def _infer_auto_recovery_policy(
    incident: dict,
    rollback_level: str,
    safe_action: str | None,
) -> tuple[bool, str | None]:
    if (
        incident.get("owner") == "system"
        and rollback_level == "step"
        and safe_action in {"normalize_orphan_pending_state", "normalize_terminal_state"}
    ):
        return True, "metadata_normalization"
    return False, None


def _build_recovery_playbook(
    incident: dict,
    *,
    rollback_level: str,
    rollback_target: str,
    safe_action: str | None,
    auto_recoverable: bool,
) -> dict:
    step_codes: list[str] = []

    next_action = str(incident.get("next_action") or "").strip()
    if next_action:
        step_codes.append(next_action)

    if safe_action:
        step_codes.append("apply_safe_action")

    if _requires_reconfirmation(rollback_level):
        step_codes.append("open_chat")

    if incident.get("incident_type") in {
        "resume_failed",
        "orphan_pending_request",
        "binding_required",
        "interrupted",
        "failed",
        "stalled",
    }:
        step_codes.append("open_task")

    if safe_action in {
        "step_reenter",
        "retry_resume_chain",
        "normalize_orphan_pending_state",
    } or incident.get("incident_type") == "interrupted":
        step_codes.append("resume_job")

    step_codes.append("recheck_task_state")

    deduped_step_codes: list[str] = []
    for code in step_codes:
        if code and code not in deduped_step_codes:
            deduped_step_codes.append(code)

    return {
        "goal": (
            "stabilize_runtime_metadata"
            if auto_recoverable
            else "restore_execution_progress"
        ),
        "rollback_target": rollback_target,
        "step_codes": deduped_step_codes,
    }


def _build_safe_action_note(
    incident: dict,
    *,
    rollback_level: str,
    safe_action: str | None,
    auto_recoverable: bool,
) -> str | None:
    incident_type = str(incident.get("incident_type") or "").strip()
    job_status = str(incident.get("job_status") or "unknown").strip() or "unknown"
    eligibility = _build_safe_action_eligibility(incident)
    env_failure = _extract_environment_failure_signal(incident)

    if incident_type == "resume_failed" and safe_action == "retry_resume_chain":
        return (
            "Resume-chain retry is available because the resolved decision signal and pending request "
            f"reference are still attached, and the job remains in a retryable paused state ({job_status})."
        )
    if incident_type == "resume_failed" and safe_action is None:
        if eligibility and eligibility.get("eligible") and rollback_level != "step":
            return (
                "Resume-chain retry is intentionally withheld because the safer rollback scope is "
                f"{rollback_level}, so operator review should revisit that layer before resuming."
            )
        blockers = _describe_resume_retry_blockers(eligibility, job_status=job_status)
        blocker_text = "; ".join(blockers) if blockers else (
            f"the job is no longer in a retryable paused state ({job_status})"
        )
        return (
            "Resume-chain retry is withheld because "
            f"{blocker_text}. Inspect the decision chain before resuming or normalizing state."
        )
    if auto_recoverable and safe_action:
        return (
            f"{safe_action} is eligible for low-risk auto recovery because it only normalizes metadata "
            "without changing execution intent."
        )
    if safe_action and not auto_recoverable:
        return (
            f"{safe_action} remains operator-triggered because it can affect execution state at rollback level "
            f"{rollback_level}."
        )
    if incident_type == "failed" and env_failure is not None:
        failure_kind = str(env_failure.get("failure_kind") or "").strip()
        package_candidates = {
            str(package).strip(): [
                str(candidate).strip()
                for candidate in candidates
                if str(candidate).strip()
            ]
            for package, candidates in (env_failure.get("package_candidates") or {}).items()
            if str(package).strip()
        }
        implicated_steps = [
            item
            for item in (env_failure.get("implicated_steps") or [])
            if isinstance(item, dict)
        ]
        if bool(env_failure.get("retryable")) and failure_kind == "missing_package":
            candidate_fragments = []
            for package, candidates in package_candidates.items():
                alternatives = [candidate for candidate in candidates if candidate != package]
                if alternatives:
                    candidate_fragments.append(f"{package} -> {', '.join(alternatives[:3])}")
            if candidate_fragments:
                candidate_note = " Suggested package candidates: " + "; ".join(candidate_fragments) + "."
            else:
                candidate_note = ""
            step_fragments: list[str] = []
            for item in implicated_steps[:3]:
                label = str(item.get("display_name") or item.get("step_key") or "").strip()
                packages = [
                    str(pkg).strip()
                    for pkg in (item.get("packages") or [])
                    if str(pkg).strip()
                ]
                if not label:
                    continue
                if packages:
                    step_fragments.append(f"{label} ({', '.join(packages)})")
                else:
                    step_fragments.append(label)
            step_note = f" Check order: {'; '.join(step_fragments)}." if step_fragments else ""
            return (
                "This environment failure looks retryable after correcting package resolution, step-to-package "
                "mapping, or dynamic spec pixi_packages, but no safe automatic environment repair is enabled yet."
                f"{step_note}{candidate_note}"
            )
        return (
            "This environment failure occurred before execution started and still requires manual environment "
            "diagnosis before retry."
        )
    return None


def _classify_resource_blocker_cause(
    *,
    status: str,
    kind: str,
    candidate_count: int,
    derived_from_count: int,
) -> str:
    if status == "ambiguous" and candidate_count > 1:
        return "ambiguous_candidates"
    if status == "stale" and derived_from_count > 0:
        return "stale_derived_resource"
    if status == "missing" and kind in {"reference_fasta", "annotation_gtf"}:
        return "missing_primary_resource"
    if status == "missing":
        return "missing_runtime_resource"
    return f"{status}_resource"


def _resource_candidate_source_rank(source_type: str | None) -> int:
    return {
        "known_path": 50,
        "resource_entity": 45,
        "artifact_record": 35,
        "filerun_db": 30,
        "project_file": 20,
    }.get(str(source_type or "").strip(), 0)


def _resource_registry_key_for_kind(kind: str | None) -> str | None:
    value = str(kind or "").strip()
    if value in {"reference_fasta", "annotation_gtf", "annotation_bed"}:
        return value
    return None


def _candidate_sort_key(candidate: dict) -> tuple[int, float, str]:
    source_type = str(candidate.get("source_type") or "").strip()
    confidence = float(candidate.get("confidence")) if isinstance(candidate.get("confidence"), (int, float)) else -1.0
    path = str(candidate.get("path") or "").strip()
    return (
        _resource_candidate_source_rank(source_type),
        confidence,
        path,
    )


def _build_resource_candidate_rationale(candidate: dict, top_score: tuple[int, float]) -> str:
    reasons: list[str] = []
    confidence = candidate.get("confidence")
    source_type = str(candidate.get("source_type") or "").strip()
    if isinstance(confidence, (int, float)) and confidence >= 0.8:
        reasons.append("high confidence")
    elif isinstance(confidence, (int, float)) and confidence >= 0.6:
        reasons.append("moderate confidence")
    if source_type:
        reasons.append(f"source={source_type}")
    candidate_score = (
        _resource_candidate_source_rank(source_type),
        float(confidence) if isinstance(confidence, (int, float)) else -1.0,
    )
    if candidate_score == top_score:
        reasons.insert(0, "top-ranked candidate")
    return ", ".join(reasons) or "candidate available"


def _summarize_resource_blocker(
    node_id: str,
    node: dict,
    all_nodes: dict,
    cause: str,
) -> dict:
    label = str(node.get("label") or node.get("resolved_path") or node_id)
    kind = str(node.get("kind") or "unknown")
    status = str(node.get("status") or "unknown")
    candidates = [
        item for item in (node.get("candidates") or [])
        if isinstance(item, dict) and str(item.get("path") or "").strip()
    ]
    candidates.sort(key=_candidate_sort_key, reverse=True)
    top_score = _candidate_sort_key(candidates[0])[:2] if candidates else (-1, -1.0)
    candidate_choices = [
        {
            "path": str(item.get("path") or "").strip(),
            "organism": str(item.get("organism") or "").strip() or None,
            "genome_build": str(item.get("genome_build") or "").strip() or None,
            "source_type": str(item.get("source_type") or "").strip() or None,
            "confidence": float(item.get("confidence")) if isinstance(item.get("confidence"), (int, float)) else None,
            "recommended": index == 0,
            "rationale": _build_resource_candidate_rationale(item, top_score),
        }
        for index, item in enumerate(candidates[:3])
    ]
    preferred_candidate = candidate_choices[0] if candidate_choices and cause == "ambiguous_candidates" else None
    derived_from_preview: list[str] = []
    for source_id in list(node.get("derived_from_ids") or [])[:3]:
        source = all_nodes.get(source_id) if isinstance(all_nodes, dict) else None
        if isinstance(source, dict):
            derived_from_preview.append(
                str(source.get("label") or source.get("resolved_path") or source_id)
            )
        else:
            derived_from_preview.append(str(source_id))

    why_blocked = "Resource readiness is blocked."
    operator_hint = "Inspect the blocker and update the relevant resource binding before resuming."
    recommended_action = "resolve_resource_readiness"
    registry_key = _resource_registry_key_for_kind(kind)
    workspace_section = "recognized"
    if cause == "missing_primary_resource":
        why_blocked = "A required primary reference/annotation resource is missing."
        operator_hint = "Register or select the matching reference FASTA / annotation GTF for this project."
        recommended_action = "register_primary_resource"
        workspace_section = "registry" if registry_key else "recognized"
    elif cause == "ambiguous_candidates":
        why_blocked = "Multiple candidate resources match this requirement and binding cannot choose safely."
        operator_hint = "Inspect the candidate list and confirm the preferred resource instead of allowing implicit binding drift."
        recommended_action = "resolve_ambiguous_resource_candidates"
        workspace_section = "recognized"
    elif cause == "stale_derived_resource":
        why_blocked = "The derived resource exists but is stale relative to its upstream source."
        operator_hint = "Refresh or rebuild the derived resource from its listed upstream dependency before resuming."
        recommended_action = "refresh_stale_derived_resource"
        workspace_section = "recognized"
    elif cause == "missing_runtime_resource":
        why_blocked = "A runtime resource expected by the current step is missing from bindings or filesystem state."
        operator_hint = "Restore the missing runtime resource or rebind the step to an available equivalent."
        recommended_action = "restore_missing_runtime_resource"
        workspace_section = "files"

    return {
        "id": str(node.get("id") or node_id),
        "label": label,
        "kind": kind,
        "status": status,
        "cause": cause,
        "source_type": str(node.get("source_type") or "").strip() or None,
        "organism": str(node.get("organism") or "").strip() or None,
        "genome_build": str(node.get("genome_build") or "").strip() or None,
        "why_blocked": why_blocked,
        "operator_hint": operator_hint,
        "recommended_action": recommended_action,
        "registry_key": registry_key,
        "workspace_section": workspace_section,
        "candidate_choices": candidate_choices,
        "preferred_candidate": preferred_candidate,
        "derived_from_preview": derived_from_preview,
    }


def _resource_blocker_sort_key(item: dict) -> tuple[int, int, str]:
    cause_priority = {
        "missing_primary_resource": 0,
        "ambiguous_candidates": 1,
        "stale_derived_resource": 2,
        "missing_runtime_resource": 3,
    }
    status_priority = {
        "missing": 0,
        "ambiguous": 1,
        "stale": 2,
    }
    cause = str(item.get("cause") or "")
    status = str(item.get("status") or "")
    label = str(item.get("label") or "")
    return (
        cause_priority.get(cause, 9),
        status_priority.get(status, 9),
        label,
    )


def _summarize_resource_graph_snapshot(raw_graph: str | dict | None) -> dict:
    payload: dict | None = None
    if isinstance(raw_graph, str):
        try:
            payload = json.loads(raw_graph)
        except Exception:
            payload = None
    elif isinstance(raw_graph, dict):
        payload = raw_graph

    if not isinstance(payload, dict):
        return {
            "available": False,
            "total_nodes": 0,
            "blocking_total": 0,
            "status_counts": {},
            "blocking_kind_counts": {},
            "blocking_cause_counts": {},
            "blocking_nodes": [],
            "blocking_summary": [],
            "dominant_blocker": None,
        }

    nodes = payload.get("nodes")
    if not isinstance(nodes, dict):
        return {
            "available": False,
            "total_nodes": 0,
            "blocking_total": 0,
            "status_counts": {},
            "blocking_kind_counts": {},
            "blocking_cause_counts": {},
            "blocking_nodes": [],
            "blocking_summary": [],
            "dominant_blocker": None,
        }

    status_counts: Counter[str] = Counter()
    blocking_kind_counts: Counter[str] = Counter()
    blocking_cause_counts: Counter[str] = Counter()
    blocking_nodes: list[dict[str, str]] = []
    blocking_summary: list[dict] = []
    for node_id, node in nodes.items():
        if not isinstance(node, dict):
            continue
        status = str(node.get("status") or "unknown")
        kind = str(node.get("kind") or "unknown")
        candidate_count = len(node.get("candidates") or [])
        derived_from_count = len(node.get("derived_from_ids") or [])
        status_counts[status] += 1
        if status in {"missing", "ambiguous", "stale"}:
            blocking_kind_counts[kind] += 1
            cause = _classify_resource_blocker_cause(
                status=status,
                kind=kind,
                candidate_count=candidate_count,
                derived_from_count=derived_from_count,
            )
            blocking_cause_counts[cause] += 1
            blocking_summary.append(_summarize_resource_blocker(node_id, node, nodes, cause))
        if status in {"missing", "ambiguous", "stale"} and len(blocking_nodes) < 5:
            candidate_preview = [
                str(item.get("path") or "").strip()
                for item in (node.get("candidates") or [])[:3]
                if isinstance(item, dict) and str(item.get("path") or "").strip()
            ]
            blocking_nodes.append(
                {
                    "id": str(node.get("id") or node_id),
                    "kind": kind,
                    "label": str(node.get("label") or node.get("resolved_path") or node_id),
                    "status": status,
                    "cause": _classify_resource_blocker_cause(
                        status=status,
                        kind=kind,
                        candidate_count=candidate_count,
                        derived_from_count=derived_from_count,
                    ),
                    "source_type": str(node.get("source_type") or ""),
                    "organism": str(node.get("organism") or ""),
                    "genome_build": str(node.get("genome_build") or ""),
                    "candidate_count": candidate_count,
                    "candidate_preview": candidate_preview,
                    "derived_from_count": derived_from_count,
                }
            )

    blocking_summary.sort(key=_resource_blocker_sort_key)
    trimmed_blocking_summary = blocking_summary[:5]

    return {
        "available": True,
        "total_nodes": sum(status_counts.values()),
        "blocking_total": sum(blocking_kind_counts.values()),
        "status_counts": dict(status_counts),
        "blocking_kind_counts": dict(blocking_kind_counts),
        "blocking_cause_counts": dict(blocking_cause_counts),
        "blocking_nodes": blocking_nodes,
        "blocking_summary": trimmed_blocking_summary,
        "dominant_blocker": trimmed_blocking_summary[0] if trimmed_blocking_summary else None,
    }


def _resolve_resource_decision_target(entity: Any) -> dict[str, str] | None:
    resource_role = str(getattr(entity, "resource_role", "") or "").strip()
    direct = _RESOURCE_DECISION_TARGETS.get(resource_role)
    if direct:
        return direct
    resource_files = getattr(entity, "resource_files", None) or []
    file_roles = {
        str(getattr(item, "file_role", "") or "").strip()
        for item in resource_files
    }
    if "reference_fasta" in file_roles:
        return _RESOURCE_DECISION_TARGETS["reference_fasta"]
    if "annotation_gtf" in file_roles:
        return _RESOURCE_DECISION_TARGETS["annotation_gtf"]
    return None


def _resolve_resource_file_path(resource_file: Any) -> str | None:
    path = str(getattr(getattr(resource_file, "file", None), "path", "") or "").strip()
    return path or None


def _resolve_recognized_primary_path(entity: Any, file_role: str) -> str | None:
    matching = [
        item
        for item in (getattr(entity, "resource_files", None) or [])
        if str(getattr(item, "file_role", "") or "").strip() == file_role and _resolve_resource_file_path(item)
    ]
    primary = next((item for item in matching if bool(getattr(item, "is_primary", False))), None)
    return _resolve_resource_file_path(primary or (matching[0] if matching else None))


def _summarize_resource_decision_snapshot(
    entities: list[Any] | None,
    known_paths: list[Any] | None,
) -> dict[str, Any]:
    if not entities:
        return {
            "available": True,
            "tracked_total": 0,
            "mismatch_total": 0,
            "stale_decision_total": 0,
            "keep_registered_total": 0,
            "unregistered_total": 0,
            "entries": [],
        }

    known_path_by_key = {
        str(getattr(item, "key", "") or "").strip(): item
        for item in (known_paths or [])
        if str(getattr(item, "key", "") or "").strip()
    }

    entries: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for entity in entities:
        target = _resolve_resource_decision_target(entity)
        if not target:
            continue
        recognized_path = _resolve_recognized_primary_path(entity, target["file_role"])
        known_path = known_path_by_key.get(target["key"])
        registered_path = (
            str(getattr(known_path, "path", "") or "").strip()
            if known_path is not None
            else ""
        ) or None
        metadata = getattr(entity, "metadata_json", None) or {}
        known_path_decisions = metadata.get("known_path_decisions") or {}
        decision = known_path_decisions.get(target["key"]) or {}
        decision_name = str(decision.get("decision") or "").strip() or None
        decision_stale = bool(
            decision_name == "keep_registered"
            and (
                decision.get("recognized_path") != recognized_path
                or decision.get("registered_path") != registered_path
            )
        )
        keep_registered_active = bool(
            decision_name == "keep_registered"
            and recognized_path
            and registered_path
            and decision.get("recognized_path") == recognized_path
            and decision.get("registered_path") == registered_path
        )

        status = "info_only"
        if recognized_path:
            if not registered_path:
                status = "unregistered"
            elif registered_path == recognized_path:
                status = "registered"
            elif keep_registered_active:
                status = "keep_registered"
            else:
                status = "mismatch"

        counts["tracked_total"] += 1
        if status == "mismatch":
            counts["mismatch_total"] += 1
        if status == "unregistered":
            counts["unregistered_total"] += 1
        if keep_registered_active:
            counts["keep_registered_total"] += 1
        if decision_stale:
            counts["stale_decision_total"] += 1

        if status in {"mismatch", "unregistered", "keep_registered"} or decision_stale:
            entries.append(
                {
                    "entity_id": str(getattr(entity, "id", "") or ""),
                    "display_name": str(getattr(entity, "display_name", "") or ""),
                    "resource_role": str(getattr(entity, "resource_role", "") or ""),
                    "known_path_key": target["key"],
                    "recognized_path": recognized_path,
                    "registered_path": registered_path,
                    "status": status,
                    "decision": decision_name,
                    "decision_stale": decision_stale,
                    "updated_at": decision.get("updated_at"),
                }
            )

    entries.sort(
        key=lambda item: (
            0 if item.get("decision_stale") else 1,
            {"mismatch": 0, "unregistered": 1, "keep_registered": 2}.get(str(item.get("status") or ""), 3),
            str(item.get("display_name") or ""),
        )
    )

    return {
        "available": True,
        "tracked_total": counts["tracked_total"],
        "mismatch_total": counts["mismatch_total"],
        "stale_decision_total": counts["stale_decision_total"],
        "keep_registered_total": counts["keep_registered_total"],
        "unregistered_total": counts["unregistered_total"],
        "entries": entries[:5],
    }


async def _fetch_project_resource_decision_snapshot(
    session: AsyncSession,
    project_id: str | None,
) -> dict[str, Any]:
    unavailable = {
        "available": False,
        "tracked_total": 0,
        "mismatch_total": 0,
        "stale_decision_total": 0,
        "keep_registered_total": 0,
        "unregistered_total": 0,
        "entries": [],
    }
    if not hasattr(session, "execute"):
        return unavailable
    project_key = str(project_id or "").strip()
    if not project_key:
        return unavailable

    try:
        entities = (
            await session.execute(
                select(ResourceEntity)
                .where(ResourceEntity.project_id == project_key)
                .options(selectinload(ResourceEntity.resource_files).selectinload(ResourceFile.file))
            )
        ).scalars().all()
        known_paths = (
            await session.execute(select(KnownPath).where(KnownPath.project_id == project_key))
        ).scalars().all()
    except Exception:
        log.debug(
            "jobs: failed to load project resource decision snapshot for project_id=%s",
            project_key,
            exc_info=True,
        )
        return unavailable
    return _summarize_resource_decision_snapshot(entities, known_paths)


def _build_incident_memory_query(job: AnalysisJob, incident: dict, current_step=None) -> str:
    confirmation = _serialize_confirmation_details(
        job,
        incident.get("pending_interaction_type"),
    )
    rollback_context = {
        "job_id": getattr(job, "id", None),
        "incident_type": incident.get("incident_type"),
        "current_step": {
            "step_key": (
                str(incident.get("current_step_key") or "").strip()
                or str(getattr(current_step, "step_key", None) or "").strip()
            ),
        },
        "execution_confirmation_overview": confirmation.get("execution_confirmation_overview"),
        "execution_plan_delta": confirmation.get("execution_plan_delta"),
    }
    rollback_context["rollback_hint"] = _build_dossier_rollback_hint(rollback_context)
    rollback_target, rollback_level, _diagnosis, safe_action = _resolve_supervisor_incident_controls(
        incident,
        dossier=rollback_context,
    )
    env_failure = _extract_environment_failure_signal(incident)
    parts = [
        str(incident.get("incident_type") or "").strip(),
        str(incident.get("summary") or "").strip(),
        str(incident.get("detail") or "").strip(),
        str(incident.get("job_status") or getattr(job, "status", "") or "").strip(),
        str(
            incident.get("current_step_name")
            or incident.get("current_step_key")
            or getattr(current_step, "display_name", None)
            or getattr(current_step, "step_key", None)
            or getattr(job, "pending_step_key", None)
            or ""
        ).strip(),
        f"rollback_level={rollback_level}",
        f"rollback_target={rollback_target}",
    ]
    if safe_action:
        parts.append(f"safe_action={safe_action}")
    rollback_hint = rollback_context.get("rollback_hint") or {}
    if str(rollback_hint.get("suggested_level") or "").strip():
        parts.append(f"rollback_hint={str(rollback_hint.get('suggested_level')).strip()}")
    if env_failure is not None:
        parts.append(str(env_failure.get("failure_kind") or "").strip())
        parts.extend(
            str(pkg).strip()
            for pkg in (env_failure.get("failed_packages") or [])
            if str(pkg).strip()
        )
        parts.extend(
            str(item.get("step_key") or "").strip()
            for item in (env_failure.get("implicated_steps") or [])
            if isinstance(item, dict) and str(item.get("step_key") or "").strip()
        )
    return " ".join(item for item in parts if item)


def _extract_safe_action_from_resolution(resolution: str | None) -> str | None:
    text = str(resolution or "").strip()
    if not text:
        return None
    match = re.search(r"Applied safe action '([a-z_]+)'", text)
    if match:
        return match.group(1)
    match = re.search(r"\bsafe_action=([a-z_]+)\b", text)
    if match:
        return match.group(1)
    return None


def _extract_rollback_level_from_resolution(resolution: str | None) -> str | None:
    text = str(resolution or "").strip()
    if not text:
        return None
    match = re.search(r"\brollback_level=([a-z_]+)\b", text)
    if match:
        return match.group(1)
    return None


def _extract_rollback_target_from_resolution(resolution: str | None) -> str | None:
    text = str(resolution or "").strip()
    if not text:
        return None
    match = re.search(r"\brollback_target=([A-Za-z0-9_:-]+)\b", text)
    if match:
        return match.group(1)
    return None


def _serialize_project_execution_event(event) -> dict:
    resolution = str(getattr(event, "resolution", None) or "").strip()
    return {
        "event_type": str(getattr(event, "event_type", None) or "").strip(),
        "description": str(getattr(event, "description", None) or "").strip(),
        "resolution": resolution,
        "safe_action": _extract_safe_action_from_resolution(resolution),
        "rollback_level": _extract_rollback_level_from_resolution(resolution),
        "rollback_target": _extract_rollback_target_from_resolution(resolution),
        "user_contributed": bool(getattr(event, "user_contributed", False)),
        "created_at": getattr(event, "created_at", None).isoformat()
        if getattr(event, "created_at", None)
        else None,
    }


async def _fetch_similar_project_execution_events(
    session: AsyncSession,
    job: AnalysisJob,
    incident: dict,
    *,
    current_step=None,
    limit: int = 3,
) -> list[dict]:
    project_id = getattr(job, "project_id", None)
    if not project_id:
        return []

    try:
        from tune.core.memory.project_memory import query_project_events

        query_text = _build_incident_memory_query(job, incident, current_step=current_step)
        rows = await query_project_events(session, project_id, query_text, top_k=limit)
        return [_serialize_project_execution_event(item) for item in rows or []]
    except Exception:
        return []


def _build_historical_guidance(
    similar_resolutions: list[dict] | None,
    *,
    safe_action: str | None,
    safe_action_eligibility: dict | None = None,
    incident_type: str | None = None,
    job_status: str | None = None,
    rollback_level: str | None = None,
    rollback_target: str | None = None,
) -> str | None:
    historical_policy = _build_historical_policy(
        similar_resolutions,
        safe_action=safe_action,
        rollback_level=rollback_level,
        rollback_target=rollback_target,
    )
    if not historical_policy:
        return None

    preferred_safe_action = historical_policy.get("preferred_safe_action")
    support_count = int(historical_policy.get("support_count") or 0)
    total_matches = int(historical_policy.get("total_matches") or 0)
    current_safe_action = historical_policy.get("current_safe_action")
    current_supported_count = int(historical_policy.get("current_supported_count") or 0)
    aligns_with_current = historical_policy.get("aligns_with_current")
    preferred_rollback_level = historical_policy.get("preferred_rollback_level")
    rollback_level_aligns_with_current = historical_policy.get("rollback_level_aligns_with_current")

    if current_safe_action and current_supported_count > 0:
        if rollback_level_aligns_with_current is False and preferred_rollback_level:
            return (
                f"Project memory shows {current_supported_count} similar resolution(s) "
                f"using {current_safe_action}, but it most often paired that action with "
                f"rollback level {preferred_rollback_level}."
            )
        return (
            f"Project memory shows {current_supported_count} similar resolution(s) "
            f"using {current_safe_action}."
        )

    if preferred_safe_action:
        if aligns_with_current is False and current_safe_action:
            return (
                f"Project memory most often resolved similar incidents via {preferred_safe_action} "
                f"({support_count}/{total_matches}); current recommendation is {current_safe_action}."
            )
        if (
            not current_safe_action
            and str(incident_type or "").strip() == "resume_failed"
        ):
            if safe_action_eligibility and safe_action_eligibility.get("blocking_reasons"):
                blocker_text = "; ".join(
                    _describe_resume_retry_blockers(
                        safe_action_eligibility,
                        job_status=str(job_status or "unknown").strip() or "unknown",
                    )
                ) or "the current retry eligibility checks are blocked"
                return (
                    f"Project memory most often resolved similar incidents via {preferred_safe_action} "
                    f"({support_count}/{total_matches}), but the current recommendation intentionally withholds "
                    f"that path because {blocker_text}."
                )
            if str(rollback_level or "").strip() and str(rollback_level or "").strip() != "step":
                return (
                    f"Project memory most often resolved similar incidents via {preferred_safe_action} "
                    f"({support_count}/{total_matches}), but the current recommendation intentionally withholds "
                    f"that path because the safer rollback scope is {rollback_level}."
                )
        return (
            f"Project memory most often resolved similar incidents via {preferred_safe_action} "
            f"({support_count}/{total_matches})."
        )

    return f"Project memory contains {total_matches} similar resolved incident record(s)."


def _build_historical_policy(
    similar_resolutions: list[dict] | None,
    *,
    safe_action: str | None,
    rollback_level: str | None = None,
    rollback_target: str | None = None,
) -> dict | None:
    items = [item for item in (similar_resolutions or []) if isinstance(item, dict)]
    if not items:
        return None

    safe_action_counts: Counter[str] = Counter(
        str(item.get("safe_action") or "").strip()
        for item in items
        if item.get("safe_action")
    )
    rollback_level_counts: Counter[str] = Counter(
        str(item.get("rollback_level") or "").strip()
        for item in items
        if item.get("rollback_level")
    )
    rollback_target_counts: Counter[str] = Counter(
        str(item.get("rollback_target") or "").strip()
        for item in items
        if item.get("rollback_target")
    )
    current_rollback_level = str(rollback_level or "").strip() or None
    current_rollback_target = str(rollback_target or "").strip() or None
    preferred_rollback_level = rollback_level_counts.most_common(1)[0][0] if rollback_level_counts else None
    preferred_rollback_target = rollback_target_counts.most_common(1)[0][0] if rollback_target_counts else None
    rollback_level_supported_count = rollback_level_counts.get(current_rollback_level or "", 0)
    rollback_target_supported_count = rollback_target_counts.get(current_rollback_target or "", 0)

    if not safe_action_counts:
        return {
            "preferred_safe_action": None,
            "support_count": 0,
            "total_matches": len(items),
            "confidence": "low",
            "current_safe_action": safe_action,
            "current_supported_count": 0,
            "aligns_with_current": None,
            "preferred_rollback_level": preferred_rollback_level,
            "current_rollback_level": current_rollback_level,
            "rollback_level_supported_count": rollback_level_supported_count,
            "rollback_level_aligns_with_current": (
                preferred_rollback_level == current_rollback_level
                if preferred_rollback_level and current_rollback_level
                else None
            ),
            "preferred_rollback_target": preferred_rollback_target,
            "current_rollback_target": current_rollback_target,
            "rollback_target_supported_count": rollback_target_supported_count,
            "rollback_target_aligns_with_current": (
                preferred_rollback_target == current_rollback_target
                if preferred_rollback_target and current_rollback_target
                else None
            ),
        }

    preferred_safe_action, support_count = safe_action_counts.most_common(1)[0]
    total_matches = len(items)
    support_ratio = support_count / max(total_matches, 1)
    if support_ratio >= 0.75:
        confidence = "high"
    elif support_ratio >= 0.4:
        confidence = "medium"
    else:
        confidence = "low"

    current_safe_action = str(safe_action or "").strip() or None
    current_supported_count = safe_action_counts.get(current_safe_action or "", 0)

    return {
        "preferred_safe_action": preferred_safe_action,
        "support_count": support_count,
        "total_matches": total_matches,
        "confidence": confidence,
        "current_safe_action": current_safe_action,
        "current_supported_count": current_supported_count,
        "aligns_with_current": (
            preferred_safe_action == current_safe_action
            if current_safe_action
            else None
        ),
        "preferred_rollback_level": preferred_rollback_level,
        "current_rollback_level": current_rollback_level,
        "rollback_level_supported_count": rollback_level_supported_count,
        "rollback_level_aligns_with_current": (
            preferred_rollback_level == current_rollback_level
            if preferred_rollback_level and current_rollback_level
            else None
        ),
        "preferred_rollback_target": preferred_rollback_target,
        "current_rollback_target": current_rollback_target,
        "rollback_target_supported_count": rollback_target_supported_count,
        "rollback_target_aligns_with_current": (
            preferred_rollback_target == current_rollback_target
            if preferred_rollback_target and current_rollback_target
            else None
        ),
    }


def _build_recommended_action_confidence(
    *,
    safe_action: str | None,
    auto_recoverable: bool,
    safe_action_eligibility: dict | None,
    historical_policy: dict | None,
) -> tuple[str, list[str]]:
    basis: list[str] = []
    if not safe_action:
        basis.append("no_safe_action")

    if auto_recoverable:
        basis.append("auto_recoverable")

    if safe_action_eligibility is not None:
        if safe_action_eligibility.get("eligible"):
            basis.append("eligibility_passed")
        elif safe_action_eligibility.get("blocking_reasons"):
            basis.append("eligibility_blocked")

    if historical_policy:
        if historical_policy.get("aligns_with_current") is True:
            basis.append("historical_alignment")
        elif historical_policy.get("aligns_with_current") is False:
            basis.append("historical_divergence")
        if historical_policy.get("rollback_level_aligns_with_current") is True:
            basis.append("historical_rollback_alignment")
        elif historical_policy.get("rollback_level_aligns_with_current") is False:
            basis.append("historical_rollback_divergence")
        if historical_policy.get("rollback_target_aligns_with_current") is True:
            basis.append("historical_target_alignment")
        elif historical_policy.get("rollback_target_aligns_with_current") is False:
            basis.append("historical_target_divergence")
        confidence = str(historical_policy.get("confidence") or "").strip()
        if confidence:
            basis.append(f"historical_confidence_{confidence}")

    if not safe_action:
        if (
            "historical_rollback_alignment" in basis
            and (
                "historical_confidence_high" in basis
                or "historical_confidence_medium" in basis
            )
        ):
            return "medium", basis
        return "low", ["no_safe_action", *basis]

    if auto_recoverable:
        return "high", basis
    if "eligibility_blocked" in basis:
        return "low", basis
    if "historical_divergence" in basis or "historical_rollback_divergence" in basis:
        return "low", basis
    if (
        "historical_alignment" in basis
        and "historical_confidence_high" in basis
        and (
            "historical_rollback_alignment" in basis
            or historical_policy is None
            or historical_policy.get("rollback_level_aligns_with_current") is None
        )
    ):
        return "high", basis
    if (
        "eligibility_passed" in basis
        and (
            "historical_alignment" in basis
            or "historical_rollback_alignment" in basis
        )
    ):
        return "medium", basis
    if "historical_alignment" in basis or "historical_rollback_alignment" in basis:
        return "medium", basis
    return "low", basis


def _recommendation_sort_key(rec: dict, incident_age_seconds: int) -> tuple:
    severity_rank = {"critical": 0, "warning": 1, "info": 2}.get(
        str(rec.get("severity") or "").strip(),
        9,
    )
    confidence_rank = {"high": 0, "medium": 1, "low": 2}.get(
        str(rec.get("recommended_action_confidence") or "").strip(),
        3,
    )
    has_safe_action_rank = 0 if rec.get("safe_action") else 1
    auto_recoverable_rank = 0 if rec.get("auto_recoverable") else 1
    eligibility = rec.get("safe_action_eligibility") or {}
    eligibility_rank = 0
    if eligibility:
        eligibility_rank = 0 if eligibility.get("eligible") else 1
    historical_policy = rec.get("historical_policy") or {}
    historical_alignment_rank = 1
    if historical_policy:
        aligns = historical_policy.get("aligns_with_current")
        if aligns is True:
            historical_alignment_rank = 0
        elif aligns is False:
            historical_alignment_rank = 2
    rollback_alignment_rank = 1
    if historical_policy:
        rollback_aligns = historical_policy.get("rollback_level_aligns_with_current")
        if rollback_aligns is True:
            rollback_alignment_rank = 0
        elif rollback_aligns is False:
            rollback_alignment_rank = 2
    original_priority = int(rec.get("priority") or 9999)
    return (
        severity_rank,
        confidence_rank,
        auto_recoverable_rank,
        has_safe_action_rank,
        eligibility_rank,
        historical_alignment_rank,
        rollback_alignment_rank,
        -max(0, int(incident_age_seconds or 0)),
        original_priority,
        str(rec.get("job_id") or ""),
    )


def _finalize_supervisor_recommendations(
    recommendations: list[dict],
    incidents: list[dict],
) -> list[dict]:
    incident_age_by_job = {
        str(item.get("job_id") or ""): int(item.get("age_seconds") or 0)
        for item in incidents
        if item.get("job_id")
    }
    ordered = sorted(
        (dict(item) for item in recommendations),
        key=lambda item: _recommendation_sort_key(
            item,
            incident_age_by_job.get(str(item.get("job_id") or ""), 0),
        ),
    )
    for index, item in enumerate(ordered, start=1):
        item["priority"] = index
    return ordered


def _build_supervisor_overview_and_message(
    summary: dict,
    recommendations: list[dict],
    dossiers: list[dict] | None = None,
) -> tuple[str, str]:
    recs = [item for item in (recommendations or []) if isinstance(item, dict)]
    dossiers = [item for item in (dossiers or []) if isinstance(item, dict)]

    high_conf = sum(
        1 for item in recs
        if str(item.get("recommended_action_confidence") or "").strip() == "high"
    )
    auto_recoverable = sum(1 for item in recs if item.get("auto_recoverable"))
    user_held = sum(1 for item in recs if str(item.get("owner") or "").strip() == "user")
    system_held = sum(1 for item in recs if str(item.get("owner") or "").strip() == "system")

    cause_counts = _collect_resource_readiness_cause_counts(dossiers)

    overview_parts = [
        f"{summary.get('total_open', 0)} open incidents",
        f"{summary.get('critical', 0)} critical",
        f"{summary.get('warning', 0)} warning",
        f"{summary.get('info', 0)} info",
    ]
    if high_conf:
        overview_parts.append(f"{high_conf} high-confidence recommendation{'s' if high_conf != 1 else ''}")
    if auto_recoverable:
        overview_parts.append(f"{auto_recoverable} auto-recoverable")

    message_parts: list[str] = []
    top = recs[0] if recs else None
    if top:
        top_safe_action = str(top.get("safe_action") or "").strip()
        top_confidence = str(top.get("recommended_action_confidence") or "").strip()
        if top_safe_action:
            message_parts.append(
                f"Top priority is job '{top.get('job_name')}' via {top_safe_action} ({top_confidence or 'low'} confidence)."
            )
        else:
            message_parts.append(
                f"Top priority is job '{top.get('job_name')}' and it still needs operator review."
            )

    if system_held:
        message_parts.append(
            f"{system_held} incident{'s' if system_held != 1 else ''} "
            f"{'is' if system_held == 1 else 'are'} system-owned."
        )
    if user_held:
        message_parts.append(
            f"{user_held} incident{'s' if user_held != 1 else ''} "
            f"{'is' if user_held == 1 else 'are'} waiting on user action."
        )
    latest_auto_recovery = _find_latest_auto_recovery_event(dossiers)
    if latest_auto_recovery:
        auto_job_label = (
            str(latest_auto_recovery.get("job_name") or "").strip()
            or str(latest_auto_recovery.get("job_id") or "").strip()
            or "unknown job"
        )
        auto_issue = str(latest_auto_recovery.get("issue_kind") or "unknown_issue").strip()
        auto_action = str(latest_auto_recovery.get("safe_action") or "unknown_action").strip()
        auto_status = str(latest_auto_recovery.get("resulting_status") or "unknown_status").strip()
        message_parts.append(
            f"Most recent watchdog recovery handled {auto_issue} via {auto_action} "
            f"for job '{auto_job_label}' and left it in {auto_status}."
        )
    if cause_counts:
        top_cause, top_count = cause_counts.most_common(1)[0]
        message_parts.append(f"Most common resource blocker cause is {top_cause} ({top_count}).")

    if not message_parts:
        message_parts.append(
            "Prioritize critical repair / failure incidents first, then user-held gates, and finally informational confirmation gates."
        )

    return ", ".join(overview_parts) + ".", " ".join(message_parts)


def _build_supervisor_focus_summary(
    recommendations: list[dict],
    dossiers: list[dict] | None = None,
) -> dict:
    recs = [item for item in (recommendations or []) if isinstance(item, dict)]
    dossiers = [item for item in (dossiers or []) if isinstance(item, dict)]

    owner_counts: Counter[str] = Counter(
        str(item.get("owner") or "").strip()
        for item in recs
        if item.get("owner")
    )
    incident_type_counts: Counter[str] = Counter(
        str(item.get("incident_type") or "").strip()
        for item in recs
        if item.get("incident_type")
    )
    cause_counts = _collect_resource_readiness_cause_counts(dossiers)

    top_owner = owner_counts.most_common(1)[0][0] if owner_counts else None
    top_incident_type = incident_type_counts.most_common(1)[0][0] if incident_type_counts else None
    top_blocker_cause = cause_counts.most_common(1)[0][0] if cause_counts else None
    high_confidence_total = sum(
        1 for item in recs
        if str(item.get("recommended_action_confidence") or "").strip() == "high"
    )
    auto_recoverable_total = sum(1 for item in recs if item.get("auto_recoverable"))
    user_wait_total = sum(
        1 for item in recs
        if str(item.get("owner") or "").strip() == "user"
    )
    top_recommendation = recs[0] if recs else {}
    top_failure_layer = (
        str(
            top_recommendation.get("failure_layer")
            or _infer_failure_layer(str(top_incident_type or "").strip())
        ).strip()
        or None
    )
    top_safe_action = str(top_recommendation.get("safe_action") or "").strip() or None
    top_rollback_level = str(top_recommendation.get("rollback_level") or "").strip() or None
    top_rollback_target = str(top_recommendation.get("rollback_target") or "").strip() or None
    top_job_id = str(top_recommendation.get("job_id") or "").strip()
    top_historical_policy = top_recommendation.get("historical_policy") or {}
    top_historical_rollback_level = (
        str(top_historical_policy.get("preferred_rollback_level") or "").strip() or None
    )
    top_historical_rollback_alignment = top_historical_policy.get("rollback_level_aligns_with_current")
    top_historical_rollback_target = (
        str(top_historical_policy.get("preferred_rollback_target") or "").strip() or None
    )
    top_historical_rollback_target_alignment = top_historical_policy.get("rollback_target_aligns_with_current")
    dossier_by_job = {
        str(item.get("job_id") or "").strip(): item
        for item in dossiers
        if str(item.get("job_id") or "").strip()
    }
    top_environment_failure = (dossier_by_job.get(top_job_id) or {}).get("environment_failure") or {}

    primary_lane = "operator_review"
    lane_reason = "No single dominant lane is established yet."
    next_best_operator_move = "inspect_top_incident"
    next_best_operator_reason = "Start from the highest-priority incident and inspect its current evidence."
    if top_incident_type in {"execution_confirmation", "plan_confirmation"}:
        primary_lane = "confirmation_gates"
        lane_reason = "The highest-priority incidents are waiting at confirmation gates."
        next_best_operator_move = "review_confirmation_gate"
        next_best_operator_reason = "Confirm or revise the pending plan / execution graph so the blocked lane can continue."
    elif top_owner == "user" and top_incident_type in {"authorization", "repair", "resource_clarification"}:
        primary_lane = "user_intervention"
        lane_reason = "The current project is mainly waiting on explicit user intervention."
        if top_incident_type == "authorization":
            next_best_operator_move = "resolve_authorization_request"
            next_best_operator_reason = "Review and approve or reject the pending command so execution can continue."
        elif top_incident_type == "repair":
            next_best_operator_move = "resolve_repair_request"
            next_best_operator_reason = "Inspect the failing command and choose a repair path before resuming."
        else:
            next_best_operator_move = "resolve_resource_clarification"
            next_best_operator_reason = "Clarify the missing or ambiguous resource details so planning/binding can continue."
    elif top_blocker_cause:
        resource_guidance = _build_resource_readiness_guidance(top_blocker_cause)
        primary_lane = "resource_readiness"
        lane_reason = resource_guidance["lane_reason"]
        next_best_operator_move = resource_guidance["next_move"]
        next_best_operator_reason = resource_guidance["next_reason"]
    elif top_failure_layer == "resource_binding":
        resource_guidance = _build_resource_readiness_guidance(None)
        primary_lane = "resource_readiness"
        lane_reason = resource_guidance["lane_reason"]
        next_best_operator_move = resource_guidance["next_move"]
        next_best_operator_reason = resource_guidance["next_reason"]
    elif top_environment_failure:
        environment_guidance = _build_environment_readiness_guidance(top_environment_failure)
        primary_lane = "environment_readiness"
        lane_reason = environment_guidance["lane_reason"]
        next_best_operator_move = environment_guidance["next_move"]
        next_best_operator_reason = environment_guidance["next_reason"]
    elif (
        not top_safe_action
        and top_historical_rollback_alignment is True
        and top_historical_rollback_level in {"abstract_plan", "execution_ir", "dag"}
    ):
        primary_lane = "rollback_review"
        if top_historical_rollback_level == "abstract_plan":
            lane_reason = "Project memory and the current dossier both point to abstract-plan rollback review before runtime retry."
            next_best_operator_move = "review_rollback_scope"
            next_best_operator_reason = "Re-open the abstract plan in chat, confirm the rollback scope, and only then decide whether runtime recovery is still appropriate."
        elif top_historical_rollback_level == "execution_ir":
            lane_reason = "Project memory and the current dossier both point to execution-semantics rollback review before runtime retry."
            next_best_operator_move = "review_rollback_scope"
            next_best_operator_reason = "Review resource clarification / execution semantics first, then rebuild the execution graph before retrying runtime recovery."
        else:
            lane_reason = "Project memory and the current dossier both point to expanded-DAG rollback review before runtime retry."
            next_best_operator_move = "review_rollback_scope"
            next_best_operator_reason = "Review the expanded DAG changes in chat and reconfirm the execution graph before attempting runtime recovery."
    elif top_safe_action or auto_recoverable_total > 0:
        primary_lane = "runtime_recovery"
        lane_reason = "The current project is mainly in runtime recovery / normalization work."
        next_best_operator_move = "apply_runtime_recovery"
        next_best_operator_reason = "Apply the top safe action or normalization path, then recheck job state consistency."

    latest_auto_recovery = _find_latest_auto_recovery_event(dossiers)
    payload = {
        "top_owner": top_owner,
        "top_incident_type": top_incident_type,
        "top_blocker_cause": top_blocker_cause,
        "high_confidence_total": high_confidence_total,
        "auto_recoverable_total": auto_recoverable_total,
        "user_wait_total": user_wait_total,
        "top_failure_layer": top_failure_layer,
        "top_safe_action": top_safe_action,
        "top_rollback_level": top_rollback_level,
        "top_historical_rollback_level": top_historical_rollback_level,
        "top_historical_rollback_alignment": top_historical_rollback_alignment,
        "primary_lane": primary_lane,
        "lane_reason": lane_reason,
        "next_best_operator_move": next_best_operator_move,
        "next_best_operator_reason": next_best_operator_reason,
    }
    if top_rollback_target:
        payload["top_rollback_target"] = top_rollback_target
    if top_historical_rollback_target or top_historical_rollback_target_alignment is not None:
        payload.update(
            {
                "top_historical_rollback_target": top_historical_rollback_target,
                "top_historical_rollback_target_alignment": top_historical_rollback_target_alignment,
            }
        )
    if latest_auto_recovery:
        payload.update(
            {
                "latest_auto_recovery_issue": latest_auto_recovery.get("issue_kind"),
                "latest_auto_recovery_action": latest_auto_recovery.get("safe_action"),
                "latest_auto_recovery_status": latest_auto_recovery.get("resulting_status"),
                "latest_auto_recovery_pending_types": latest_auto_recovery.get("pending_types"),
                "latest_auto_recovery_job_id": latest_auto_recovery.get("job_id"),
            }
        )
    return payload


def _parse_event_timestamp(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _find_latest_auto_recovery_event(dossiers: list[dict] | None) -> dict | None:
    latest_event: dict | None = None
    latest_key: tuple[int, datetime, int, int] | None = None
    for dossier_index, dossier in enumerate(dossiers or []):
        job_id = str(dossier.get("job_id") or "").strip() or None
        job_name = str(dossier.get("job_name") or "").strip() or None
        for event_index, event in enumerate(dossier.get("auto_recovery_events") or []):
            if not isinstance(event, dict):
                continue
            event_time = _parse_event_timestamp(event.get("ts"))
            sort_key = (
                1 if event_time else 0,
                event_time or datetime.min.replace(tzinfo=timezone.utc),
                dossier_index,
                event_index,
            )
            if latest_key is None or sort_key > latest_key:
                latest_key = sort_key
                latest_event = {
                    **event,
                    "job_id": job_id,
                    "job_name": job_name,
                }
    return latest_event


def _collect_resource_readiness_cause_counts(
    dossiers: list[dict] | None,
) -> Counter[str]:
    cause_counts: Counter[str] = Counter()
    for dossier in dossiers or []:
        if not isinstance(dossier, dict):
            continue
        for key, value in ((dossier.get("resource_graph") or {}).get("blocking_cause_counts") or {}).items():
            try:
                cause_counts[str(key)] += int(value)
            except Exception:
                continue
        resource_decisions = dossier.get("resource_decisions") or {}
        try:
            mismatch_total = int(resource_decisions.get("mismatch_total", 0) or 0)
        except Exception:
            mismatch_total = 0
        try:
            stale_decision_total = int(resource_decisions.get("stale_decision_total", 0) or 0)
        except Exception:
            stale_decision_total = 0
        if mismatch_total:
            cause_counts["registered_path_mismatch"] += mismatch_total
        if stale_decision_total:
            cause_counts["stale_resource_decision"] += stale_decision_total
    return cause_counts


def _build_resource_readiness_guidance(blocker_cause: str | None) -> dict[str, object]:
    cause = str(blocker_cause or "").strip()
    if cause == "stale_resource_decision":
        return {
            "lane_reason": "Resource readiness is dominated by stale previously-kept resource registration decisions.",
            "next_move": "review_stale_resource_decision",
            "next_reason": "Re-open the recognized resource mismatch, compare the latest detected path against the registered path, and choose whether to keep or replace the registration again.",
            "step_codes": [
                "open_task",
                "inspect_resource_candidates",
                "review_stale_resource_decision",
                "recheck_task_state",
            ],
        }
    if cause == "registered_path_mismatch":
        return {
            "lane_reason": "Resource readiness is dominated by recognized resources disagreeing with current registered paths.",
            "next_move": "resolve_resource_registration_mismatch",
            "next_reason": "Review the recognized resource mismatch and either replace the registered path or explicitly keep the current registration before resuming binding.",
            "step_codes": [
                "open_task",
                "inspect_resource_candidates",
                "resolve_resource_registration_mismatch",
                "recheck_task_state",
            ],
        }
    if cause == "missing_primary_resource":
        return {
            "lane_reason": "Resource readiness is dominated by missing primary reference / annotation resources.",
            "next_move": "register_primary_resource",
            "next_reason": "Register or confirm the required reference FASTA / annotation GTF before retrying planning or bindings.",
            "step_codes": [
                "open_task",
                "inspect_resource_blockers",
                "register_or_select_primary_resource",
                "recheck_task_state",
            ],
        }
    if cause == "ambiguous_candidates":
        return {
            "lane_reason": "Resource readiness is dominated by ambiguous resource candidate selection.",
            "next_move": "resolve_ambiguous_resource_candidates",
            "next_reason": "Inspect the competing resource candidates and choose the preferred match before resuming binding.",
            "step_codes": [
                "open_task",
                "inspect_resource_candidates",
                "choose_preferred_resource_candidate",
                "recheck_task_state",
            ],
        }
    if cause == "stale_derived_resource":
        return {
            "lane_reason": "Resource readiness is dominated by stale derived resources.",
            "next_move": "refresh_stale_derived_resource",
            "next_reason": "Refresh or rebuild the stale derived resource so downstream execution uses an up-to-date index or artifact.",
            "step_codes": [
                "open_task",
                "inspect_stale_resource_lineage",
                "refresh_or_rebuild_derived_resource",
                "recheck_task_state",
            ],
        }
    if cause == "missing_runtime_resource":
        return {
            "lane_reason": "Resource readiness is dominated by missing runtime resources.",
            "next_move": "restore_missing_runtime_resource",
            "next_reason": "Inspect current bindings/runtime availability and restore the missing runtime resource before resuming execution.",
            "step_codes": [
                "open_task",
                "inspect_runtime_resource_bindings",
                "restore_runtime_resource_binding",
                "recheck_task_state",
            ],
        }
    return {
        "lane_reason": f"Resource readiness is the dominant blocker line ({cause or 'unknown'}).",
        "next_move": "resolve_resource_readiness",
        "next_reason": "Fix the dominant resource blocker line before attempting further execution recovery.",
        "step_codes": [
            "open_task",
            "inspect_bindings_and_resume",
            "recheck_task_state",
        ],
    }


def _build_environment_readiness_guidance(environment_failure: dict | None) -> dict[str, object]:
    env = environment_failure or {}
    failure_kind = str(env.get("failure_kind") or "install_failed").strip()
    failed_packages = [
        str(pkg).strip()
        for pkg in (env.get("failed_packages") or [])
        if str(pkg).strip()
    ]
    package_fragment = ""
    if failed_packages:
        package_fragment = f" for package(s) {', '.join(failed_packages[:3])}"

    if failure_kind == "missing_package":
        return {
            "lane_reason": "Environment readiness is dominated by missing or mismatched runtime packages.",
            "next_move": "inspect_environment_failure",
            "next_reason": (
                "Inspect the failed package mapping, candidate aliases, and implicated step requirements"
                f"{package_fragment} before retrying environment preparation."
            ),
            "step_codes": [
                "open_task",
                "inspect_environment_failure",
                "recheck_task_state",
            ],
        }
    return {
        "lane_reason": "Environment readiness is dominated by runtime environment install failure.",
        "next_move": "inspect_environment_failure",
        "next_reason": (
            "Inspect the environment preparation diagnostics, install output, and implicated step packages"
            f"{package_fragment} before retrying execution."
        ),
        "step_codes": [
            "open_task",
            "inspect_environment_failure",
            "recheck_task_state",
        ],
    }


def _build_project_playbook(
    focus_summary: dict | None,
    recommendations: list[dict] | None = None,
) -> dict:
    focus_summary = focus_summary or {}
    recommendations = [item for item in (recommendations or []) if isinstance(item, dict)]
    top = recommendations[0] if recommendations else {}
    move = str(focus_summary.get("next_best_operator_move") or "inspect_top_incident").strip()
    lane = str(focus_summary.get("primary_lane") or "operator_review").strip()
    step_codes: list[str] = []

    if move == "review_confirmation_gate":
        step_codes.extend([
            "open_chat",
            "confirm_or_edit_execution",
            "recheck_task_state",
        ])
    elif move == "resolve_authorization_request":
        step_codes.extend([
            "open_task",
            "review_and_authorize_command",
            "recheck_task_state",
        ])
    elif move == "resolve_repair_request":
        step_codes.extend([
            "open_task",
            "review_failure_and_choose_repair",
            "recheck_task_state",
        ])
    elif move == "resolve_resource_clarification":
        step_codes.extend([
            "open_chat",
            "provide_missing_resource_clarification",
            "recheck_task_state",
        ])
    elif move == "inspect_environment_failure":
        step_codes.extend([
            "open_task",
            "inspect_environment_failure",
            "recheck_task_state",
        ])
    elif move in {
        "resolve_resource_readiness",
        "review_stale_resource_decision",
        "resolve_resource_registration_mismatch",
        "register_primary_resource",
        "resolve_ambiguous_resource_candidates",
        "refresh_stale_derived_resource",
        "restore_missing_runtime_resource",
    }:
        step_codes.extend(
            list(
                _build_resource_readiness_guidance(
                    focus_summary.get("top_blocker_cause")
                ).get("step_codes")
                or []
            )
        )
    elif move == "review_rollback_scope":
        rollback_level = str(top.get("rollback_level") or "").strip()
        if rollback_level == "abstract_plan":
            step_codes.extend([
                "open_chat",
                "confirm_or_edit_plan",
                "recheck_task_state",
            ])
        elif rollback_level == "execution_ir":
            step_codes.extend([
                "open_chat",
                "provide_missing_resource_clarification",
                "recheck_task_state",
            ])
        else:
            step_codes.extend([
                "open_chat",
                "confirm_or_edit_execution",
                "recheck_task_state",
            ])
    elif move == "apply_runtime_recovery":
        step_codes.extend([
            "open_task",
            "apply_safe_action",
            "recheck_task_state",
        ])
    else:
        step_codes.extend([
            "open_task",
            "inspect_task",
            "recheck_task_state",
        ])

    historical_policy = top.get("historical_policy") or {}
    has_historical_signal = bool(top.get("historical_guidance")) or bool(
        historical_policy.get("support_count")
    )
    if has_historical_signal:
        if step_codes and step_codes[0] in {"open_task", "open_chat"}:
            step_codes.insert(1, "review_historical_policy")
        else:
            step_codes.insert(0, "review_historical_policy")

    if lane == "runtime_recovery" and top.get("safe_action") in {
        "retry_resume_chain",
        "step_reenter",
        "normalize_orphan_pending_state",
    }:
        step_codes.append("resume_job")

    deduped_step_codes: list[str] = []
    for code in step_codes:
        if code and code not in deduped_step_codes:
            deduped_step_codes.append(code)

    return {
        "goal": lane or "operator_review",
        "next_move": move,
        "step_codes": deduped_step_codes,
    }


def _clear_pending_request_metadata(job: AnalysisJob) -> None:
    job.pending_auth_request_id = None
    job.pending_repair_request_id = None
    job.pending_step_key = None
    if getattr(job, "pending_interaction_type", None) in {"authorization", "repair"}:
        job.pending_interaction_type = None
        job.pending_interaction_payload_json = None


def _normalized_status_for_orphan_pending(job: AnalysisJob, effective_status: str | None) -> str:
    status = effective_status or getattr(job, "status", None) or "interrupted"
    if status in {"waiting_for_authorization", "waiting_for_repair"}:
        return "interrupted"
    return status


async def _record_supervisor_resolution_event(
    session: AsyncSession,
    job: AnalysisJob,
    incident: dict,
    *,
    safe_action: str,
    rollback_level: str,
    rollback_target: str,
    outcome_status: str,
    detail: str | None = None,
    user_contributed: bool = True,
) -> None:
    project_id = getattr(job, "project_id", None)
    if not project_id:
        return

    step_label = str(
        incident.get("current_step_name")
        or incident.get("current_step_key")
        or getattr(job, "pending_step_key", None)
        or ""
    ).strip()
    step_fragment = f" at step '{step_label}'" if step_label else ""
    description = (
        f"Supervisor resolved {incident.get('incident_type', 'unknown')} incident "
        f"for job '{getattr(job, 'name', None) or getattr(job, 'id', 'unknown')}'{step_fragment}."
    )
    resolution_parts = [
        f"Applied safe action '{safe_action}'",
        f"rollback_level={rollback_level}",
        f"rollback_target={rollback_target}",
        f"failure_layer={_infer_failure_layer(str(incident.get('incident_type') or '').strip())}",
        f"reconfirmation_required={'true' if _requires_reconfirmation(rollback_level) else 'false'}",
        f"resulting_status={outcome_status}",
    ]
    if detail:
        resolution_parts.append(detail)
    env_fragment = _build_environment_failure_memory_fragment(incident)
    if env_fragment:
        resolution_parts.append(env_fragment)

    try:
        from tune.core.memory.project_memory import write_execution_event

        await write_execution_event(
            session,
            project_id=project_id,
            event_type="supervisor_resolution",
            description=description,
            resolution="; ".join(resolution_parts),
            user_contributed=user_contributed,
        )
    except Exception:
        log.exception(
            "Failed to record supervisor resolution memory for job_id=%s safe_action=%s",
            getattr(job, "id", None),
            safe_action,
        )


async def _fetch_recent_job_logs(
    session: AsyncSession,
    job_id: str,
    limit: int = 8,
) -> list[dict]:
    from tune.core.models import JobLog

    try:
        rows = (
            await session.execute(
                select(JobLog)
                .where(JobLog.job_id == job_id)
                .order_by(JobLog.ts.desc())
                .limit(limit)
            )
        ).scalars().all()
    except Exception:
        return []
    return [
        {
            "stream": row.stream,
            "line": row.line,
            "ts": row.ts.isoformat() if row.ts else None,
        }
        for row in reversed(rows)
    ]


def _extract_auto_recovery_events(recent_logs: list[dict] | None) -> list[dict]:
    events: list[dict] = []
    for item in recent_logs or []:
        line = str(item.get("line") or "").strip()
        if not line:
            continue
        match = _WATCHDOG_AUTO_RECOVERY_RE.match(line)
        if not match:
            continue
        events.append(
            {
                "source": "watchdog",
                "issue_kind": match.group("issue_kind"),
                "safe_action": match.group("safe_action"),
                "resulting_status": match.group("resulting_status"),
                "pending_types": match.group("pending_types"),
                "line": line,
                "ts": item.get("ts"),
            }
        )
    return events


async def _fetch_recent_user_decisions(
    session: AsyncSession,
    job_id: str,
    limit: int = 5,
) -> list[dict]:
    from tune.core.models import UserDecision

    try:
        rows = (
            await session.execute(
                select(UserDecision)
                .where(UserDecision.job_id == job_id)
                .order_by(UserDecision.created_at.desc())
                .limit(limit)
            )
        ).scalars().all()
    except Exception:
        return []
    return [
        {
            "decision_type": row.decision_type,
            "payload": row.payload_json,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
        for row in reversed(rows)
    ]


async def _fetch_recent_auth_requests(
    session: AsyncSession,
    job_id: str,
    limit: int = 5,
) -> list[dict]:
    from tune.core.models import CommandAuthorizationRequest

    try:
        rows = (
            await session.execute(
                select(CommandAuthorizationRequest)
                .where(CommandAuthorizationRequest.job_id == job_id)
                .order_by(CommandAuthorizationRequest.requested_at.desc())
                .limit(limit)
            )
        ).scalars().all()
    except Exception:
        return []
    return [
        {
            "id": row.id,
            "status": row.status,
            "command_type": row.command_template_type,
            "requested_at": row.requested_at.isoformat() if row.requested_at else None,
            "resolved_at": row.resolved_at.isoformat() if row.resolved_at else None,
        }
        for row in reversed(rows)
    ]


async def _fetch_recent_repair_requests(
    session: AsyncSession,
    job_id: str,
    limit: int = 5,
) -> list[dict]:
    from tune.core.models import RepairRequest

    try:
        rows = (
            await session.execute(
                select(RepairRequest)
                .where(RepairRequest.job_id == job_id)
                .order_by(RepairRequest.created_at.desc())
                .limit(limit)
            )
        ).scalars().all()
    except Exception:
        return []
    return [
        {
            "id": row.id,
            "status": row.status,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "resolved_at": row.resolved_at.isoformat() if row.resolved_at else None,
        }
        for row in reversed(rows)
    ]


async def _fetch_recent_step_runs(
    session: AsyncSession,
    job_id: str,
    limit: int = 12,
) -> list[dict]:
    from tune.core.models import AnalysisStepRun

    try:
        rows = (
            await session.execute(
                select(AnalysisStepRun)
                .where(AnalysisStepRun.job_id == job_id)
                .order_by(
                    AnalysisStepRun.finished_at.desc(),
                    AnalysisStepRun.started_at.desc(),
                )
                .limit(limit)
            )
        ).scalars().all()
    except Exception:
        return []
    return [
        {
            "step_key": row.step_key,
            "display_name": row.display_name,
            "status": row.status,
            "started_at": row.started_at.isoformat() if row.started_at else None,
            "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        }
        for row in reversed(rows)
    ]


async def _fetch_recent_artifacts(
    session: AsyncSession,
    job_id: str,
    limit: int = 12,
) -> list[dict]:
    from tune.core.models import ArtifactRecord

    try:
        rows = (
            await session.execute(
                select(ArtifactRecord)
                .where(ArtifactRecord.job_id == job_id)
                .order_by(ArtifactRecord.created_at.desc())
                .limit(limit)
            )
        ).scalars().all()
    except Exception:
        return []
    return [
        {
            "step_key": row.step_key,
            "step_type": row.step_type,
            "slot_name": row.slot_name,
            "artifact_role": row.artifact_role,
            "file_path": row.file_path,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
        for row in reversed(rows)
    ]


def _humanize_event_token(token: str | None) -> str:
    text = str(token or "").strip().replace("_", " ")
    if not text:
        return "unknown"
    return text[0].upper() + text[1:]


def _artifact_result_kind(file_path: str | None) -> str:
    suffix = Path(str(file_path or "")).suffix.lower()
    if suffix == ".html":
        return "report"
    if suffix == ".csv":
        return "table"
    if suffix in {".png", ".jpg", ".jpeg", ".svg", ".pdf"}:
        return "plot"
    return "file"


def _artifact_timeline_title(file_path: str | None) -> str:
    result_kind = _artifact_result_kind(file_path)
    if result_kind == "report":
        return "Report ready"
    if result_kind == "table":
        return "Table ready"
    if result_kind == "plot":
        return "Plot ready"
    return "Result file ready"


def _decision_timeline_entry(decision: dict) -> dict | None:
    decision_type = str(decision.get("decision_type") or "").strip()
    if not decision_type:
        return None
    payload = decision.get("payload") or {}
    title = _humanize_event_token(decision_type)
    detail = None
    category = "confirmation"

    if decision_type == "repair_choice":
        choice = str(payload.get("choice") or "").strip()
        title = "Repair decision recorded"
        detail = f"choice={choice}" if choice else None
        category = "recovery"
    elif decision_type.startswith("supervisor_"):
        title = "Supervisor action executed"
        detail = decision_type.removeprefix("supervisor_")
        category = "recovery"
    elif decision_type == "authorization_approved":
        title = "Authorization approved"
        detail = payload.get("command_type")
    elif decision_type == "authorization_rejected":
        title = "Authorization rejected"
        detail = payload.get("command_type")
    elif decision_type == "plan_confirmed":
        title = "Plan confirmed"
    elif decision_type == "plan_modified":
        title = "Plan modified"

    return {
        "ts": decision.get("created_at"),
        "kind": "user_decision",
        "source": "decision_log",
        "title": title,
        "detail": str(detail) if detail else None,
        "category": category,
    }


def _build_job_timeline(
    job,
    *,
    recent_logs: list[dict] | None = None,
    recent_decisions: list[dict] | None = None,
    auth_requests: list[dict] | None = None,
    repair_requests: list[dict] | None = None,
    step_runs: list[dict] | None = None,
    artifacts: list[dict] | None = None,
    runtime_diagnostics: list[dict] | None = None,
    rollback_guidance: dict | None = None,
) -> list[dict]:
    events: list[dict] = []
    step_display_by_key = {
        str(item.get("step_key") or "").strip(): str(item.get("display_name") or "").strip()
        for item in step_runs or []
        if str(item.get("step_key") or "").strip()
    }

    if getattr(job, "created_at", None):
        events.append(
            {
                "ts": job.created_at.isoformat() if getattr(job.created_at, "isoformat", None) else job.created_at,
                "kind": "job_lifecycle",
                "source": "job",
                "category": "step",
                "title": "Job created",
                "detail": getattr(job, "goal", None) or getattr(job, "name", None),
            }
        )
    if getattr(job, "started_at", None):
        events.append(
            {
                "ts": job.started_at.isoformat() if getattr(job.started_at, "isoformat", None) else job.started_at,
                "kind": "job_lifecycle",
                "source": "job",
                "category": "step",
                "title": "Job execution started",
                "detail": None,
            }
        )
    if getattr(job, "ended_at", None):
        events.append(
            {
                "ts": job.ended_at.isoformat() if getattr(job.ended_at, "isoformat", None) else job.ended_at,
                "kind": "job_lifecycle",
                "source": "job",
                "category": "step",
                "title": "Job execution ended",
                "detail": f"status={getattr(job, 'status', 'unknown')}",
            }
        )

    for req in auth_requests or []:
        if req.get("requested_at"):
            events.append(
                {
                    "ts": req.get("requested_at"),
                    "kind": "authorization_request",
                    "source": "authorization_request",
                    "category": "confirmation",
                    "title": "Authorization requested",
                    "detail": req.get("command_type"),
                }
            )

    for req in repair_requests or []:
        if req.get("created_at"):
            events.append(
                {
                    "ts": req.get("created_at"),
                    "kind": "repair_request",
                    "source": "repair_request",
                    "category": "recovery",
                    "title": "Repair requested",
                    "detail": None,
                }
            )

    timeline_step_statuses = {
        "running",
        "succeeded",
        "failed",
        "skipped",
        "awaiting_authorization",
        "waiting_for_human_repair",
        "binding_missing",
    }
    for step in step_runs or []:
        status = str(step.get("status") or "").strip()
        if status not in timeline_step_statuses:
            continue
        ts = step.get("finished_at") or step.get("started_at")
        if not ts:
            continue
        step_name = str(step.get("display_name") or step.get("step_key") or "step").strip()
        if status == "running":
            title = "Step started"
        elif status == "succeeded":
            title = "Step completed"
        elif status == "failed":
            title = "Step failed"
        elif status == "skipped":
            title = "Step skipped"
        elif status == "awaiting_authorization":
            title = "Step paused for authorization"
        elif status == "waiting_for_human_repair":
            title = "Step paused for repair"
        else:
            title = "Step blocked on bindings"
        events.append(
            {
                "ts": ts,
                "kind": "step_run",
                "source": "step_run",
                "category": "step",
                "title": title,
                "detail": step_name,
            }
        )

    for artifact in artifacts or []:
        ts = artifact.get("created_at")
        if not ts:
            continue
        role = str(artifact.get("artifact_role") or artifact.get("slot_name") or "artifact").strip()
        step_key = str(artifact.get("step_key") or "").strip()
        step_label = (
            step_display_by_key.get(step_key)
            or step_key
            or str(artifact.get("step_type") or "step").strip()
        )
        path = str(artifact.get("file_path") or "").strip()
        file_name = Path(path).name if path else ""
        detail = step_label or "step"
        if file_name:
            detail = f"{detail} -> {file_name}"
        if role and role not in {file_name, "artifact"}:
            detail = f"{detail} · role={role}"
        events.append(
            {
                "ts": ts,
                "kind": "artifact",
                "source": "artifact_record",
                "category": "result",
                "result_kind": _artifact_result_kind(path),
                "title": _artifact_timeline_title(path),
                "detail": detail,
            }
        )

    for item in runtime_diagnostics or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("kind") or "").strip() != "environment_prepare_failed":
            continue
        detail_parts: list[str] = []
        stage = str(item.get("stage") or "").strip()
        if stage:
            detail_parts.append(f"stage={stage}")
        failed_packages = [
            str(pkg).strip()
            for pkg in (item.get("failed_packages") or [])
            if str(pkg).strip()
        ]
        if failed_packages:
            detail_parts.append("packages=" + ",".join(failed_packages))
        detail = " · ".join(detail_parts) or (
            str(item.get("detail") or "").strip()
            or str(item.get("error_message") or "").strip()
            or None
        )
        events.append(
            {
                "ts": item.get("detected_at") or item.get("ts") or getattr(job, "ended_at", None) or getattr(job, "updated_at", None),
                "kind": "runtime_diagnostic",
                "source": "runtime_diagnostic",
                "category": "recovery",
                "title": "Environment preparation failed",
                "detail": detail,
            }
        )

    if rollback_guidance and str(rollback_guidance.get("level") or "").strip() not in {"", "step"}:
        detail_parts = [
            f"level={str(rollback_guidance.get('level') or '').strip()}",
            (
                f"target={str(rollback_guidance.get('target') or '').strip()}"
                if str(rollback_guidance.get("target") or "").strip()
                else None
            ),
            (
                "reconfirm=true"
                if rollback_guidance.get("reconfirmation_required")
                else None
            ),
            str(rollback_guidance.get("reason") or "").strip() or None,
        ]
        reference_ts = (
            get_job_progress_reference(job)
            or getattr(job, "updated_at", None)
            or getattr(job, "ended_at", None)
            or getattr(job, "started_at", None)
            or getattr(job, "created_at", None)
        )
        events.append(
            {
                "ts": reference_ts.isoformat() if getattr(reference_ts, "isoformat", None) else reference_ts,
                "kind": "rollback_guidance",
                "source": "supervisor",
                "category": "recovery",
                "title": "Rollback review recommended",
                "detail": " · ".join(part for part in detail_parts if part),
            }
        )

    for item in recent_logs or []:
        line = str(item.get("line") or "").strip()
        if not line.startswith("[watchdog]"):
            continue
        match = _WATCHDOG_AUTO_RECOVERY_RE.match(line)
        if match:
            events.append(
                {
                    "ts": item.get("ts"),
                    "kind": "auto_recovery",
                    "source": "watchdog",
                    "category": "recovery",
                    "title": "Watchdog auto recovery",
                    "detail": (
                        f"{match.group('issue_kind')} -> {match.group('safe_action')} -> "
                        f"{match.group('resulting_status')}"
                    ),
                }
            )
        else:
            events.append(
                {
                    "ts": item.get("ts"),
                    "kind": "watchdog_signal",
                    "source": "watchdog",
                    "category": "recovery",
                    "title": "Watchdog signal detected",
                    "detail": line,
                }
            )

    for decision in recent_decisions or []:
        entry = _decision_timeline_entry(decision)
        if entry:
            events.append(entry)

    events = [item for item in events if item.get("ts")]
    events.sort(key=lambda item: str(item.get("ts") or ""))
    return events[-20:]


def _build_dossier_summary(dossier: dict) -> str:
    fragments: list[str] = []

    current_step = (dossier.get("current_step") or {}).get("display_name")
    if current_step:
        fragments.append(f"step={current_step}")

    execution = dossier.get("execution_plan_summary") or {}
    if execution.get("has_expanded_dag"):
        fragments.append(
            f"dag={execution.get('group_count', 0)} groups/{execution.get('node_count', 0)} nodes"
        )
    confirmation_overview = dossier.get("execution_confirmation_overview") or {}
    if confirmation_overview:
        fragments.append(
            "exec_overview="
            + ",".join(
                [
                    f"abstract:{confirmation_overview.get('abstract_step_count', 0)}",
                    f"ir:{confirmation_overview.get('execution_ir_step_count', 0)}",
                    f"groups:{confirmation_overview.get('execution_group_count', 0)}",
                    f"added:{confirmation_overview.get('added_group_count', 0)}",
                    f"changed:{confirmation_overview.get('changed_group_count', 0)}",
                ]
            )
        )
        fragments.append(
            "exec_modes="
            + ",".join(
                [
                    f"per_sample:{confirmation_overview.get('per_sample_step_count', 0)}",
                    f"aggregate:{confirmation_overview.get('aggregate_step_count', 0)}",
                    f"global:{confirmation_overview.get('global_step_count', 0)}",
                ]
            )
        )
        fragments.append(
            "exec_changes="
            + ",".join(
                [
                    f"fan_out:{confirmation_overview.get('fan_out_change_count', 0)}",
                    f"aggregate:{confirmation_overview.get('aggregate_change_count', 0)}",
                    f"auto_injected:{confirmation_overview.get('auto_injected_change_count', 0)}",
                ]
            )
        )

    impacted_step_keys = dossier.get("impacted_step_keys") or []
    if impacted_step_keys:
        fragments.append(f"impacted_steps={len(impacted_step_keys)}")

    resource_graph = dossier.get("resource_graph") or {}
    status_counts = resource_graph.get("status_counts") or {}
    blocking_total = int(resource_graph.get("blocking_total", 0)) or sum(
        int(status_counts.get(key, 0))
        for key in ("missing", "ambiguous", "stale")
    )
    if resource_graph.get("available"):
        fragments.append(f"resource_blockers={blocking_total}")
        blocking_kind_counts = resource_graph.get("blocking_kind_counts") or {}
        if blocking_kind_counts:
            fragments.append(
                "resource_kinds="
                + ",".join(
                    f"{key}:{blocking_kind_counts[key]}"
                    for key in sorted(blocking_kind_counts)
                )
            )
        blocking_cause_counts = resource_graph.get("blocking_cause_counts") or {}
        if blocking_cause_counts:
            fragments.append(
                "resource_causes="
                + ",".join(
                    f"{key}:{blocking_cause_counts[key]}"
                    for key in sorted(blocking_cause_counts)
                )
            )
        dominant_blocker = resource_graph.get("dominant_blocker") or {}
        dominant_label = str(dominant_blocker.get("label") or "").strip()
        dominant_cause = str(dominant_blocker.get("cause") or "").strip()
        if dominant_label and dominant_cause:
            fragments.append(f"resource_focus={dominant_label}:{dominant_cause}")
    resource_decisions = dossier.get("resource_decisions") or {}
    if resource_decisions.get("available"):
        tracked_total = int(resource_decisions.get("tracked_total", 0) or 0)
        mismatch_total = int(resource_decisions.get("mismatch_total", 0) or 0)
        stale_decision_total = int(resource_decisions.get("stale_decision_total", 0) or 0)
        if tracked_total:
            fragments.append(f"resource_decisions={tracked_total}")
        if mismatch_total:
            fragments.append(f"resource_mismatches={mismatch_total}")
        if stale_decision_total:
            fragments.append(f"resource_decision_stale={stale_decision_total}")

    recent_logs = dossier.get("recent_logs") or []
    if recent_logs:
        fragments.append(f"log_tail={len(recent_logs)}")

    recent_decisions = dossier.get("recent_decisions") or []
    if recent_decisions:
        fragments.append(f"decisions={len(recent_decisions)}")

    runtime_diagnostics = dossier.get("runtime_diagnostics") or []
    if runtime_diagnostics:
        fragments.append(f"runtime_diagnostics={len(runtime_diagnostics)}")
    environment_failure = dossier.get("environment_failure") or {}
    if environment_failure:
        failure_kind = str(environment_failure.get("failure_kind") or "install_failed").strip()
        fragments.append(f"env_failure={failure_kind}")
        failed_packages = [
            str(pkg).strip()
            for pkg in (environment_failure.get("failed_packages") or [])
            if str(pkg).strip()
        ]
        if failed_packages:
            fragments.append(f"env_packages={len(failed_packages)}")
        implicated_steps = [
            item
            for item in (environment_failure.get("implicated_steps") or [])
            if isinstance(item, dict)
        ]
        if implicated_steps:
            fragments.append(f"env_steps={len(implicated_steps)}")
    pending_requests = dossier.get("pending_requests") or {}
    if pending_requests.get("active_type"):
        fragments.append(f"pending={pending_requests.get('active_type')}")
    elif pending_requests.get("diagnostic_types"):
        fragments.append(
            "pending_diagnostics="
            + ",".join(str(item) for item in pending_requests.get("diagnostic_types") or [])
        )
    if pending_requests.get("recent_authorizations"):
        fragments.append(f"auth_requests={len(pending_requests.get('recent_authorizations') or [])}")
    if pending_requests.get("recent_repairs"):
        fragments.append(f"repair_requests={len(pending_requests.get('recent_repairs') or [])}")
    auto_recovery_events = dossier.get("auto_recovery_events") or []
    if auto_recovery_events:
        fragments.append(f"auto_recoveries={len(auto_recovery_events)}")
    similar_resolutions = dossier.get("similar_resolutions") or []
    if similar_resolutions:
        fragments.append(f"memory_matches={len(similar_resolutions)}")
    rollback_hint = dossier.get("rollback_hint") or {}
    if rollback_hint.get("suggested_level"):
        fragments.append(f"rollback_hint={rollback_hint.get('suggested_level')}")

    return " · ".join(fragments) if fragments else "No additional dossier signals."


def _build_dossier_rollback_hint(dossier: dict) -> dict[str, str] | None:
    incident_type = str(dossier.get("incident_type") or "").strip()
    current_step_key = str(((dossier.get("current_step") or {}).get("step_key")) or "").strip()
    overview = dossier.get("execution_confirmation_overview") or {}
    delta = dossier.get("execution_plan_delta") or {}

    added_groups = {
        str(item.get("group_key") or "").strip()
        for item in (delta.get("added_groups") or [])
        if isinstance(item, dict) and str(item.get("group_key") or "").strip()
    }
    changed_groups = {
        str(item.get("group_key") or "").strip()
        for item in (delta.get("changed_groups") or [])
        if isinstance(item, dict) and str(item.get("group_key") or "").strip()
    }

    if incident_type in {"plan_confirmation", "execution_confirmation"}:
        return {
            "suggested_level": _infer_rollback_level(incident_type),
            "reason": "The job is already paused at a confirmation gate.",
        }

    if current_step_key and current_step_key in added_groups:
        return {
            "suggested_level": "execution_ir",
            "reason": "The current failing / blocked step was added by orchestration, so rollback should revisit execution semantics before retrying.",
        }

    if current_step_key and current_step_key in changed_groups:
        return {
            "suggested_level": "dag",
            "reason": "The current failing / blocked step was materially re-orchestrated in the expanded DAG, so rollback should revisit the execution graph.",
        }

    if incident_type in {"binding_required", "resource_clarification"} and int(overview.get("added_group_count", 0) or 0) > 0:
        return {
            "suggested_level": "execution_ir",
            "reason": "Binding / clarification is blocked after orchestration added preparation steps, so revisit execution semantics first.",
        }

    if incident_type in {"failed", "repair", "stalled", "resume_failed"} and int(overview.get("changed_group_count", 0) or 0) > 0:
        return {
            "suggested_level": "dag",
            "reason": "Runtime failure happened after orchestration changed execution groups, so revisit the expanded DAG before retrying from a step.",
        }

    return None


def _build_rollback_guidance(dossier: dict) -> dict | None:
    rollback_level = str(dossier.get("rollback_level") or "").strip() or None
    rollback_target = str(dossier.get("rollback_target") or "").strip() or None
    rollback_hint = dossier.get("rollback_hint") or {}
    reason = str(rollback_hint.get("reason") or "").strip() or None
    reconfirmation_required = bool(dossier.get("reconfirmation_required"))
    similar_resolutions = [
        item
        for item in (dossier.get("similar_resolutions") or [])
        if isinstance(item, dict)
    ]
    same_level_count = 0
    same_target_count = 0
    if rollback_level:
        same_level_count = sum(
            1
            for item in similar_resolutions
            if str(item.get("rollback_level") or "").strip() == rollback_level
        )
    if rollback_target:
        same_target_count = sum(
            1
            for item in similar_resolutions
            if str(item.get("rollback_target") or "").strip() == rollback_target
        )

    if rollback_level in {None, "step"} and not reason:
        return None

    summary_parts = [f"level={rollback_level or 'step'}"]
    if rollback_target:
        summary_parts.append(f"target={rollback_target}")
    if reconfirmation_required:
        summary_parts.append("reconfirm=true")
    if same_level_count:
        summary_parts.append(f"history_same_level={same_level_count}/{len(similar_resolutions)}")
    if same_target_count:
        summary_parts.append(f"history_same_target={same_target_count}/{len(similar_resolutions)}")
    if reason:
        summary_parts.append(reason)

    return {
        "level": rollback_level or "step",
        "target": rollback_target,
        "reconfirmation_required": reconfirmation_required,
        "reason": reason,
        "historical_matches": len(similar_resolutions),
        "historical_same_level_count": same_level_count,
        "historical_same_target_count": same_target_count,
        "summary": " · ".join(summary_parts),
    }


async def _fetch_job_current_step(session: AsyncSession, job: AnalysisJob):
    from tune.core.models import AnalysisStepRun

    if getattr(job, "current_step_id", None):
        step = (
            await session.execute(
                select(AnalysisStepRun).where(AnalysisStepRun.id == job.current_step_id)
            )
        ).scalar_one_or_none()
        if step is not None:
            return step

    pending_step_key = str(getattr(job, "pending_step_key", None) or "").strip()
    if pending_step_key:
        step = (
            await session.execute(
                select(AnalysisStepRun)
                .where(
                    AnalysisStepRun.job_id == job.id,
                    AnalysisStepRun.step_key == pending_step_key,
                )
                .order_by(AnalysisStepRun.started_at.desc(), AnalysisStepRun.finished_at.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        if step is not None:
            return step

    return (
        await session.execute(
            select(AnalysisStepRun)
            .where(AnalysisStepRun.job_id == job.id)
            .order_by(AnalysisStepRun.started_at.desc(), AnalysisStepRun.finished_at.desc())
            .limit(1)
        )
    ).scalar_one_or_none()


async def _build_job_rollback_guidance(
    session: AsyncSession,
    job: AnalysisJob,
    effective: dict,
    *,
    current_step=None,
) -> dict | None:
    incident = _derive_job_incident(job, effective, current_step=current_step)
    if incident is None:
        return None
    confirmation = _serialize_confirmation_details(job, effective.get("pending_interaction_type"))
    rollback_context = {
        "job_id": job.id,
        "incident_type": incident["incident_type"],
        "current_step": {
            "step_key": getattr(current_step, "step_key", None),
        },
        "execution_confirmation_overview": confirmation.get("execution_confirmation_overview"),
        "execution_plan_delta": confirmation.get("execution_plan_delta"),
    }
    rollback_context["rollback_hint"] = _build_dossier_rollback_hint(rollback_context)
    rollback_target, rollback_level, _diagnosis, _safe_action = _resolve_supervisor_incident_controls(
        incident,
        dossier=rollback_context,
    )
    return _build_rollback_guidance(
        {
            "rollback_level": rollback_level,
            "rollback_target": rollback_target,
            "reconfirmation_required": _requires_reconfirmation(rollback_level),
            "rollback_hint": rollback_context["rollback_hint"],
            "similar_resolutions": await _fetch_similar_project_execution_events(
                session,
                job,
                incident,
                current_step=current_step,
            ),
        }
    )


def _build_pending_request_snapshot(
    job: AnalysisJob,
    effective: dict,
    *,
    auth_requests: list[dict] | None = None,
    repair_requests: list[dict] | None = None,
) -> dict:
    runtime_diagnostics = [
        item for item in (effective.get("runtime_diagnostics") or [])
        if isinstance(item, dict)
    ]
    diagnostic_types = sorted(
        {
            str(item.get("request_type") or "request")
            for item in runtime_diagnostics
            if item.get("request_type")
        }
    )
    return {
        "active_type": effective.get("pending_interaction_type"),
        "auth_request_id": getattr(job, "pending_auth_request_id", None),
        "repair_request_id": getattr(job, "pending_repair_request_id", None),
        "has_payload": bool(effective.get("pending_interaction_payload")),
        "diagnostic_kinds": sorted(
            {
                str(item.get("kind") or "unknown")
                for item in runtime_diagnostics
                if item.get("kind")
            }
        ),
        "diagnostic_types": diagnostic_types,
        "recent_authorizations": [
            {
                "id": item.get("id"),
                "status": item.get("status"),
                "command_type": item.get("command_type"),
                "requested_at": item.get("requested_at"),
                "resolved_at": item.get("resolved_at"),
            }
            for item in (auth_requests or [])
        ],
        "recent_repairs": [
            {
                "id": item.get("id"),
                "status": item.get("status"),
                "created_at": item.get("created_at"),
                "resolved_at": item.get("resolved_at"),
            }
            for item in (repair_requests or [])
        ],
    }


def _collect_impacted_step_keys(plan_payload: dict | list | None, anchor_step_key: str) -> set[str]:
    nodes = extract_plan_steps(plan_payload)
    if not nodes and isinstance(plan_payload, dict):
        raw_nodes = plan_payload.get("nodes")
        if isinstance(raw_nodes, list):
            nodes = [dict(node) for node in raw_nodes if isinstance(node, dict)]

    anchor = str(anchor_step_key or "").strip()
    if not anchor:
        return set()

    downstream: dict[str, set[str]] = {}
    for node in nodes:
        step_key = str(node.get("step_key") or node.get("name") or "").strip()
        if not step_key:
            continue
        for dep in node.get("depends_on") or []:
            dep_key = str(dep or "").strip()
            if dep_key:
                downstream.setdefault(dep_key, set()).add(step_key)

    impacted = {anchor}
    queue = [anchor]
    while queue:
        current = queue.pop(0)
        for child in sorted(downstream.get(current, set())):
            if child not in impacted:
                impacted.add(child)
                queue.append(child)
    return impacted


async def _resolve_supervisor_step_anchor(
    session: AsyncSession,
    job: AnalysisJob,
    incident: dict,
    current_step=None,
) -> tuple[str | None, str | None]:
    from tune.core.models import AnalysisStepRun

    if current_step is not None and getattr(current_step, "step_key", None):
        return current_step.step_key, current_step.id

    incident_step_key = str(incident.get("current_step_key") or "").strip()
    if incident_step_key:
        step = (
            await session.execute(
                select(AnalysisStepRun).where(
                    AnalysisStepRun.job_id == job.id,
                    AnalysisStepRun.step_key == incident_step_key,
                )
            )
        ).scalar_one_or_none()
        return incident_step_key, getattr(step, "id", None)

    if getattr(job, "current_step_id", None):
        step = (
            await session.execute(
                select(AnalysisStepRun).where(AnalysisStepRun.id == job.current_step_id)
            )
        ).scalar_one_or_none()
        if step is not None and getattr(step, "step_key", None):
            return step.step_key, step.id

    candidate = (
        await session.execute(
            select(AnalysisStepRun)
            .where(
                AnalysisStepRun.job_id == job.id,
                AnalysisStepRun.status.in_(
                    [
                        "failed",
                        "repairable_failed",
                        "binding_missing",
                        "awaiting_authorization",
                        "waiting_for_human_repair",
                        "running",
                        "ready",
                    ]
                ),
            )
            .order_by(AnalysisStepRun.started_at.desc(), AnalysisStepRun.finished_at.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if candidate is not None and getattr(candidate, "step_key", None):
        return candidate.step_key, candidate.id

    return None, None


async def _build_job_dossier(
    session: AsyncSession,
    job: AnalysisJob,
    effective: dict,
    incident: dict,
    current_step=None,
) -> dict:
    confirmation = _serialize_confirmation_details(job, effective.get("pending_interaction_type"))
    execution_plan = _serialize_execution_plan(job)
    recent_logs = await _fetch_recent_job_logs(session, job.id)
    recent_decisions = await _fetch_recent_user_decisions(session, job.id)
    recent_auth_requests = await _fetch_recent_auth_requests(session, job.id)
    recent_repair_requests = await _fetch_recent_repair_requests(session, job.id)
    auto_recovery_events = _extract_auto_recovery_events(recent_logs)
    resource_decisions = await _fetch_project_resource_decision_snapshot(session, job.project_id)
    similar_resolutions = await _fetch_similar_project_execution_events(
        session,
        job,
        incident,
        current_step=current_step,
    )
    anchor_step_key = (
        getattr(current_step, "step_key", None)
        or incident.get("current_step_key")
        or getattr(job, "pending_step_key", None)
    )
    impacted_step_keys = sorted(
        _collect_impacted_step_keys(getattr(job, "expanded_dag_json", None), anchor_step_key)
        or _collect_impacted_step_keys(getattr(job, "resolved_plan_json", None), anchor_step_key)
    )
    environment_failure = _extract_environment_failure_signal(
        {"runtime_diagnostics": effective.get("runtime_diagnostics") or []}
    )
    rollback_context = {
        "job_id": job.id,
        "incident_type": incident["incident_type"],
        "current_step": {
            "step_key": getattr(current_step, "step_key", None),
        },
        "execution_confirmation_overview": confirmation.get("execution_confirmation_overview"),
        "execution_plan_delta": confirmation.get("execution_plan_delta"),
    }
    rollback_context["rollback_hint"] = _build_dossier_rollback_hint(rollback_context)
    rollback_target, rollback_level, _diagnosis, _safe_action = _resolve_supervisor_incident_controls(
        incident,
        dossier=rollback_context,
    )

    dossier = {
        "job_id": job.id,
        "job_name": job.name,
        "job_status": effective.get("status") or job.status,
        "thread_id": job.thread_id,
        "project_id": job.project_id,
        "incident_type": incident["incident_type"],
        "pending_interaction_type": effective.get("pending_interaction_type"),
        "failure_layer": _infer_failure_layer(incident["incident_type"]),
        "rollback_level": rollback_level,
        "rollback_target": rollback_target,
        "reconfirmation_required": _requires_reconfirmation(rollback_level),
        "error_message": effective.get("error_message"),
        "current_step": {
            "step_key": getattr(current_step, "step_key", None),
            "display_name": getattr(current_step, "display_name", None) or getattr(current_step, "step_key", None),
        },
        "impacted_step_keys": impacted_step_keys,
        "confirmation": confirmation,
        "execution_plan_summary": execution_plan["summary"],
        "execution_confirmation_overview": confirmation.get("execution_confirmation_overview"),
        "execution_ir_review": confirmation.get("execution_ir_review") or [],
        "execution_plan_delta": confirmation.get("execution_plan_delta"),
        "execution_plan_changes": confirmation.get("execution_plan_changes") or [],
        "resource_graph": _summarize_resource_graph_snapshot(getattr(job, "resource_graph_json", None)),
        "resource_decisions": resource_decisions,
        "recent_logs": recent_logs,
        "recent_decisions": recent_decisions,
        "pending_requests": _build_pending_request_snapshot(
            job,
            effective,
            auth_requests=recent_auth_requests,
            repair_requests=recent_repair_requests,
        ),
        "pending_interaction_payload": effective.get("pending_interaction_payload"),
        "runtime_diagnostics": effective.get("runtime_diagnostics") or [],
        "environment_failure": environment_failure,
        "auto_recovery_events": auto_recovery_events,
        "similar_resolutions": similar_resolutions,
    }
    dossier["rollback_hint"] = rollback_context["rollback_hint"]
    dossier["rollback_guidance"] = _build_rollback_guidance(dossier)
    dossier["summary"] = _build_dossier_summary(dossier)
    return dossier


def _derive_job_incident(
    job: AnalysisJob,
    effective: dict,
    current_step=None,
) -> dict | None:
    status = effective.get("status") or getattr(job, "status", None)
    pending_type = effective.get("pending_interaction_type")
    pending_payload = effective.get("pending_interaction_payload") or {}
    runtime_diagnostics = [
        item for item in (effective.get("runtime_diagnostics") or [])
        if isinstance(item, dict)
    ]
    now = datetime.now(timezone.utc)
    progress_ts = get_job_progress_reference(job)
    stalled_seconds = get_job_stall_age_seconds(job, now=now)
    is_stalled = is_job_stalled(job, now=now)
    if status not in {
        "awaiting_plan_confirmation",
        "waiting_for_authorization",
        "waiting_for_repair",
        "binding_required",
        "resource_clarification_required",
        "interrupted",
        "failed",
        "running",
    } and not runtime_diagnostics:
        return None
    if status == "running" and not is_stalled:
        return None

    incident_type = "unknown"
    severity = "warning"
    owner = "system"
    summary = effective.get("error_message") or ""
    detail = pending_payload.get("prompt_text") or summary
    next_action = "inspect_task"

    resolved_pending = [item for item in runtime_diagnostics if item.get("kind") == "resolved_pending_request"]
    orphan_pending = [item for item in runtime_diagnostics if item.get("kind") == "orphan_pending_request"]
    terminal_statuses = {"completed", "failed", "cancelled"}
    pending_request_types = ", ".join(
        sorted({str(item.get("request_type") or "request") for item in runtime_diagnostics})
    )

    if status in terminal_statuses and runtime_diagnostics:
        incident_type = "job_status_mismatch"
        severity = "warning"
        owner = "system"
        summary = "The job is already terminal, but stale pending request metadata is still attached."
        detail = (
            f"Terminal status '{status}' still carries stale {pending_request_types or 'pending request'} metadata."
        )
        next_action = "inspect_stale_pending_state"
    elif orphan_pending:
        incident_type = "orphan_pending_request"
        severity = "warning"
        owner = "system"
        summary = "The job references a pending request record that no longer exists."
        detail = (
            f"Missing {pending_request_types or 'pending request'} record prevents the runtime state from being normalized."
        )
        next_action = "inspect_orphan_pending_request"
    elif resolved_pending:
        incident_type = "resume_failed"
        severity = "warning"
        owner = "system"
        summary = "A human decision was already resolved, but the resume chain did not complete."
        detail = (
            f"Resolved {pending_request_types or 'pending request'} is still attached to the job, so the worker may need manual resume or cleanup."
        )
        next_action = "inspect_resume_chain"
    elif any(item.get("kind") == "environment_prepare_failed" for item in runtime_diagnostics):
        env_failure = next(
            item for item in runtime_diagnostics if item.get("kind") == "environment_prepare_failed"
        )
        incident_type = "failed"
        severity = "critical"
        owner = "system"
        summary = "Environment preparation failed before execution could start."
        detail = str(env_failure.get("error_message") or env_failure.get("detail") or summary)
        next_action = "inspect_failure_and_retry"

    if incident_type == "resume_failed":
        pass
    elif incident_type == "orphan_pending_request":
        pass
    elif incident_type == "job_status_mismatch":
        pass
    elif pending_type == "plan_confirmation":
        incident_type = "plan_confirmation"
        severity = "info"
        owner = "user"
        summary = "Abstract analysis plan is waiting for confirmation."
        detail = pending_payload.get("prompt_text") or summary
        next_action = "confirm_or_edit_plan"
    elif pending_type == "execution_confirmation":
        incident_type = "execution_confirmation"
        severity = "info"
        owner = "user"
        summary = "Execution graph is waiting for final confirmation."
        detail = pending_payload.get("prompt_text") or summary
        next_action = "confirm_or_edit_execution"
    elif pending_type == "authorization" or status == "waiting_for_authorization":
        incident_type = "authorization"
        severity = "warning"
        owner = "user"
        summary = "A command requires authorization before execution can continue."
        detail = pending_payload.get("prompt_text") or summary
        next_action = "review_and_authorize_command"
    elif pending_type == "repair" or status == "waiting_for_repair":
        incident_type = "repair"
        severity = "critical"
        owner = "user"
        summary = "A failed command needs repair input before the job can continue."
        detail = pending_payload.get("prompt_text") or pending_payload.get("stderr_excerpt") or summary
        next_action = "review_failure_and_choose_repair"
    elif status == "resource_clarification_required":
        incident_type = "resource_clarification"
        severity = "warning"
        owner = "user"
        summary = "Required resources or metadata are missing or ambiguous."
        detail = pending_payload.get("prompt_text") or summary
        next_action = "provide_missing_resource_clarification"
    elif status == "binding_required":
        incident_type = "binding_required"
        severity = "warning"
        owner = "system"
        summary = effective.get("error_message") or "Some required inputs could not be bound."
        detail = summary
        next_action = "inspect_bindings_and_resume"
    elif status == "running" and is_stalled:
        incident_type = "stalled"
        severity = "warning"
        owner = "system"
        summary = (
            f"The job is still running but has reported no progress for at least {STALL_PROGRESS_THRESHOLD_SECONDS // 60} minutes."
        )
        detail = (
            f"Current step '{getattr(current_step, 'display_name', None) or getattr(current_step, 'step_key', None) or getattr(job, 'pending_step_key', None) or 'unknown'}' has not emitted a heartbeat or log update recently."
        )
        next_action = "inspect_stalled_task"
    elif status == "interrupted":
        incident_type = "interrupted"
        severity = "critical"
        owner = "system"
        summary = effective.get("error_message") or "The job was interrupted and needs to be resumed."
        detail = summary
        next_action = "resume_job"
    elif status == "failed":
        incident_type = "failed"
        severity = "critical"
        owner = "system"
        summary = effective.get("error_message") or "The job failed."
        detail = summary
        next_action = "inspect_failure_and_retry"

    reference_ts = progress_ts if incident_type == "stalled" else (
        getattr(job, "started_at", None) or getattr(job, "created_at", None)
    )
    if reference_ts is not None and getattr(reference_ts, "tzinfo", None) is None:
        reference_ts = reference_ts.replace(tzinfo=timezone.utc)
    age_seconds = None
    if reference_ts:
        age_seconds = max(
            0,
            int((now - reference_ts).total_seconds()),
        )

    return {
        "job_id": job.id,
        "job_name": getattr(job, "name", None),
        "job_status": status,
        "project_id": getattr(job, "project_id", None),
        "thread_id": getattr(job, "thread_id", None),
        "incident_type": incident_type,
        "severity": severity,
        "owner": owner,
        "summary": summary,
        "detail": detail,
        "next_action": next_action,
        "age_seconds": age_seconds,
        "pending_interaction_type": pending_type,
        "pending_auth_request_id": getattr(job, "pending_auth_request_id", None),
        "pending_repair_request_id": getattr(job, "pending_repair_request_id", None),
        "current_step_key": getattr(current_step, "step_key", None),
        "current_step_name": getattr(current_step, "display_name", None) or getattr(current_step, "step_key", None),
        "runtime_diagnostics": runtime_diagnostics,
    }


async def _collect_job_incidents(
    session: AsyncSession,
    project: str | None = None,
) -> tuple[list[dict], dict]:
    incidents, summary, _dossiers = await _collect_job_supervisor_data(session, project=project)
    return incidents, summary


async def _collect_job_status_summary(
    session: AsyncSession,
    project: str | None = None,
) -> dict:
    q = select(AnalysisJob)
    if project:
        q = q.where(AnalysisJob.project_id == project)
    q = q.order_by(AnalysisJob.created_at.desc())

    jobs = (await session.execute(q)).scalars().all()
    status_counts: Counter[str] = Counter()
    for job in jobs:
        effective = await _get_effective_job_state(session, job)
        status_counts[str(effective.get("status") or job.status)] += 1

    active_statuses = {
        "queued",
        "running",
        "binding_required",
        "resource_clarification_required",
        "awaiting_plan_confirmation",
        "waiting_for_authorization",
        "waiting_for_repair",
    }
    active_total = sum(
        count
        for status, count in status_counts.items()
        if status in active_statuses
    )
    return {
        "total": len(jobs),
        "active": active_total,
        "by_status": dict(status_counts),
    }


def _normalize_attention_reason(incident_type: str | None) -> str | None:
    raw = str(incident_type or "").strip()
    if raw in {"authorization", "repair"}:
        return raw
    if raw in {"plan_confirmation", "execution_confirmation"}:
        return "confirmation"
    if raw == "resource_clarification":
        return "clarification"
    if raw in {"stalled", "resume_failed", "job_status_mismatch", "orphan_pending_request", "failed", "interrupted", "binding", "binding_required"}:
        return "warning"
    return None


def _build_task_attention_summary_payload(
    incidents: list[dict],
    overview: dict,
    *,
    dossiers: list[dict] | None = None,
    auto_authorize_commands: bool = False,
    reminder_threshold_seconds: int = 120,
) -> dict:
    dossiers_by_job = {
        str(item.get("job_id") or "").strip(): item
        for item in (dossiers or [])
        if isinstance(item, dict) and str(item.get("job_id") or "").strip()
    }
    normalized: list[dict] = []
    for incident in incidents:
        job_id = str(incident.get("job_id") or "").strip()
        reason = _normalize_attention_reason(incident.get("incident_type"))
        if not reason:
            continue
        dossier = dossiers_by_job.get(job_id) or {}
        rollback_guidance = dossier.get("rollback_guidance") or {}
        rollback_level = str(rollback_guidance.get("level") or "").strip() or None
        reconfirmation_required = bool(rollback_guidance.get("reconfirmation_required"))
        if (
            reason == "warning"
            and reconfirmation_required
            and rollback_level in {"abstract_plan", "execution_ir", "dag"}
        ):
            reason = "rollback_review"
        normalized.append({
            "key": f"{incident.get('job_id')}:{reason}",
            "job_id": incident.get("job_id"),
            "job_name": incident.get("job_name"),
            "thread_id": incident.get("thread_id"),
            "incident_type": incident.get("incident_type"),
            "reason": reason,
            "age_seconds": int(incident.get("age_seconds") or 0),
            "summary": incident.get("summary"),
            "severity": incident.get("severity"),
            "owner": incident.get("owner"),
            "next_action": incident.get("next_action"),
            "pending_interaction_type": incident.get("pending_interaction_type"),
            "rollback_level": rollback_level,
            "rollback_target": str(rollback_guidance.get("target") or "").strip() or None,
            "rollback_reason": str(rollback_guidance.get("reason") or "").strip() or None,
            "reconfirmation_required": reconfirmation_required,
        })

    needs_input = [item for item in normalized if item["reason"] != "warning"]
    needs_review = [item for item in normalized if item["reason"] == "warning"]

    counts = {
        "running": int(((overview or {}).get("by_status") or {}).get("running", 0) or 0),
        "authorization": sum(1 for item in needs_input if item["reason"] == "authorization"),
        "repair": sum(1 for item in needs_input if item["reason"] == "repair"),
        "confirmation": sum(1 for item in needs_input if item["reason"] == "confirmation"),
        "clarification": sum(1 for item in needs_input if item["reason"] == "clarification"),
        "rollback_review": sum(1 for item in needs_input if item["reason"] == "rollback_review"),
        "warning": len(needs_review),
        "needs_input": len(needs_input),
        "needs_review": len(needs_review),
    }

    signal = (
        "attention" if counts["needs_input"] > 0
        else "warning" if counts["needs_review"] > 0
        else "running" if counts["running"] > 0
        else "idle"
    )
    count = (
        counts["needs_input"] if counts["needs_input"] > 0
        else counts["needs_review"] if counts["needs_review"] > 0
        else counts["running"]
    )
    reminders = [
        item
        for item in needs_input
        if int(item.get("age_seconds") or 0) >= reminder_threshold_seconds
    ]

    return {
        "signal": signal,
        "count": count,
        "counts": counts,
        "needs_input": needs_input,
        "needs_review": needs_review,
        "reminders": reminders,
        "auto_authorize_commands": bool(auto_authorize_commands),
    }


async def _collect_job_attention_summary(
    session: AsyncSession,
    project: str | None = None,
) -> dict:
    from tune.core.config import get_config

    incidents, summary, _dossiers = await _collect_job_supervisor_data(session, project=project)
    overview = await _collect_job_status_summary(session, project=project)
    try:
        auto_authorize_commands = bool(get_config().auto_authorize_commands)
    except RuntimeError:
        auto_authorize_commands = False
    attention = _build_task_attention_summary_payload(
        incidents,
        overview,
        dossiers=_dossiers,
        auto_authorize_commands=auto_authorize_commands,
    )
    attention["summary"] = summary
    attention["incidents"] = incidents
    attention["overview"] = overview
    return attention


async def _collect_job_supervisor_data(
    session: AsyncSession,
    project: str | None = None,
) -> tuple[list[dict], dict, list[dict]]:
    from tune.core.models import AnalysisStepRun

    q = select(AnalysisJob)
    if project:
        q = q.where(AnalysisJob.project_id == project)
    q = q.order_by(AnalysisJob.created_at.desc())

    jobs = (await session.execute(q)).scalars().all()
    incidents: list[dict] = []
    dossiers: list[dict] = []

    for job in jobs:
        effective = await _get_effective_job_state(session, job)
        current_step = None
        if getattr(job, "current_step_id", None):
            current_step = (
                await session.execute(
                    select(AnalysisStepRun).where(AnalysisStepRun.id == job.current_step_id)
                )
            ).scalar_one_or_none()
        incident = _derive_job_incident(job, effective, current_step=current_step)
        if incident:
            incidents.append(incident)
            dossiers.append(
                await _build_job_dossier(
                    session,
                    job,
                    effective,
                    incident,
                    current_step=current_step,
                )
            )

    severity_counts = Counter(incident["severity"] for incident in incidents)
    type_counts = Counter(incident["incident_type"] for incident in incidents)
    summary = {
        "total_open": len(incidents),
        "critical": severity_counts.get("critical", 0),
        "warning": severity_counts.get("warning", 0),
        "info": severity_counts.get("info", 0),
        "by_type": dict(type_counts),
    }
    return incidents, summary, dossiers


def _build_supervisor_review_fallback(
    incidents: list[dict],
    summary: dict,
    dossiers: list[dict] | None = None,
) -> dict:
    if not incidents:
        return {
            "mode": "heuristic",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "overview": "No open incidents detected.",
            "supervisor_message": (
                "No blocked execution, authorization, repair, or clarification events are active."
            ),
            "recommendations": [],
        }

    dossier_map = {
        str(dossier.get("job_id")): dossier
        for dossier in (dossiers or [])
        if dossier.get("job_id")
    }
    recommendations: list[dict] = []
    for index, incident in enumerate(
        sorted(
            incidents,
            key=lambda item: (
                {"critical": 0, "warning": 1, "info": 2}.get(item["severity"], 9),
                -(item.get("age_seconds") or 0),
            ),
        ),
        start=1,
    ):
        dossier = dossier_map.get(incident["job_id"])
        rollback_target, rollback_level, diagnosis, safe_action = _resolve_supervisor_incident_controls(
            incident,
            dossier=dossier,
        )
        auto_recoverable, auto_recovery_kind = _infer_auto_recovery_policy(
            incident,
            rollback_level,
            safe_action,
        )
        safe_action_eligibility = _build_safe_action_eligibility(incident)
        historical_policy = _build_historical_policy(
            (dossier or {}).get("similar_resolutions") or [],
            safe_action=safe_action,
            rollback_level=rollback_level,
            rollback_target=rollback_target,
        )
        historical_guidance = _build_historical_guidance(
            (dossier or {}).get("similar_resolutions") or [],
            safe_action=safe_action,
            safe_action_eligibility=safe_action_eligibility,
            incident_type=incident.get("incident_type"),
            job_status=incident.get("job_status"),
            rollback_level=rollback_level,
            rollback_target=rollback_target,
        )
        recommended_action_confidence, recommended_action_basis = _build_recommended_action_confidence(
            safe_action=safe_action,
            auto_recoverable=auto_recoverable,
            safe_action_eligibility=safe_action_eligibility,
            historical_policy=historical_policy,
        )
        recommendations.append(
            {
                "priority": index,
                "job_id": incident["job_id"],
                "job_name": incident["job_name"],
                "thread_id": incident.get("thread_id") or "",
                "incident_type": incident["incident_type"],
                "severity": incident["severity"],
                "owner": incident["owner"],
                "diagnosis": diagnosis,
                "failure_layer": _infer_failure_layer(incident["incident_type"]),
                "rollback_level": rollback_level,
                "rollback_target": rollback_target,
                "reconfirmation_required": _requires_reconfirmation(rollback_level),
                "historical_matches": len((dossier or {}).get("similar_resolutions") or []),
                "safe_action": safe_action,
                "safe_action_eligibility": safe_action_eligibility,
                "historical_policy": historical_policy,
                "auto_recoverable": auto_recoverable,
                "auto_recovery_kind": auto_recovery_kind,
                "recommended_action_confidence": recommended_action_confidence,
                "recommended_action_basis": recommended_action_basis,
                "safe_action_note": _build_safe_action_note(
                    incident,
                    rollback_level=rollback_level,
                    safe_action=safe_action,
                    auto_recoverable=auto_recoverable,
                ),
                "historical_guidance": historical_guidance,
                "immediate_action": incident["next_action"],
                "why_now": incident["summary"],
                "dossier_summary": dossier.get("summary") if dossier else "No additional dossier signals.",
                "recovery_playbook": _build_recovery_playbook(
                    incident,
                    rollback_level=rollback_level,
                    rollback_target=rollback_target,
                    safe_action=safe_action,
                    auto_recoverable=auto_recoverable,
                ),
            }
        )

    recommendations = _finalize_supervisor_recommendations(recommendations, incidents)
    overview, supervisor_message = _build_supervisor_overview_and_message(
        summary,
        recommendations,
        dossiers,
    )
    focus_summary = _build_supervisor_focus_summary(recommendations, dossiers)
    project_playbook = _build_project_playbook(focus_summary, recommendations)

    return {
        "mode": "heuristic",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "overview": overview,
        "supervisor_message": supervisor_message,
        "focus_summary": focus_summary,
        "project_playbook": project_playbook,
        "recommendations": recommendations[:8],
    }


async def _build_supervisor_review_with_llm(
    incidents: list[dict],
    summary: dict,
    dossiers: list[dict] | None = None,
) -> dict:
    from tune.core.llm.gateway import GatewayNotConfiguredError, LLMMessage, get_gateway

    if not incidents:
        return _build_supervisor_review_fallback(incidents, summary, dossiers)

    schema = {
        "type": "object",
        "properties": {
            "overview": {"type": "string"},
            "supervisor_message": {"type": "string"},
            "recommendations": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "priority": {"type": "integer"},
                        "job_id": {"type": "string"},
                        "job_name": {"type": "string"},
                        "thread_id": {"type": "string"},
                        "incident_type": {"type": "string"},
                        "severity": {"type": "string"},
                        "owner": {"type": "string"},
                        "diagnosis": {"type": "string"},
                        "failure_layer": {"type": "string"},
                        "rollback_level": {"type": "string"},
                        "rollback_target": {"type": "string"},
                        "reconfirmation_required": {"type": "boolean"},
                        "historical_matches": {"type": "integer"},
                        "safe_action": {"type": "string"},
                        "safe_action_eligibility": {
                            "type": "object",
                            "properties": {
                                "eligible": {"type": "boolean"},
                                "current_job_status": {"type": "string"},
                                "retryable_job_statuses": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                },
                                "has_resolved_pending_signal": {"type": "boolean"},
                                "has_pending_request_reference": {"type": "boolean"},
                                "blocking_reasons": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                },
                            },
                        },
                        "historical_policy": {
                            "type": "object",
                            "properties": {
                                "preferred_safe_action": {"type": "string"},
                                "support_count": {"type": "integer"},
                                "total_matches": {"type": "integer"},
                                "confidence": {"type": "string"},
                                "current_safe_action": {"type": "string"},
                                "current_supported_count": {"type": "integer"},
                                "aligns_with_current": {"type": "boolean"},
                                "preferred_rollback_level": {"type": "string"},
                                "current_rollback_level": {"type": "string"},
                                "rollback_level_supported_count": {"type": "integer"},
                                "rollback_level_aligns_with_current": {"type": "boolean"},
                                "preferred_rollback_target": {"type": "string"},
                                "current_rollback_target": {"type": "string"},
                                "rollback_target_supported_count": {"type": "integer"},
                                "rollback_target_aligns_with_current": {"type": "boolean"},
                            },
                        },
                        "auto_recoverable": {"type": "boolean"},
                        "auto_recovery_kind": {"type": "string"},
                        "recommended_action_confidence": {"type": "string"},
                        "recommended_action_basis": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                        "safe_action_note": {"type": "string"},
                        "historical_guidance": {"type": "string"},
                        "immediate_action": {"type": "string"},
                        "why_now": {"type": "string"},
                        "dossier_summary": {"type": "string"},
                        "recovery_playbook": {
                            "type": "object",
                            "properties": {
                                "goal": {"type": "string"},
                                "rollback_target": {"type": "string"},
                                "step_codes": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                },
                            },
                        },
                    },
                    "required": [
                        "priority",
                        "job_id",
                        "job_name",
                        "thread_id",
                        "incident_type",
                        "severity",
                        "owner",
                        "diagnosis",
                        "immediate_action",
                        "why_now",
                    ],
                },
            },
            "focus_summary": {
                "type": "object",
                "properties": {
                    "top_owner": {"type": "string"},
                    "top_incident_type": {"type": "string"},
                    "top_blocker_cause": {"type": "string"},
                    "high_confidence_total": {"type": "integer"},
                    "auto_recoverable_total": {"type": "integer"},
                    "user_wait_total": {"type": "integer"},
                    "top_failure_layer": {"type": "string"},
                    "top_safe_action": {"type": "string"},
                    "top_rollback_level": {"type": "string"},
                    "top_rollback_target": {"type": "string"},
                    "top_historical_rollback_level": {"type": "string"},
                    "top_historical_rollback_alignment": {"type": "boolean"},
                    "top_historical_rollback_target": {"type": "string"},
                    "top_historical_rollback_target_alignment": {"type": "boolean"},
                    "primary_lane": {"type": "string"},
                    "lane_reason": {"type": "string"},
                    "next_best_operator_move": {"type": "string"},
                    "next_best_operator_reason": {"type": "string"},
                    "latest_auto_recovery_issue": {"type": "string"},
                    "latest_auto_recovery_action": {"type": "string"},
                    "latest_auto_recovery_status": {"type": "string"},
                    "latest_auto_recovery_pending_types": {"type": "string"},
                    "latest_auto_recovery_job_id": {"type": "string"},
                },
            },
            "project_playbook": {
                "type": "object",
                "properties": {
                    "goal": {"type": "string"},
                    "next_move": {"type": "string"},
                    "step_codes": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
            },
        },
        "required": ["overview", "supervisor_message", "recommendations"],
    }
    prompt = (
        "You are the Project Manager agent for a bioinformatics execution system. "
        "Review the open incidents and produce a concise operational diagnosis. "
        "You must not invent hidden system state. Base every recommendation on the given incidents and dossiers only.\n\n"
        f"Incident summary:\n{summary}\n\n"
        f"Open incidents:\n{incidents}\n\n"
        f"Structured dossiers:\n{dossiers or []}"
    )
    try:
        result = await get_gateway().structured_output(
            [LLMMessage("user", prompt)],
            schema=schema,
            system=(
                "Return an operator-facing supervisor review for this project. "
                "Prioritize blocked execution, identify the safest rollback target, "
                "and keep the language concrete and technical."
            ),
        )
        fallback = _build_supervisor_review_fallback(incidents, summary, dossiers)
        fallback_by_job = {
            item["job_id"]: item for item in fallback["recommendations"]
        }
        recommendations: list[dict] = []
        seen_job_ids: set[str] = set()
        for item in list(result.get("recommendations") or []):
            job_id = item.get("job_id")
            baseline = fallback_by_job.get(job_id or "", {})
            merged = dict(item)
            if baseline:
                merged["thread_id"] = baseline.get("thread_id", "")
                merged["failure_layer"] = baseline.get("failure_layer", "step_execution")
                merged["rollback_level"] = baseline.get("rollback_level", "step")
                merged["rollback_target"] = baseline.get("rollback_target", "job_detail")
                merged["reconfirmation_required"] = baseline.get("reconfirmation_required", False)
                merged["historical_matches"] = baseline.get("historical_matches", 0)
                merged["safe_action"] = baseline.get("safe_action")
                merged["safe_action_eligibility"] = baseline.get("safe_action_eligibility")
                merged["historical_policy"] = baseline.get("historical_policy")
                merged["auto_recoverable"] = baseline.get("auto_recoverable", False)
                merged["auto_recovery_kind"] = baseline.get("auto_recovery_kind")
                merged["recommended_action_confidence"] = baseline.get("recommended_action_confidence", "low")
                merged["recommended_action_basis"] = baseline.get("recommended_action_basis", [])
                merged["safe_action_note"] = baseline.get("safe_action_note")
                merged["historical_guidance"] = baseline.get("historical_guidance")
                merged["dossier_summary"] = baseline.get("dossier_summary", "No additional dossier signals.")
                merged["recovery_playbook"] = baseline.get("recovery_playbook")
            else:
                merged.setdefault("thread_id", "")
                merged.setdefault("failure_layer", "step_execution")
                merged.setdefault("rollback_level", "step")
                merged.setdefault("rollback_target", "job_detail")
                merged.setdefault("reconfirmation_required", False)
                merged.setdefault("historical_matches", 0)
                merged.setdefault("safe_action", None)
                merged.setdefault("safe_action_eligibility", None)
                merged.setdefault("historical_policy", None)
                merged.setdefault("auto_recoverable", False)
                merged.setdefault("auto_recovery_kind", None)
                merged.setdefault("recommended_action_confidence", "low")
                merged.setdefault("recommended_action_basis", [])
                merged.setdefault("safe_action_note", None)
                merged.setdefault("historical_guidance", None)
                merged.setdefault("dossier_summary", "No additional dossier signals.")
                merged.setdefault("recovery_playbook", None)
            recommendations.append(merged)
            if job_id:
                seen_job_ids.add(str(job_id))
        for baseline in fallback["recommendations"]:
            job_id = str(baseline.get("job_id") or "")
            if not job_id or job_id in seen_job_ids:
                continue
            recommendations.append(dict(baseline))
        recommendations = _finalize_supervisor_recommendations(recommendations, incidents)
        focus_summary = fallback.get("focus_summary") or _build_supervisor_focus_summary(recommendations, dossiers)
        return {
            "mode": "llm",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "overview": result.get("overview", ""),
            "supervisor_message": result.get("supervisor_message", ""),
            "focus_summary": focus_summary,
            "project_playbook": fallback.get("project_playbook") or _build_project_playbook(focus_summary, recommendations),
            "recommendations": recommendations[:8],
        }
    except GatewayNotConfiguredError:
        return _build_supervisor_review_fallback(incidents, summary, dossiers)
    except Exception:
        return _build_supervisor_review_fallback(incidents, summary, dossiers)


@router.post("/")
async def submit_job(body: JobCreate, session: AsyncSession = Depends(get_session)):
    from tune.core.config import get_config
    from tune.core.models import Project, Thread

    project_name = "default"
    if body.project_id:
        project = (
            await session.execute(select(Project).where(Project.id == body.project_id))
        ).scalar_one_or_none()
        if project and getattr(project, "name", None):
            project_name = project.name

    if body.thread_id:
        thread = (
            await session.execute(select(Thread).where(Thread.id == body.thread_id))
        ).scalar_one_or_none()
        if not thread:
            raise HTTPException(404, f"Thread '{body.thread_id}' not found")

    created_at = datetime.now(tz=timezone.utc)
    job = AnalysisJob(
        id=str(uuid.uuid4()),
        thread_id=body.thread_id,
        project_id=body.project_id,
        name=body.name,
        goal=body.goal,
        plan=body.plan,
        status="queued",
        created_at=created_at,
        env_status="pending",
        output_dir=str(
            build_output_dir_path(
                get_config().analysis_dir,
                project_name,
                body.name,
                created_at=created_at,
            )
        ),
    )
    session.add(job)
    await session.commit()

    from tune.workers.defer import defer_async_with_fallback
    from tune.workers.tasks import run_analysis_task
    await defer_async_with_fallback(run_analysis_task, job_id=job.id)
    return {"id": job.id, "status": "queued"}


@router.get("/")
async def list_jobs(
    response: Response,
    status: str | None = None,
    project: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
):
    effective_only_statuses = {
        "awaiting_plan_confirmation",
        "waiting_for_authorization",
        "waiting_for_repair",
    }
    q = select(AnalysisJob)
    count_q = select(func.count()).select_from(AnalysisJob)
    if project:
        q = q.where(AnalysisJob.project_id == project)
        count_q = count_q.where(AnalysisJob.project_id == project)
    if status and status not in effective_only_statuses:
        q = q.where(AnalysisJob.status == status)
        count_q = count_q.where(AnalysisJob.status == status)
        total = int((await session.execute(count_q)).scalar_one() or 0)
        q = q.order_by(AnalysisJob.created_at.desc()).offset(offset).limit(limit)
        jobs = (await session.execute(q)).scalars().all()
    else:
        q = q.order_by(AnalysisJob.created_at.desc())
        all_jobs = (await session.execute(q)).scalars().all()
        filtered_jobs: list[AnalysisJob] = []
        for job in all_jobs:
            effective = await _get_effective_job_state(session, job)
            if status and effective["status"] != status:
                continue
            filtered_jobs.append(job)
        total = len(filtered_jobs)
        jobs = filtered_jobs[offset: offset + limit]

    response.headers["X-Total-Count"] = str(total)
    response.headers["X-Has-More"] = "1" if offset + len(jobs) < total else "0"
    payload = []
    for j in jobs:
        effective = await _get_effective_job_state(session, j)
        payload.append(
            {
                "id": j.id,
                "name": j.name,
                "status": effective["status"],
                "goal": j.goal,
                "thread_id": j.thread_id,
                "project_id": j.project_id,
                "created_at": j.created_at,
                "started_at": j.started_at,
                "ended_at": j.ended_at,
                "error_message": effective["error_message"],
                "pending_interaction_type": effective["pending_interaction_type"],
                "peak_cpu_pct": j.peak_cpu_pct,
                "peak_mem_mb": j.peak_mem_mb,
                "has_execution_plan": bool(getattr(j, "expanded_dag_json", None)),
            }
        )
    return payload


@router.get("/incidents")
async def list_job_incidents(
    project: str | None = None,
    session: AsyncSession = Depends(get_session),
):
    incidents, summary, _dossiers = await _collect_job_supervisor_data(session, project=project)
    return {
        "summary": summary,
        "incidents": incidents,
    }


@router.get("/overview")
async def get_job_overview(
    project: str | None = None,
    session: AsyncSession = Depends(get_session),
):
    return await _collect_job_status_summary(session, project=project)


@router.get("/attention-summary")
async def get_job_attention_summary(
    project: str | None = None,
    session: AsyncSession = Depends(get_session),
):
    return await _collect_job_attention_summary(session, project=project)


@router.get("/supervisor-dossier")
async def get_supervisor_dossier(
    project: str | None = None,
    session: AsyncSession = Depends(get_session),
):
    incidents, summary, dossiers = await _collect_job_supervisor_data(session, project=project)
    return {
        "summary": summary,
        "incident_count": len(incidents),
        "dossiers": dossiers,
    }


@router.get("/supervisor-review")
async def get_supervisor_review(
    project: str | None = None,
    session: AsyncSession = Depends(get_session),
):
    incidents, summary, dossiers = await _collect_job_supervisor_data(session, project=project)
    review = await _build_supervisor_review_with_llm(incidents, summary, dossiers)
    review["incident_summary"] = summary
    review["dossiers"] = dossiers
    return review


# NOTE: /result must be registered BEFORE /{job_id} so FastAPI doesn't match
# "result" as a job_id value.
@router.get("/result")
async def get_result_file(path: str):
    """Serve an analysis result file. Path must be within the configured analysis directory."""
    from tune.core.config import get_config

    cfg = get_config()
    result_path = Path(path).resolve()
    analysis_dir = cfg.analysis_dir.resolve()

    try:
        result_path.relative_to(analysis_dir)
    except ValueError:
        raise HTTPException(403, "Path is outside the analysis directory")

    if not result_path.exists() or not result_path.is_file():
        raise HTTPException(404, "File not found")

    content_type, _ = mimetypes.guess_type(str(result_path))
    return FileResponse(str(result_path), media_type=content_type or "application/octet-stream")


@router.delete("/{job_id}/purge")
async def purge_job(job_id: str, session: AsyncSession = Depends(get_session)):
    """Permanently delete a terminal-state job: DB records + output directory."""
    j = (await session.execute(select(AnalysisJob).where(AnalysisJob.id == job_id))).scalar_one_or_none()
    if not j:
        raise HTTPException(404, "Job not found")
    if j.status not in ("completed", "failed", "cancelled", "interrupted"):
        raise HTTPException(409, "Cannot purge a job that is not in a terminal state")

    # Capture metadata before deleting the record
    output_dirs = await _collect_job_output_cleanup_targets(session, j)
    project_id = j.project_id
    thread_id = j.thread_id
    job_name = j.name

    # Delete DB records first (transactional, rollback-safe)
    from tune.core.models import JobLog
    from sqlalchemy import delete as _delete
    await session.execute(_delete(JobLog).where(JobLog.job_id == job_id))
    await session.delete(j)
    await session.commit()

    from tune.api.ws import broadcast_project_task_event
    await broadcast_project_task_event(
        job_id,
        reason="deleted",
        deleted=True,
        project_id=project_id,
        thread_id=thread_id,
        job_name=job_name,
    )

    # Delete filesystem output directory after DB commit
    from tune.core.config import get_config

    deleted_output_dirs = _delete_job_output_dirs(output_dirs, get_config().analysis_dir)
    return {
        "ok": True,
        "files_deleted": bool(deleted_output_dirs),
        "deleted_output_dirs": deleted_output_dirs,
    }


@router.get("/{job_id}")
async def get_job(job_id: str, session: AsyncSession = Depends(get_session)):
    j = (await session.execute(select(AnalysisJob).where(AnalysisJob.id == job_id))).scalar_one_or_none()
    if not j:
        raise HTTPException(404, "Job not found")
    effective = await _get_effective_job_state(session, j)
    current_step = await _fetch_job_current_step(session, j)
    rollback_guidance = await _build_job_rollback_guidance(
        session,
        j,
        effective,
        current_step=current_step,
    )
    execution_plan = _serialize_execution_plan(j)
    recent_logs = await _fetch_recent_job_logs(session, job_id)
    recent_decisions = await _fetch_recent_user_decisions(session, job_id)
    auth_requests = await _fetch_recent_auth_requests(session, job_id)
    repair_requests = await _fetch_recent_repair_requests(session, job_id)
    step_runs = await _fetch_recent_step_runs(session, job_id)
    artifacts = await _fetch_recent_artifacts(session, job_id)
    return {
        "id": j.id, "name": j.name, "status": effective["status"],
        "goal": j.goal, "plan": j.plan, "output_dir": j.output_dir,
        "project_id": j.project_id, "created_at": j.created_at,
        "started_at": j.started_at, "ended_at": j.ended_at,
        "peak_cpu_pct": j.peak_cpu_pct, "peak_mem_mb": j.peak_mem_mb,
        "error_message": effective["error_message"],
        "pending_interaction_type": effective["pending_interaction_type"],
        "pending_interaction_payload": effective["pending_interaction_payload"],
        "resolved_plan": execution_plan["abstract_plan"],
        "execution_ir": execution_plan["execution_ir"],
        "expanded_dag": execution_plan["expanded_dag"],
        "execution_plan_summary": execution_plan["summary"],
        "rollback_guidance": rollback_guidance,
        "auto_recovery_events": _extract_auto_recovery_events(recent_logs),
        "timeline": _build_job_timeline(
            j,
            recent_logs=recent_logs,
            recent_decisions=recent_decisions,
            auth_requests=auth_requests,
            repair_requests=repair_requests,
            step_runs=step_runs,
            artifacts=artifacts,
            runtime_diagnostics=effective.get("runtime_diagnostics") or [],
            rollback_guidance=rollback_guidance,
        ),
    }


@router.get("/{job_id}/execution-plan")
async def get_execution_plan(job_id: str, session: AsyncSession = Depends(get_session)):
    j = (await session.execute(select(AnalysisJob).where(AnalysisJob.id == job_id))).scalar_one_or_none()
    if not j:
        raise HTTPException(404, "Job not found")
    return _serialize_execution_plan(j)


@router.delete("/{job_id}")
async def cancel_job(job_id: str, session: AsyncSession = Depends(get_session)):
    j = (await session.execute(select(AnalysisJob).where(AnalysisJob.id == job_id))).scalar_one_or_none()
    if not j:
        raise HTTPException(404, "Job not found")
    effective = await _get_effective_job_state(session, j)
    effective_status = effective["status"]
    if effective_status not in (
        "queued",
        "running",
        "waiting_for_authorization",
        "waiting_for_repair",
        "resource_clarification_required",
    ):
        raise HTTPException(400, f"Cannot cancel job with status '{j.status}'")

    from tune.core.analysis.engine import cancel_job as _cancel
    await _cancel(job_id)

    j.status = "cancelled"
    j.ended_at = datetime.now(tz=timezone.utc)
    j.pending_auth_request_id = None
    j.pending_repair_request_id = None
    j.pending_step_key = None
    j.pending_interaction_type = None
    j.pending_interaction_payload_json = None
    await session.commit()
    from tune.api.ws import broadcast_project_task_event
    await broadcast_project_task_event(job_id, reason="cancelled")
    return {"ok": True}


@router.post("/{job_id}/resume")
async def resume_job(job_id: str, session: AsyncSession = Depends(get_session)):
    """Re-queue a stuck or paused job and re-defer its analysis task."""
    j = (await session.execute(select(AnalysisJob).where(AnalysisJob.id == job_id))).scalar_one_or_none()
    if not j:
        raise HTTPException(404, "Job not found")
    effective = await _get_effective_job_state(session, j)
    effective_status = effective["status"]
    resumable = {
        "queued",
        "binding_required",
        "resource_clarification_required",
        "interrupted",
        "waiting_for_authorization",
        "waiting_for_repair",
        "failed",
    }
    if effective_status not in resumable:
        raise HTTPException(400, f"Cannot resume job with status '{effective_status}'")

    j.status = "queued"
    j.error_message = None
    j.started_at = None
    j.ended_at = None
    j.pending_auth_request_id = None
    j.pending_repair_request_id = None
    j.pending_step_key = None
    j.pending_interaction_type = None
    j.pending_interaction_payload_json = None
    await session.commit()

    from tune.workers.defer import defer_async_with_fallback
    from tune.workers.tasks import run_analysis_task
    from tune.api.ws import broadcast_project_task_event

    await defer_async_with_fallback(run_analysis_task, job_id=job_id)
    await broadcast_project_task_event(job_id, reason="resumed")
    return {"ok": True, "status": "queued"}


@router.post("/{job_id}/supervisor-actions/step-reenter")
async def supervisor_step_reenter(job_id: str, session: AsyncSession = Depends(get_session)):
    """Queue a low-risk supervisor-directed retry from the current/failed step."""
    from tune.core.models import AnalysisStepRun, UserDecision
    from tune.workers.defer import defer_async_with_fallback
    from tune.workers.tasks import run_analysis_task

    j = (await session.execute(select(AnalysisJob).where(AnalysisJob.id == job_id))).scalar_one_or_none()
    if not j:
        raise HTTPException(404, "Job not found")

    effective = await _get_effective_job_state(session, j)
    current_step = None
    if getattr(j, "current_step_id", None):
        current_step = (
            await session.execute(
                select(AnalysisStepRun).where(AnalysisStepRun.id == j.current_step_id)
            )
        ).scalar_one_or_none()

    incident = _derive_job_incident(j, effective, current_step=current_step)
    if not incident:
        raise HTTPException(409, "No open supervisor incident is available for this job")

    rollback_target, rollback_level, _diagnosis, safe_action = _resolve_supervisor_incident_controls(
        incident,
        dossier=_build_supervisor_rollback_context(
            j,
            effective,
            incident,
            current_step=current_step,
        ),
    )
    if rollback_level != "step" or incident["owner"] != "system" or safe_action != "step_reenter":
        raise HTTPException(409, "This incident is not eligible for safe step-level supervisor re-entry")

    anchor_step_key, anchor_step_id = await _resolve_supervisor_step_anchor(
        session,
        j,
        incident,
        current_step=current_step,
    )
    if not anchor_step_key:
        raise HTTPException(409, "Unable to determine a step-level rollback target")

    impacted_keys = _collect_impacted_step_keys(getattr(j, "expanded_dag_json", None), anchor_step_key)
    if not impacted_keys:
        impacted_keys = {anchor_step_key}

    step_runs = (
        await session.execute(
            select(AnalysisStepRun).where(AnalysisStepRun.job_id == job_id)
        )
    ).scalars().all()
    rewound_steps = 0
    for step_run in step_runs:
        if step_run.step_key not in impacted_keys:
            continue
        step_run.status = "pending"
        step_run.started_at = None
        step_run.finished_at = None
        step_run.bindings_json = None
        step_run.outputs_json = None
        rewound_steps += 1

    j.status = "queued"
    j.error_message = None
    j.started_at = None
    j.ended_at = None
    j.pending_auth_request_id = None
    j.pending_repair_request_id = None
    j.pending_step_key = anchor_step_key
    j.pending_interaction_type = None
    j.pending_interaction_payload_json = {
        "resume_anchor": {
            "step_key": anchor_step_key,
            "step_id": anchor_step_id,
            "mode": "step_reenter",
            "requested_by": "supervisor",
            "reason": incident["incident_type"],
        }
    }
    j.current_step_id = anchor_step_id

    session.add(
        UserDecision(
            id=str(uuid.uuid4()),
            job_id=job_id,
            step_id=anchor_step_id,
            decision_type="supervisor_step_reenter",
            payload_json={
                "incident_type": incident["incident_type"],
                "rollback_level": rollback_level,
                "rollback_target": anchor_step_key,
                "rewound_step_count": rewound_steps,
            },
        )
    )
    await session.commit()

    await defer_async_with_fallback(run_analysis_task, job_id=job_id)
    await _record_supervisor_resolution_event(
        session,
        j,
        incident,
        safe_action="step_reenter",
        rollback_level=rollback_level,
        rollback_target=anchor_step_key,
        outcome_status="queued",
        detail=f"rewound_step_count={rewound_steps}",
    )
    from tune.api.ws import sync_supervisor_thread_state

    thread_message = (
        f"监督器已从步骤 `{anchor_step_key}` 重新进入执行，任务重新排队。"
        if getattr(j, "language", "en") == "zh"
        else f"Supervisor re-entered execution from step `{anchor_step_key}` and re-queued the job."
    )
    await sync_supervisor_thread_state(
        job_id,
        clear_pending_command_auth=True,
        clear_pending_error_recovery=True,
        clear_resource_clarification=True,
        clear_pending_analysis_plan=True,
        message=thread_message,
        emit_job_started=True,
        job_name=j.name,
    )
    return {
        "ok": True,
        "status": "queued",
        "rollback_level": rollback_level,
        "rollback_target": anchor_step_key,
        "rewound_step_count": rewound_steps,
    }


@router.post("/{job_id}/supervisor-actions/execute")
async def execute_supervisor_safe_action(
    job_id: str,
    request: SupervisorSafeActionRequest,
    session: AsyncSession = Depends(get_session),
):
    """Dispatch a declared safe_action to the matching supervisor recovery route."""
    action = (request.safe_action or "").strip()
    if action == "step_reenter":
        return await supervisor_step_reenter(job_id, session=session)
    if action == "refresh_execution_graph":
        return await supervisor_refresh_execution_graph(job_id, session=session)
    if action == "refresh_execution_plan":
        return await supervisor_refresh_execution_plan(job_id, session=session)
    if action == "revalidate_abstract_plan":
        return await supervisor_revalidate_abstract_plan(job_id, session=session)
    if action == "retry_resume_chain":
        return await supervisor_retry_resume_chain(job_id, session=session)
    if action == "normalize_orphan_pending_state":
        return await supervisor_normalize_orphan_pending_state(job_id, session=session)
    if action == "normalize_terminal_state":
        return await supervisor_normalize_terminal_state(job_id, session=session)
    raise HTTPException(400, f"Unknown supervisor safe action: {action}")


@router.post("/{job_id}/supervisor-actions/refresh-execution-graph")
async def supervisor_refresh_execution_graph(
    job_id: str,
    session: AsyncSession = Depends(get_session),
):
    """Re-materialize the execution DAG from the confirmed abstract plan."""
    return await _supervisor_refresh_execution_plan(
        job_id,
        session,
        expected_safe_action="refresh_execution_graph",
        decision_type="supervisor_refresh_execution_graph",
        success_message="Execution graph refreshed and waiting for final confirmation.",
        rollback_level="dag",
        rollback_target="execution_confirmation_gate",
    )


@router.post("/{job_id}/supervisor-actions/refresh-execution-plan")
async def supervisor_refresh_execution_plan(
    job_id: str,
    session: AsyncSession = Depends(get_session),
):
    """Re-materialize execution IR and DAG after resource clarification changes."""
    return await _supervisor_refresh_execution_plan(
        job_id,
        session,
        expected_safe_action="refresh_execution_plan",
        decision_type="supervisor_refresh_execution_plan",
        success_message="Execution plan refreshed after resource clarification and waiting for final confirmation.",
        rollback_level="execution_ir",
        rollback_target="resource_clarification_gate",
    )


async def _supervisor_refresh_execution_plan(
    job_id: str,
    session: AsyncSession,
    *,
    expected_safe_action: str,
    decision_type: str,
    success_message: str,
    rollback_level: str,
    rollback_target: str,
):
    """Deterministically rebuild execution objects from the current abstract plan."""
    from tune.core.models import UserDecision
    from tune.core.orchestration import materialize_job_execution_plan, summarize_expanded_dag_for_confirmation
    from tune.core.workflow import transition_job
    from tune.api.ws import sync_supervisor_thread_state

    j = (await session.execute(select(AnalysisJob).where(AnalysisJob.id == job_id))).scalar_one_or_none()
    if not j:
        raise HTTPException(404, "Job not found")

    effective = await _get_effective_job_state(session, j)
    current_step = None
    incident = _derive_job_incident(j, effective, current_step=current_step)
    if not incident:
        raise HTTPException(409, "No open supervisor incident is available for this job")

    _resolved_target, inferred_rollback_level, _diagnosis, safe_action = _resolve_supervisor_incident_controls(
        incident,
        dossier=_build_supervisor_rollback_context(j, effective, incident),
    )
    if safe_action != expected_safe_action:
        raise HTTPException(409, "This incident is not eligible for the requested supervisor execution refresh")

    draft_payload = j.plan_draft_json or j.resolved_plan_json or j.plan
    if draft_payload is None:
        raise HTTPException(409, "No abstract plan is available to rebuild the execution plan")

    bundle = await materialize_job_execution_plan(session, j, draft_payload)
    if j.status == "draft":
        ok = await transition_job(job_id, "awaiting_plan_confirmation", session)
        if not ok:
            raise HTTPException(409, f"Cannot move job {job_id} to awaiting_plan_confirmation")
    j.error_message = success_message
    j.pending_interaction_type = "execution_confirmation"
    j.pending_interaction_payload_json = {
        "phase": "execution",
        "prompt_text": "Execution graph is ready for final confirmation.",
        "execution_plan_summary": _serialize_execution_plan(j)["summary"],
    }
    j.current_step_id = None
    j.pending_auth_request_id = None
    j.pending_repair_request_id = None
    j.pending_step_key = None

    session.add(
        UserDecision(
            id=str(uuid.uuid4()),
            job_id=job_id,
            decision_type=decision_type,
            payload_json={
                "incident_type": incident["incident_type"],
                "rollback_level": rollback_level,
                "group_count": len((bundle.expanded_dag or {}).get("groups", [])),
                "node_count": len((bundle.expanded_dag or {}).get("nodes", [])),
            },
        )
    )
    await session.commit()

    await _record_supervisor_resolution_event(
        session,
        j,
        incident,
        safe_action=expected_safe_action,
        rollback_level=rollback_level,
        rollback_target=rollback_target,
        outcome_status="awaiting_plan_confirmation",
        detail=(
            f"group_count={len((bundle.expanded_dag or {}).get('groups', []))}; "
            f"node_count={len((bundle.expanded_dag or {}).get('nodes', []))}"
        ),
    )

    review_plan = summarize_expanded_dag_for_confirmation(j.expanded_dag_json)
    execution_summary = _serialize_execution_plan(j)["summary"]
    thread_message = (
        "监督器已刷新执行计划，请再次确认最终执行图。"
        if getattr(j, "language", "en") == "zh"
        else "Supervisor refreshed the execution plan. Confirm the final execution graph again."
    )
    await sync_supervisor_thread_state(
        job_id,
        clear_pending_command_auth=True,
        clear_pending_error_recovery=True,
        clear_resource_clarification=True,
        pending_analysis_plan={
            "active": True,
            "job_backed": True,
            "job_id": job_id,
            "goal": getattr(j, "goal", None) or j.name,
            "project_id": j.project_id,
            "short_name": j.name,
            "plan": extract_plan_steps(draft_payload),
            "phase": "execution",
            "review_plan": review_plan,
            "execution_plan_summary": execution_summary,
        },
        message=thread_message,
    )

    return {
        "ok": True,
        "status": "awaiting_plan_confirmation",
        "rollback_level": rollback_level,
        "rollback_target": rollback_target,
        "execution_plan_summary": execution_summary,
        "execution_review_plan": review_plan,
    }


@router.post("/{job_id}/supervisor-actions/revalidate-abstract-plan")
async def supervisor_revalidate_abstract_plan(
    job_id: str,
    session: AsyncSession = Depends(get_session),
):
    """Re-run deterministic compilation and planner constraints on the current draft plan."""
    from tune.core.context.builder import PlannerContextBuilder
    from tune.core.context.models import ContextScope
    from tune.core.models import UserDecision
    from tune.core.orchestration import replace_plan_steps
    from tune.core.resources.planner_adapter import enforce_planner_constraints
    from tune.core.registry.spec_generation import augment_plan_with_dynamic_specs
    from tune.core.workflow.plan_compiler import compile_plan
    from tune.api.ws import sync_supervisor_thread_state

    j = (await session.execute(select(AnalysisJob).where(AnalysisJob.id == job_id))).scalar_one_or_none()
    if not j:
        raise HTTPException(404, "Job not found")

    effective = await _get_effective_job_state(session, j)
    incident = _derive_job_incident(j, effective, current_step=None)
    if not incident:
        raise HTTPException(409, "No open supervisor incident is available for this job")

    _resolved_target, rollback_level, _diagnosis, safe_action = _resolve_supervisor_incident_controls(
        incident,
        dossier=_build_supervisor_rollback_context(j, effective, incident),
    )
    if safe_action != "revalidate_abstract_plan":
        raise HTTPException(409, "This incident is not eligible for safe abstract-plan revalidation")

    draft_payload = j.plan_draft_json or j.resolved_plan_json or j.plan
    current_steps = extract_plan_steps(draft_payload)
    current_steps, dynamic_issues = await augment_plan_with_dynamic_specs(
        current_steps,
        context_hint=(
            f"Goal: {getattr(j, 'goal', '') or ''}\n"
            f"Project ID: {getattr(j, 'project_id', '') or ''}"
        ),
    )
    if dynamic_issues:
        return {
            "ok": False,
            "status": j.status,
            "rollback_level": rollback_level,
            "issues": dynamic_issues,
        }
    compile_result = compile_plan(current_steps)
    if not compile_result.ok:
        return {
            "ok": False,
            "status": j.status,
            "rollback_level": rollback_level,
            "issues": compile_result.errors,
        }

    normalized_payload = replace_plan_steps(draft_payload, compile_result.compiled_steps)
    planner_context = await PlannerContextBuilder(session).build(ContextScope(project_id=j.project_id))
    feasibility = enforce_planner_constraints(compile_result.compiled_steps, planner_context)
    final_steps = feasibility.amended_plan if feasibility.amended_plan != compile_result.compiled_steps else compile_result.compiled_steps
    final_payload = replace_plan_steps(normalized_payload, final_steps)

    j.plan_draft_json = final_payload
    j.resolved_plan_json = None
    j.execution_ir_json = None
    j.expanded_dag_json = None
    j.error_message = "Abstract analysis plan revalidated and waiting for confirmation."
    j.pending_interaction_type = "plan_confirmation"
    j.pending_interaction_payload_json = {
        "phase": "abstract",
        "prompt_text": "Abstract analysis plan is waiting for confirmation.",
    }
    j.current_step_id = None
    j.pending_auth_request_id = None
    j.pending_repair_request_id = None
    j.pending_step_key = None

    session.add(
        UserDecision(
            id=str(uuid.uuid4()),
            job_id=job_id,
            decision_type="supervisor_revalidate_abstract_plan",
            payload_json={
                "incident_type": incident["incident_type"],
                "rollback_level": rollback_level,
                "step_count": len(final_steps),
                "warnings": list(feasibility.warnings or []),
                "issue_count": len(feasibility.issues or []),
            },
        )
    )
    await session.commit()

    await _record_supervisor_resolution_event(
        session,
        j,
        incident,
        safe_action="revalidate_abstract_plan",
        rollback_level=rollback_level,
        rollback_target="abstract_plan_gate",
        outcome_status="awaiting_plan_confirmation",
        detail=(
            f"step_count={len(final_steps)}; "
            f"warning_count={len(feasibility.warnings or [])}; "
            f"issue_count={len(feasibility.issues or [])}"
        ),
    )

    thread_message = (
        "监督器已重新校验抽象分析方案，请再次确认。"
        if getattr(j, "language", "en") == "zh"
        else "Supervisor revalidated the abstract analysis plan. Please confirm it again."
    )
    await sync_supervisor_thread_state(
        job_id,
        clear_pending_command_auth=True,
        clear_pending_error_recovery=True,
        clear_resource_clarification=True,
        pending_analysis_plan={
            "active": True,
            "job_backed": True,
            "job_id": job_id,
            "goal": getattr(j, "goal", None) or j.name,
            "project_id": j.project_id,
            "short_name": j.name,
            "plan": final_steps,
            "phase": "abstract",
        },
        message=thread_message,
    )

    return {
        "ok": True,
        "status": "awaiting_plan_confirmation",
        "rollback_level": rollback_level,
        "rollback_target": "abstract_plan_gate",
        "steps": final_steps,
        "warnings": list(feasibility.warnings or []),
        "issues": [
            {
                "kind": issue.kind,
                "title": issue.title,
                "description": issue.description,
                "suggestion": issue.suggestion,
            }
            for issue in (feasibility.issues or [])
        ],
    }


@router.post("/{job_id}/supervisor-actions/retry-resume-chain")
async def supervisor_retry_resume_chain(
    job_id: str,
    session: AsyncSession = Depends(get_session),
):
    """Retry a resolved pending-decision resume chain without dropping decision state."""
    from tune.api.ws import sync_supervisor_thread_state
    from tune.core.models import UserDecision
    from tune.workers.defer import defer_async_with_fallback
    from tune.workers.tasks import resume_job_task

    j = (await session.execute(select(AnalysisJob).where(AnalysisJob.id == job_id))).scalar_one_or_none()
    if not j:
        raise HTTPException(404, "Job not found")

    effective = await _get_effective_job_state(session, j)
    incident = _derive_job_incident(j, effective, current_step=None)
    if not incident:
        raise HTTPException(409, "No open supervisor incident is available for this job")

    rollback_target, rollback_level, _diagnosis, safe_action = _resolve_supervisor_incident_controls(
        incident,
        dossier=_build_supervisor_rollback_context(j, effective, incident),
    )
    if safe_action != "retry_resume_chain":
        raise HTTPException(409, "This incident is not eligible for safe resume-chain retry")

    effective_status = effective.get("status") or j.status
    if effective_status not in {"waiting_for_authorization", "waiting_for_repair", "interrupted"}:
        raise HTTPException(409, "Resume-chain retry requires a paused or interrupted job state")
    if not (j.pending_auth_request_id or j.pending_repair_request_id):
        raise HTTPException(409, "No resolved pending-decision metadata is available to retry")

    if rollback_target in {"job_detail", "resume_chain"}:
        rollback_target = (
            incident.get("current_step_key")
            or getattr(j, "pending_step_key", None)
            or "resume_chain"
        )
    pending_types = sorted(
        filter(
            None,
            [
                "authorization" if j.pending_auth_request_id else "",
                "repair" if j.pending_repair_request_id else "",
            ],
        )
    )
    j.error_message = "Retrying resolved pending-decision resume chain."

    session.add(
        UserDecision(
            id=str(uuid.uuid4()),
            job_id=job_id,
            step_id=j.current_step_id,
            decision_type="supervisor_retry_resume_chain",
            payload_json={
                "incident_type": incident["incident_type"],
                "rollback_level": rollback_level,
                "rollback_target": rollback_target,
                "pending_types": pending_types,
                "effective_status": effective_status,
            },
        )
    )
    await session.commit()

    await defer_async_with_fallback(resume_job_task, job_id=job_id)
    await _record_supervisor_resolution_event(
        session,
        j,
        incident,
        safe_action="retry_resume_chain",
        rollback_level=rollback_level,
        rollback_target=rollback_target,
        outcome_status=effective_status,
        detail=f"pending_types={','.join(pending_types) or 'none'}",
    )

    thread_message = (
        "监督器已重试已解析人工决策的恢复链，任务将继续尝试恢复。"
        if getattr(j, "language", "en") == "zh"
        else "Supervisor retried the resolved human-decision resume chain. The job will attempt to continue."
    )
    await sync_supervisor_thread_state(
        job_id,
        clear_pending_command_auth=bool(j.pending_auth_request_id),
        clear_pending_error_recovery=bool(j.pending_repair_request_id),
        clear_resource_clarification=False,
        clear_pending_analysis_plan=False,
        message=thread_message,
        emit_job_started=True,
        job_name=j.name,
    )

    return {
        "ok": True,
        "status": effective_status,
        "rollback_level": rollback_level,
        "rollback_target": rollback_target,
        "retry_scheduled": True,
    }


@router.post("/{job_id}/supervisor-actions/normalize-terminal-state")
async def supervisor_normalize_terminal_state(
    job_id: str,
    session: AsyncSession = Depends(get_session),
):
    """Clear stale pending request metadata from a terminal job."""
    from tune.api.ws import sync_supervisor_thread_state
    from tune.core.models import UserDecision

    j = (await session.execute(select(AnalysisJob).where(AnalysisJob.id == job_id))).scalar_one_or_none()
    if not j:
        raise HTTPException(404, "Job not found")

    effective = await _get_effective_job_state(session, j)
    incident = _derive_job_incident(j, effective, current_step=None)
    if not incident:
        raise HTTPException(409, "No open supervisor incident is available for this job")

    rollback_target, rollback_level, _diagnosis, safe_action = _resolve_supervisor_incident_controls(
        incident,
        dossier=_build_supervisor_rollback_context(j, effective, incident),
    )
    if safe_action != "normalize_terminal_state":
        raise HTTPException(409, "This incident is not eligible for safe terminal-state normalization")

    final_status = effective.get("status") or j.status
    if final_status not in {"completed", "failed", "cancelled"}:
        raise HTTPException(409, "Only terminal jobs can be normalized with this action")

    j.status = final_status
    _clear_pending_request_metadata(j)
    j.pending_interaction_type = None
    j.pending_interaction_payload_json = None

    session.add(
        UserDecision(
            id=str(uuid.uuid4()),
            job_id=job_id,
            decision_type="supervisor_normalize_terminal_state",
            payload_json={
                "incident_type": incident["incident_type"],
                "rollback_level": rollback_level,
                "final_status": final_status,
            },
        )
    )
    await session.commit()

    await _record_supervisor_resolution_event(
        session,
        j,
        incident,
        safe_action="normalize_terminal_state",
        rollback_level=rollback_level,
        rollback_target=rollback_target,
        outcome_status=final_status,
    )

    thread_message = (
        "监督器已清理终态任务上的陈旧 pending 状态。"
        if getattr(j, "language", "en") == "zh"
        else "Supervisor cleared stale pending state from the terminal job."
    )
    await sync_supervisor_thread_state(
        job_id,
        clear_pending_command_auth=True,
        clear_pending_error_recovery=True,
        clear_resource_clarification=True,
        clear_pending_analysis_plan=True,
        message=thread_message,
    )

    return {
        "ok": True,
        "status": final_status,
        "rollback_level": rollback_level,
        "rollback_target": rollback_target,
    }


@router.post("/{job_id}/supervisor-actions/normalize-orphan-pending-state")
async def supervisor_normalize_orphan_pending_state(
    job_id: str,
    session: AsyncSession = Depends(get_session),
):
    """Clear dangling pending request metadata and move blocked waiting jobs to a resumable state."""
    from tune.api.ws import sync_supervisor_thread_state
    from tune.core.models import UserDecision

    j = (await session.execute(select(AnalysisJob).where(AnalysisJob.id == job_id))).scalar_one_or_none()
    if not j:
        raise HTTPException(404, "Job not found")

    effective = await _get_effective_job_state(session, j)
    incident = _derive_job_incident(j, effective, current_step=None)
    if not incident:
        raise HTTPException(409, "No open supervisor incident is available for this job")

    rollback_target, rollback_level, _diagnosis, safe_action = _resolve_supervisor_incident_controls(
        incident,
        dossier=_build_supervisor_rollback_context(j, effective, incident),
    )
    if safe_action != "normalize_orphan_pending_state":
        raise HTTPException(409, "This incident is not eligible for safe orphan-pending normalization")

    normalized_status = _normalized_status_for_orphan_pending(j, effective.get("status"))
    j.status = normalized_status
    _clear_pending_request_metadata(j)
    if normalized_status == "interrupted":
        j.error_message = (
            "Dangling pending request metadata was cleared. Resume the job to continue."
        )

    session.add(
        UserDecision(
            id=str(uuid.uuid4()),
            job_id=job_id,
            decision_type="supervisor_normalize_orphan_pending_state",
            payload_json={
                "incident_type": incident["incident_type"],
                "rollback_level": rollback_level,
                "final_status": normalized_status,
            },
        )
    )
    await session.commit()

    await _record_supervisor_resolution_event(
        session,
        j,
        incident,
        safe_action="normalize_orphan_pending_state",
        rollback_level=rollback_level,
        rollback_target=rollback_target,
        outcome_status=normalized_status,
    )

    thread_message = (
        "监督器已清理悬挂的 pending request 元数据，并把任务归一化到可恢复状态。"
        if getattr(j, "language", "en") == "zh"
        else "Supervisor cleared dangling pending request metadata and normalized the job to a resumable state."
    )
    await sync_supervisor_thread_state(
        job_id,
        clear_pending_command_auth=True,
        clear_pending_error_recovery=True,
        clear_resource_clarification=False,
        clear_pending_analysis_plan=False,
        message=thread_message,
    )

    return {
        "ok": True,
        "status": normalized_status,
        "rollback_level": rollback_level,
        "rollback_target": rollback_target,
    }


@router.get("/{job_id}/logs")
async def get_job_logs(
    job_id: str, limit: int = 500, session: AsyncSession = Depends(get_session)
):
    from tune.core.models import JobLog
    logs = (
        await session.execute(
            select(JobLog).where(JobLog.job_id == job_id)
            .order_by(JobLog.ts).limit(limit)
        )
    ).scalars().all()
    return [{"stream": l.stream, "line": l.line, "ts": l.ts} for l in logs]



# ---------------------------------------------------------------------------
# Pipeline-v2: plan modification and confirmation endpoints
# ---------------------------------------------------------------------------


class PlanChangeBody(BaseModel):
    change: dict  # structured change object: {type, ...}


@router.patch("/{job_id}/plan")
async def modify_plan(
    job_id: str,
    body: PlanChangeBody,
    session: AsyncSession = Depends(get_session),
):
    """Apply a structured change to an AnalysisJob's plan_draft_json."""
    from tune.core.models import UserDecision
    from tune.core.workflow.plan_changes import apply_plan_change, PlanChangeError

    j = (await session.execute(select(AnalysisJob).where(AnalysisJob.id == job_id))).scalar_one_or_none()
    if not j:
        raise HTTPException(404, "Job not found")
    if j.status not in ("awaiting_plan_confirmation", "draft"):
        raise HTTPException(409, f"Job is not in a plan-editable state (status={j.status})")

    current_steps = []
    if j.plan_draft_json:
        current_steps = j.plan_draft_json.get("steps", []) if isinstance(j.plan_draft_json, dict) else j.plan_draft_json
    elif j.plan:
        current_steps = j.plan if isinstance(j.plan, list) else j.plan.get("steps", [])

    try:
        new_steps = apply_plan_change(current_steps, body.change)
    except PlanChangeError as e:
        raise HTTPException(400, str(e))

    # Update plan_draft_json
    existing_draft = j.plan_draft_json or {}
    if isinstance(existing_draft, dict):
        existing_draft = dict(existing_draft)
        existing_draft["steps"] = new_steps
    else:
        existing_draft = {"steps": new_steps}
    j.plan_draft_json = existing_draft
    j.execution_ir_json = None
    j.expanded_dag_json = None

    decision = UserDecision(
        id=str(uuid.uuid4()),
        job_id=job_id,
        decision_type="plan_modified",
        payload_json={"change": body.change},
    )
    session.add(decision)
    await session.commit()
    return {"ok": True, "steps": new_steps}


@router.post("/{job_id}/plan/confirm")
async def confirm_plan(
    job_id: str,
    session: AsyncSession = Depends(get_session),
):
    """Confirm the plan, then either prepare execution confirmation or queue the job."""
    from tune.core.models import UserDecision
    from tune.core.orchestration import (
        materialize_job_execution_plan,
        replace_plan_steps,
        summarize_expanded_dag_for_confirmation,
    )
    from tune.core.workflow import transition_job
    from tune.core.context.builder import PlannerContextBuilder
    from tune.core.context.models import ContextScope
    from tune.core.resources.planner_adapter import enforce_planner_constraints
    from tune.core.registry.spec_generation import augment_plan_with_dynamic_specs
    from tune.core.workflow.plan_compiler import compile_plan

    j = (await session.execute(select(AnalysisJob).where(AnalysisJob.id == job_id))).scalar_one_or_none()
    if not j:
        raise HTTPException(404, "Job not found")
    if j.status not in ("awaiting_plan_confirmation", "draft"):
        raise HTTPException(409, f"Job is not awaiting plan confirmation (status={j.status})")

    # If an execution plan already exists, this is the second confirmation.
    if j.status == "awaiting_plan_confirmation" and j.execution_ir_json and j.expanded_dag_json:
        ok = await transition_job(job_id, "queued", session)
        if not ok:
            raise HTTPException(409, f"Cannot transition job {job_id} from {j.status} to queued")
        await session.commit()

        from tune.workers.tasks import run_analysis_task
        await run_analysis_task.defer_async(job_id=job_id)

        return {
            "ok": True,
            "job_id": job_id,
            "queued": True,
            "execution_plan_summary": _serialize_execution_plan(j)["summary"],
        }

    # First confirmation: validate abstract plan and materialize execution objects.
    draft = j.plan_draft_json or j.plan
    current_steps = draft.get("steps", []) if isinstance(draft, dict) else list(draft or [])
    current_steps, dynamic_issues = await augment_plan_with_dynamic_specs(
        current_steps,
        context_hint=(
            f"Goal: {getattr(j, 'goal', '') or ''}\n"
            f"Project ID: {getattr(j, 'project_id', '') or ''}"
        ),
    )
    if dynamic_issues:
        return {
            "ok": False,
            "requires_confirmation": False,
            "issues": [
                {
                    "kind": "dynamic_step_spec_error",
                    "title": "Dynamic step generation failed",
                    "description": error,
                    "suggestion": "Revise the plan or use a built-in step type.",
                }
                for error in dynamic_issues
            ],
        }
    compile_result = compile_plan(current_steps)
    if not compile_result.ok:
        return {
            "ok": False,
            "requires_confirmation": False,
            "issues": [
                {
                    "kind": "plan_compile_error",
                    "title": "Plan compilation failed",
                    "description": error,
                    "suggestion": "Modify the plan so every step has a valid type, key, and dependency chain.",
                }
                for error in compile_result.errors
            ],
        }
    draft = j.plan_draft_json or j.plan
    if isinstance(draft, dict):
        draft = dict(draft)
        draft["steps"] = compile_result.compiled_steps
    else:
        draft = compile_result.compiled_steps
    current_steps = compile_result.compiled_steps
    planner_context = await PlannerContextBuilder(session).build(
        ContextScope(project_id=j.project_id)
    )
    feasibility = enforce_planner_constraints(current_steps, planner_context)
    if feasibility.amended_plan != current_steps:
        rewritten = replace_plan_steps(draft, feasibility.amended_plan)
        j.plan_draft_json = rewritten
        await session.commit()
        return {
            "ok": False,
            "requires_confirmation": True,
            "issues": [],
            "steps": feasibility.amended_plan,
        }
    if not feasibility.ok:
        return {
            "ok": False,
            "requires_confirmation": False,
            "issues": [
                {
                    "kind": issue.kind,
                    "title": issue.title,
                    "description": issue.description,
                    "suggestion": issue.suggestion,
                }
                for issue in feasibility.issues
            ],
        }

    try:
        await materialize_job_execution_plan(session, j, draft)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc

    decision = UserDecision(
        id=str(uuid.uuid4()),
        job_id=job_id,
        decision_type="plan_confirmed",
        payload_json={"plan_step_count": len(current_steps)},
    )
    session.add(decision)

    # Transition into plan confirmation state if we started from draft.
    if j.status == "draft":
        ok = await transition_job(job_id, "awaiting_plan_confirmation", session)
        if not ok:
            raise HTTPException(409, f"Cannot transition job {job_id} from {j.status} to awaiting_plan_confirmation")
    else:
        ok = True
    if not ok:
        raise HTTPException(409, f"Cannot transition job {job_id} from {j.status}")
    await session.commit()

    return {
        "ok": True,
        "job_id": job_id,
        "requires_execution_confirmation": True,
        "execution_plan_summary": _serialize_execution_plan(j)["summary"],
        "execution_review_plan": summarize_expanded_dag_for_confirmation(j.expanded_dag_json),
    }


# ---------------------------------------------------------------------------
# Pipeline-v2: authorization and repair request endpoints
# ---------------------------------------------------------------------------


class AuthResolveBody(BaseModel):
    action: str  # "approved" | "rejected"


@router.get("/{job_id}/authorization-requests")
async def list_auth_requests(job_id: str, session: AsyncSession = Depends(get_session)):
    """Return all CommandAuthorizationRequests for a job (latest first)."""
    from tune.core.models import CommandAuthorizationRequest

    rows = (await session.execute(
        select(CommandAuthorizationRequest)
        .where(CommandAuthorizationRequest.job_id == job_id)
        .order_by(CommandAuthorizationRequest.requested_at.desc())
    )).scalars().all()
    return [
        {
            "id": r.id,
            "step_id": r.step_id,
            "command_text": r.command_text,
            "command_template_type": r.command_template_type,
            "status": r.status,
            "requested_at": r.requested_at,
            "resolved_at": r.resolved_at,
        }
        for r in rows
    ]


@router.post("/{job_id}/authorization-requests/{req_id}/resolve")
async def resolve_auth_request(
    job_id: str,
    req_id: str,
    body: AuthResolveBody,
    session: AsyncSession = Depends(get_session),
):
    """Approve or reject a CommandAuthorizationRequest (pipeline-v2 path)."""
    from datetime import datetime, timezone

    from tune.core.models import CommandAuthorizationRequest, UserDecision

    if body.action not in ("approved", "rejected"):
        raise HTTPException(400, "action must be 'approved' or 'rejected'")

    req = (await session.execute(
        select(CommandAuthorizationRequest).where(
            CommandAuthorizationRequest.id == req_id,
            CommandAuthorizationRequest.job_id == job_id,
        )
    )).scalar_one_or_none()
    if not req:
        raise HTTPException(404, "Authorization request not found")
    if req.status != "pending":
        raise HTTPException(409, f"Request already {req.status}")

    req.status = body.action
    req.resolved_at = datetime.now(timezone.utc)

    decision = UserDecision(
        id=str(uuid.uuid4()),
        job_id=job_id,
        step_id=req.step_id,
        decision_type=(
            "authorization_approved" if body.action == "approved" else "authorization_rejected"
        ),
        payload_json={"auth_request_id": req_id, "command_type": req.command_template_type},
    )
    session.add(decision)

    await session.commit()

    from tune.api.ws import _authorized_types, resolve_command_authorization
    if body.action == "approved" and req.command_template_type:
        _authorized_types.setdefault(job_id, set()).add(req.command_template_type)
    await resolve_command_authorization(job_id, req_id, approved=body.action == "approved")

    return {"ok": True}


class RepairResolveBody(BaseModel):
    choice: str  # retry_original|modify_params|rebind_input|skip_step|cancel_job
    params: dict | None = None
    slot_name: str | None = None
    new_path: str | None = None


@router.get("/{job_id}/repair-requests")
async def list_repair_requests(job_id: str, session: AsyncSession = Depends(get_session)):
    """Return all RepairRequests for a job (latest first)."""
    from tune.core.models import RepairRequest

    rows = (await session.execute(
        select(RepairRequest)
        .where(RepairRequest.job_id == job_id)
        .order_by(RepairRequest.created_at.desc())
    )).scalars().all()
    return [
        {
            "id": r.id,
            "step_id": r.step_id,
            "failed_command": r.failed_command,
            "stderr_excerpt": r.stderr_excerpt,
            "repair_level": r.repair_level,
            "status": r.status,
            "suggestion_json": r.suggestion_json,
            "created_at": r.created_at,
            "resolved_at": r.resolved_at,
        }
        for r in rows
    ]


@router.post("/{job_id}/repair-requests/{req_id}/resolve")
async def resolve_repair_request(
    job_id: str,
    req_id: str,
    body: RepairResolveBody,
    session: AsyncSession = Depends(get_session),
):
    """Submit a structured repair decision (pipeline-v2 path)."""
    from datetime import datetime, timezone

    from tune.core.models import RepairRequest, UserDecision
    from tune.core.workflow import transition_job

    valid_choices = {"retry_original", "modify_params", "rebind_input", "skip_step", "cancel_job"}
    if body.choice not in valid_choices:
        raise HTTPException(400, f"choice must be one of {sorted(valid_choices)}")

    req = (await session.execute(
        select(RepairRequest).where(
            RepairRequest.id == req_id,
            RepairRequest.job_id == job_id,
        )
    )).scalar_one_or_none()
    if not req:
        raise HTTPException(404, "Repair request not found")
    if req.status != "pending":
        raise HTTPException(409, f"Repair request already {req.status}")

    req.status = "resolved"
    req.resolved_at = datetime.now(timezone.utc)
    repair_command = req.failed_command or ""
    should_continue = body.choice != "cancel_job"

    if body.choice == "modify_params":
        repair_command = str((body.params or {}).get("command") or repair_command)
    elif body.choice == "retry_original":
        repair_command = req.failed_command or ""
    elif body.choice == "rebind_input":
        repair_command = str((body.params or {}).get("command") or repair_command)
    elif body.choice == "skip_step":
        should_continue = False

    req.human_resolution_json = {
        "command": repair_command,
        "should_continue": should_continue,
        "choice": body.choice,
        "slot_name": body.slot_name,
        "new_path": body.new_path,
        "params": body.params or {},
    }

    decision = UserDecision(
        id=str(uuid.uuid4()),
        job_id=job_id,
        step_id=req.step_id,
        decision_type="repair_choice",
        payload_json={
            "repair_request_id": req_id,
            "choice": body.choice,
            "params": body.params,
            "slot_name": body.slot_name,
            "new_path": body.new_path,
        },
    )
    session.add(decision)

    if body.choice == "cancel_job":
        req.status = "cancelled"
        await transition_job(job_id, "cancelled", session)

    await session.commit()

    from tune.api.ws import broadcast_project_task_event, clear_chat_state_for_job
    clear_chat_state_for_job("pending_error_recovery", job_id)

    if body.choice != "cancel_job":
        from tune.workers.defer import defer_async_with_fallback
        from tune.workers.tasks import run_analysis_task

        await defer_async_with_fallback(run_analysis_task, job_id=job_id)

    await broadcast_project_task_event(job_id, reason="repair_resolved")

    return {"ok": True}


# ---------------------------------------------------------------------------
# Pipeline-v2: input binding endpoints
# ---------------------------------------------------------------------------


@router.get("/{job_id}/bindings")
async def get_bindings(
    job_id: str,
    detailed: bool = False,
    session: AsyncSession = Depends(get_session),
):
    """Return all InputBinding records for a job, grouped by step_id."""
    from tune.core.models import AnalysisStepRun, InputBinding
    job = (await session.execute(
        select(AnalysisJob).where(AnalysisJob.id == job_id)
    )).scalar_one_or_none()
    if not job:
        raise HTTPException(404, "Job not found")
    effective = await _get_effective_job_state(session, job)
    current_step = await _fetch_job_current_step(session, job)
    rollback_guidance = await _build_job_rollback_guidance(
        session,
        job,
        effective,
        current_step=current_step,
    )

    if not detailed:
        confirmation = _serialize_confirmation_details(
            job,
            effective["pending_interaction_type"],
        )
        recent_logs = await _fetch_recent_job_logs(session, job_id)
        recent_decisions = await _fetch_recent_user_decisions(session, job_id)
        auth_requests = await _fetch_recent_auth_requests(session, job_id)
        repair_requests = await _fetch_recent_repair_requests(session, job_id)
        step_runs = await _fetch_recent_step_runs(session, job_id)
        artifacts = await _fetch_recent_artifacts(session, job_id)
        rows = (await session.execute(
            select(InputBinding).where(InputBinding.job_id == job_id)
        )).scalars().all()

        by_step: dict[str, list[dict]] = {}
        for r in rows:
            by_step.setdefault(r.step_id, []).append(_serialize_binding(r))
        return {
            "job_status": effective["status"],
            "error_message": effective["error_message"],
            "pending_interaction_type": effective["pending_interaction_type"],
            "pending_interaction_payload": effective["pending_interaction_payload"],
            "runtime_diagnostics": effective.get("runtime_diagnostics") or [],
            "rollback_guidance": rollback_guidance,
            "auto_recovery_events": _extract_auto_recovery_events(recent_logs),
            "timeline": _build_job_timeline(
                job,
                recent_logs=recent_logs,
                recent_decisions=recent_decisions,
                auth_requests=auth_requests,
                repair_requests=repair_requests,
                step_runs=step_runs,
                artifacts=artifacts,
                runtime_diagnostics=effective.get("runtime_diagnostics") or [],
                rollback_guidance=rollback_guidance,
            ),
            "confirmation_phase": confirmation["confirmation_phase"],
            "confirmation_plan": confirmation["confirmation_plan"],
            "execution_plan_summary": confirmation["execution_plan_summary"],
            "bindings": by_step,
        }

    rows = (
        await session.execute(
            select(InputBinding, AnalysisStepRun)
            .outerjoin(AnalysisStepRun, AnalysisStepRun.id == InputBinding.step_id)
            .where(InputBinding.job_id == job_id)
            .order_by(
                AnalysisStepRun.started_at,
                AnalysisStepRun.step_key,
                InputBinding.slot_name,
            )
        )
    ).all()

    steps: dict[str, dict] = {}
    for binding, step_run in rows:
        step_payload = steps.setdefault(
            binding.step_id,
            {
                "step_id": binding.step_id,
                "step_key": getattr(step_run, "step_key", None),
                "step_type": getattr(step_run, "step_type", None),
                "display_name": getattr(step_run, "display_name", None),
                "status": getattr(step_run, "status", None),
                "bindings": [],
            },
        )
        step_payload["bindings"].append(_serialize_binding_detail(binding, step_run))

    confirmation = _serialize_confirmation_details(
        job,
        effective["pending_interaction_type"],
    )
    recent_logs = await _fetch_recent_job_logs(session, job_id)
    recent_decisions = await _fetch_recent_user_decisions(session, job_id)
    auth_requests = await _fetch_recent_auth_requests(session, job_id)
    repair_requests = await _fetch_recent_repair_requests(session, job_id)
    step_runs = await _fetch_recent_step_runs(session, job_id)
    artifacts = await _fetch_recent_artifacts(session, job_id)

    return {
        "job_status": effective["status"],
        "error_message": effective["error_message"],
        "pending_interaction_type": effective["pending_interaction_type"],
        "pending_interaction_payload": effective["pending_interaction_payload"],
        "runtime_diagnostics": effective.get("runtime_diagnostics") or [],
        "rollback_guidance": rollback_guidance,
        "auto_recovery_events": _extract_auto_recovery_events(recent_logs),
        "timeline": _build_job_timeline(
            job,
            recent_logs=recent_logs,
            recent_decisions=recent_decisions,
            auth_requests=auth_requests,
            repair_requests=repair_requests,
            step_runs=step_runs,
            artifacts=artifacts,
            runtime_diagnostics=effective.get("runtime_diagnostics") or [],
            rollback_guidance=rollback_guidance,
        ),
        "confirmation_phase": confirmation["confirmation_phase"],
        "confirmation_plan": confirmation["confirmation_plan"],
        "execution_plan_summary": confirmation["execution_plan_summary"],
        "steps": list(steps.values()),
    }


class BindingUpdateBody(BaseModel):
    resolved_path: str
    source_type: str = "user_provided"


@router.patch("/{job_id}/bindings/{binding_id}")
async def update_binding(
    job_id: str,
    binding_id: str,
    body: BindingUpdateBody,
    session: AsyncSession = Depends(get_session),
):
    """Set or override a binding's resolved_path. Transitions job to queued if all bindings resolved."""
    from tune.core.models import InputBinding
    from tune.core.workflow import transition_job

    binding = (await session.execute(
        select(InputBinding).where(
            InputBinding.id == binding_id,
            InputBinding.job_id == job_id,
        )
    )).scalar_one_or_none()
    if not binding:
        raise HTTPException(404, "Binding not found")

    binding.resolved_path = body.resolved_path
    binding.source_type = body.source_type
    binding.status = "resolved"

    # Check if all required bindings are now resolved
    all_bindings = (await session.execute(
        select(InputBinding).where(InputBinding.job_id == job_id)
    )).scalars().all()
    any_missing = any(b.status == "missing" for b in all_bindings)

    j = (await session.execute(
        select(AnalysisJob).where(AnalysisJob.id == job_id)
    )).scalar_one_or_none()
    if j and j.status == "resource_clarification_required" and not any_missing:
        await transition_job(job_id, "queued", session)

    await session.commit()
    return {"ok": True, "all_resolved": not any_missing}


# ---------------------------------------------------------------------------
# Pipeline-v2: skill template extraction
# ---------------------------------------------------------------------------


@router.post("/{job_id}/extract-skill")
async def extract_skill(job_id: str, session: AsyncSession = Depends(get_session)):
    """Extract a reusable SkillTemplate + SkillVersion from a completed job."""
    from tune.core.models import AnalysisJob
    from tune.core.skills import extract_skill_template, create_skill_version

    job = (await session.execute(
        select(AnalysisJob).where(AnalysisJob.id == job_id)
    )).scalar_one_or_none()
    if not job:
        raise HTTPException(404, "Job not found")
    if job.status != "completed":
        raise HTTPException(400, f"Job status is '{job.status}'; must be 'completed'")

    template = await extract_skill_template(job_id, session)
    version = await create_skill_version(template.id, job_id, session)
    await session.commit()

    return {
        "template_id": template.id,
        "template_name": template.name,
        "step_types": template.step_types,
        "version_id": version.id,
        "version_number": version.version_number,
    }


# ---------------------------------------------------------------------------
# Resource graph endpoint
# ---------------------------------------------------------------------------


@router.get("/{job_id}/resource-graph")
async def get_resource_graph(job_id: str, session: AsyncSession = Depends(get_session)):
    """Return the serialized ResourceGraph for a job.

    If the job has a persisted resource_graph_json snapshot, return it.
    Otherwise, re-build the ResourceGraph on demand from the job's project context.
    """
    import json
    from tune.core.models import AnalysisJob

    job = (await session.execute(
        select(AnalysisJob).where(AnalysisJob.id == job_id)
    )).scalar_one_or_none()
    if not job:
        raise HTTPException(404, "Job not found")

    # Return cached snapshot if available
    if job.resource_graph_json:
        try:
            return json.loads(job.resource_graph_json)
        except Exception:
            pass

    # Re-build on demand if project is set
    if not job.project_id:
        raise HTTPException(404, "No resource graph available for this job")

    try:
        from tune.core.context.builder import PlannerContextBuilder
        from tune.core.context.models import ContextScope
        from tune.core.resources.graph_builder import ResourceGraphBuilder

        ctx = await PlannerContextBuilder(session).build(
            ContextScope(project_id=job.project_id)
        )
        graph = ctx.resource_graph
        if graph is None:
            graph = await ResourceGraphBuilder().build(ctx, session)

        # Serialize graph to dict for JSON response
        def _node_dict(n):
            return {
                "id": n.id,
                "kind": n.kind,
                "status": n.status,
                "label": n.label,
                "resolved_path": n.resolved_path,
                "organism": n.organism,
                "genome_build": n.genome_build,
                "source_type": n.source_type,
                "derive_command": n.derive_command,
                "candidates": [
                    {"path": c.path, "organism": c.organism, "confidence": c.confidence}
                    for c in n.candidates
                ],
            }

        return {
            "nodes": {nid: _node_dict(node) for nid, node in graph.nodes.items()},
            "edges": [
                {"from_id": e.from_id, "to_id": e.to_id, "relation": e.relation}
                for e in graph.edges
            ],
            "by_kind": graph.by_kind,
            "read_groups": [
                {
                    "sample_id": rg.sample_id,
                    "sample_name": rg.sample_name,
                    "experiment_id": rg.experiment_id,
                    "library_strategy": rg.library_strategy,
                    "read1_resource_id": rg.read1_resource_id,
                    "read2_resource_id": rg.read2_resource_id,
                }
                for rg in graph.read_groups
            ],
        }
    except Exception as exc:
        raise HTTPException(500, f"Failed to build resource graph: {exc}") from exc
