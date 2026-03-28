from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from tune.core.models import AnalysisJob, AnalysisStepRun, CommandAuthorizationRequest, JobLog, RepairRequest

log = logging.getLogger(__name__)

STALL_PROGRESS_THRESHOLD_SECONDS = 180
RUNTIME_WATCHDOG_POLL_INTERVAL_SECONDS = 30
AUTO_NORMALIZE_PENDING_ISSUES = {"orphan_pending_request", "job_status_mismatch"}
AUTO_RETRY_RESUME_STATUSES = {"waiting_for_authorization", "waiting_for_repair"}

_runtime_watchdog_task: asyncio.Task | None = None
_stalled_signatures: dict[str, str] = {}
_pending_state_signatures: dict[str, str] = {}


def _normalize_ts(ts):
    if ts is None:
        return None
    if getattr(ts, "tzinfo", None) is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts


def get_job_progress_reference(job: Any):
    return (
        _normalize_ts(getattr(job, "last_progress_at", None))
        or _normalize_ts(getattr(job, "started_at", None))
        or _normalize_ts(getattr(job, "created_at", None))
    )


def get_job_stall_age_seconds(job: Any, *, now: datetime | None = None) -> int | None:
    if getattr(job, "status", None) != "running":
        return None
    reference = get_job_progress_reference(job)
    if reference is None:
        return None
    current = now or datetime.now(timezone.utc)
    return max(0, int((current - reference).total_seconds()))


def is_job_stalled(
    job: Any,
    *,
    now: datetime | None = None,
    threshold_seconds: int = STALL_PROGRESS_THRESHOLD_SECONDS,
) -> bool:
    age_seconds = get_job_stall_age_seconds(job, now=now)
    return age_seconds is not None and age_seconds >= threshold_seconds


def _stall_signature(job: Any) -> str:
    reference = get_job_progress_reference(job)
    if reference is None:
        return "none"
    return reference.isoformat()


async def _emit_watchdog_log(session, job: AnalysisJob, line: str) -> None:
    from tune.api.ws import broadcast_job_event

    session.add(JobLog(id=str(uuid.uuid4()), job_id=job.id, stream="stderr", line=line))
    await session.commit()
    await broadcast_job_event(job.id, {"type": "log", "stream": "stderr", "line": line})


def _clear_pending_request_metadata(job: Any) -> None:
    job.pending_auth_request_id = None
    job.pending_repair_request_id = None
    job.pending_step_key = None
    if getattr(job, "pending_interaction_type", None) in {"authorization", "repair"}:
        job.pending_interaction_type = None
        job.pending_interaction_payload_json = None


def _normalized_status_for_orphan_pending(job: Any, effective_status: str | None) -> str:
    status = effective_status or getattr(job, "status", None) or "interrupted"
    if status in {"waiting_for_authorization", "waiting_for_repair"}:
        return "interrupted"
    return status


async def _record_watchdog_auto_resolution(
    session,
    job: AnalysisJob,
    *,
    issue_kind: str,
    safe_action: str,
    rollback_target: str,
    outcome_status: str,
    pending_types: str,
) -> None:
    if not getattr(job, "project_id", None):
        return

    try:
        from tune.core.memory.project_memory import write_execution_event

        step_label = str(getattr(job, "pending_step_key", None) or "").strip()
        step_fragment = f" at step '{step_label}'" if step_label else ""
        await write_execution_event(
            session,
            project_id=job.project_id,
            event_type="supervisor_resolution",
            description=(
                f"Watchdog auto-resolved {issue_kind} incident for job "
                f"'{getattr(job, 'name', None) or getattr(job, 'id', 'unknown')}'{step_fragment}."
            ),
            resolution=(
                f"Applied safe action '{safe_action}'; rollback_level=step; "
                f"rollback_target={rollback_target}; resulting_status={outcome_status}; "
                f"pending_types={pending_types or 'request'}"
            ),
            user_contributed=False,
        )
    except Exception:
        log.exception(
            "watchdog: failed to record auto-resolution memory for job_id=%s issue=%s",
            getattr(job, "id", None),
            issue_kind,
        )


async def _auto_normalize_pending_issue(
    session,
    job: AnalysisJob,
    *,
    issue_kind: str,
    pending_types: str,
    emit_log=None,
) -> dict[str, Any] | None:
    if issue_kind not in AUTO_NORMALIZE_PENDING_ISSUES:
        return None

    clear_pending_command_auth = bool(getattr(job, "pending_auth_request_id", None))
    clear_pending_error_recovery = bool(getattr(job, "pending_repair_request_id", None))
    rollback_target = getattr(job, "pending_step_key", None) or (
        "job_state_normalization" if issue_kind == "job_status_mismatch" else "pending_request_record"
    )

    if issue_kind == "job_status_mismatch":
        normalized_status = getattr(job, "status", None) or "failed"
        safe_action = "normalize_terminal_state"
    else:
        normalized_status = _normalized_status_for_orphan_pending(job, getattr(job, "status", None))
        safe_action = "normalize_orphan_pending_state"

    _clear_pending_request_metadata(job)
    job.status = normalized_status
    if issue_kind == "orphan_pending_request" and normalized_status == "interrupted":
        job.error_message = "Dangling pending request metadata was cleared automatically. Resume the job to continue."

    await session.commit()
    await _record_watchdog_auto_resolution(
        session,
        job,
        issue_kind=issue_kind,
        safe_action=safe_action,
        rollback_target=rollback_target,
        outcome_status=normalized_status,
        pending_types=pending_types,
    )

    emitter = emit_log or _emit_watchdog_log
    await emitter(
        session,
        job,
        (
            f"[watchdog] Auto-normalized {issue_kind} via {safe_action} "
            f"(status={normalized_status}, pending={pending_types or 'request'})."
        ),
    )

    try:
        from tune.api.ws import sync_supervisor_thread_state

        await sync_supervisor_thread_state(
            job.id,
            clear_pending_command_auth=clear_pending_command_auth,
            clear_pending_error_recovery=clear_pending_error_recovery,
            clear_resource_clarification=False,
            clear_pending_analysis_plan=False,
        )
    except Exception:
        log.exception(
            "watchdog: failed to sync thread state after auto-normalizing job_id=%s issue=%s",
            getattr(job, "id", None),
            issue_kind,
        )

    return {
        "safe_action": safe_action,
        "resulting_status": normalized_status,
        "rollback_target": rollback_target,
    }


def _resume_retry_is_auto_eligible(job: AnalysisJob, diagnostics: list[dict[str, Any]]) -> tuple[bool, list[str], list[str]]:
    job_status = str(getattr(job, "status", None) or "").strip()
    pending_reference_types = sorted(
        filter(
            None,
            [
                "authorization" if getattr(job, "pending_auth_request_id", None) else "",
                "repair" if getattr(job, "pending_repair_request_id", None) else "",
            ],
        )
    )
    resolved_pending_types = sorted(
        {
            str(item.get("request_type") or "").strip()
            for item in diagnostics
            if str(item.get("kind") or "").strip() == "resolved_pending_request"
            and str(item.get("request_type") or "").strip()
        }
    )
    if job_status not in AUTO_RETRY_RESUME_STATUSES:
        return False, pending_reference_types, resolved_pending_types
    if len(pending_reference_types) != 1:
        return False, pending_reference_types, resolved_pending_types
    if len(resolved_pending_types) != 1:
        return False, pending_reference_types, resolved_pending_types
    if pending_reference_types != resolved_pending_types:
        return False, pending_reference_types, resolved_pending_types
    if job_status == "waiting_for_authorization" and pending_reference_types != ["authorization"]:
        return False, pending_reference_types, resolved_pending_types
    if job_status == "waiting_for_repair" and pending_reference_types != ["repair"]:
        return False, pending_reference_types, resolved_pending_types
    return True, pending_reference_types, resolved_pending_types


async def _auto_retry_resume_failed_issue(
    session,
    job: AnalysisJob,
    *,
    diagnostics: list[dict[str, Any]],
    pending_types: str,
    emit_log=None,
) -> dict[str, Any] | None:
    eligible, pending_reference_types, resolved_pending_types = _resume_retry_is_auto_eligible(job, diagnostics)
    if not eligible:
        return None

    from tune.api.ws import sync_supervisor_thread_state
    from tune.workers.defer import defer_async_with_fallback
    from tune.workers.tasks import resume_job_task

    rollback_target = getattr(job, "pending_step_key", None) or "resume_chain"
    resulting_status = str(getattr(job, "status", None) or "interrupted")
    job.error_message = "Retrying resolved pending-decision resume chain automatically."
    await session.commit()
    await defer_async_with_fallback(resume_job_task, job_id=job.id)
    await _record_watchdog_auto_resolution(
        session,
        job,
        issue_kind="resume_failed",
        safe_action="retry_resume_chain",
        rollback_target=rollback_target,
        outcome_status=resulting_status,
        pending_types=pending_types,
    )

    emitter = emit_log or _emit_watchdog_log
    await emitter(
        session,
        job,
        (
            f"[watchdog] Auto-applied resume_failed via retry_resume_chain "
            f"(status={resulting_status}, pending={pending_types or 'request'})."
        ),
    )

    try:
        await sync_supervisor_thread_state(
            job.id,
            clear_pending_command_auth=bool(getattr(job, "pending_auth_request_id", None)),
            clear_pending_error_recovery=bool(getattr(job, "pending_repair_request_id", None)),
            clear_resource_clarification=False,
            clear_pending_analysis_plan=False,
            message="Watchdog retried the resolved human-decision resume chain. The job will attempt to continue.",
            emit_job_started=True,
            job_name=getattr(job, "name", None),
        )
    except Exception:
        log.exception(
            "watchdog: failed to sync thread state after auto-retrying resume_failed job_id=%s",
            getattr(job, "id", None),
        )

    return {
        "safe_action": "retry_resume_chain",
        "resulting_status": resulting_status,
        "rollback_target": rollback_target,
        "pending_reference_types": pending_reference_types,
        "resolved_pending_types": resolved_pending_types,
    }


async def scan_runtime_health_once(
    session,
    *,
    now: datetime | None = None,
    emit_log=None,
) -> list[dict[str, Any]]:
    current = now or datetime.now(timezone.utc)
    jobs = (
        await session.execute(
            select(AnalysisJob).where(AnalysisJob.status == "running")
        )
    ).scalars().all()

    emitted: list[dict[str, Any]] = []
    active_job_ids = {str(getattr(job, "id", "")) for job in jobs if getattr(job, "id", None)}

    for stale_job_id in list(_stalled_signatures):
        if stale_job_id not in active_job_ids:
            _stalled_signatures.pop(stale_job_id, None)

    for job in jobs:
        job_id = str(getattr(job, "id", "") or "")
        if not job_id:
            continue
        if not is_job_stalled(job, now=current):
            _stalled_signatures.pop(job_id, None)
            continue

        signature = _stall_signature(job)
        if _stalled_signatures.get(job_id) == signature:
            continue

        current_step_name = None
        if getattr(job, "current_step_id", None):
            current_step = (
                await session.execute(
                    select(AnalysisStepRun).where(AnalysisStepRun.id == job.current_step_id)
                )
            ).scalar_one_or_none()
            if current_step is not None:
                current_step_name = getattr(current_step, "display_name", None) or getattr(current_step, "step_key", None)

        step_label = current_step_name or getattr(job, "pending_step_key", None) or "unknown"
        age_seconds = get_job_stall_age_seconds(job, now=current) or STALL_PROGRESS_THRESHOLD_SECONDS
        age_minutes = max(1, age_seconds // 60)
        line = (
            f"[watchdog] No progress heartbeat for {age_minutes} min while job remains running "
            f"(step={step_label})."
        )

        emitter = emit_log or _emit_watchdog_log
        await emitter(session, job, line)
        _stalled_signatures[job_id] = signature
        emitted.append({"job_id": job_id, "line": line, "age_seconds": age_seconds})

    return emitted


async def scan_pending_request_health_once(
    session,
    *,
    emit_log=None,
    auto_normalize: bool = True,
    auto_retry_resolved_resume: bool = False,
) -> list[dict[str, Any]]:
    jobs = (
        await session.execute(
            select(AnalysisJob).where(
                (AnalysisJob.pending_auth_request_id.is_not(None))
                | (AnalysisJob.pending_repair_request_id.is_not(None))
            )
        )
    ).scalars().all()

    emitted: list[dict[str, Any]] = []
    active_job_ids = {str(getattr(job, "id", "")) for job in jobs if getattr(job, "id", None)}
    for stale_job_id in list(_pending_state_signatures):
        if stale_job_id not in active_job_ids:
            _pending_state_signatures.pop(stale_job_id, None)

    terminal_statuses = {"completed", "failed", "cancelled"}

    for job in jobs:
        diagnostics: list[dict[str, Any]] = []

        if getattr(job, "pending_auth_request_id", None):
            auth_req = (
                await session.execute(
                    select(CommandAuthorizationRequest).where(
                        CommandAuthorizationRequest.id == job.pending_auth_request_id
                    )
                )
            ).scalar_one_or_none()
            if auth_req is None:
                diagnostics.append({"kind": "orphan_pending_request", "request_type": "authorization"})
            elif getattr(auth_req, "status", None) != "pending":
                diagnostics.append(
                    {
                        "kind": "resolved_pending_request",
                        "request_type": "authorization",
                        "request_status": getattr(auth_req, "status", None),
                    }
                )

        if getattr(job, "pending_repair_request_id", None):
            repair_req = (
                await session.execute(
                    select(RepairRequest).where(
                        RepairRequest.id == job.pending_repair_request_id
                    )
                )
            ).scalar_one_or_none()
            if repair_req is None:
                diagnostics.append({"kind": "orphan_pending_request", "request_type": "repair"})
            elif getattr(repair_req, "status", None) != "pending":
                diagnostics.append(
                    {
                        "kind": "resolved_pending_request",
                        "request_type": "repair",
                        "request_status": getattr(repair_req, "status", None),
                    }
                )

        job_id = str(getattr(job, "id", "") or "")
        if not job_id:
            continue
        if not diagnostics:
            _pending_state_signatures.pop(job_id, None)
            continue

        pending_types = ", ".join(sorted({str(item.get("request_type") or "request") for item in diagnostics}))
        has_orphan = any(item.get("kind") == "orphan_pending_request" for item in diagnostics)
        if getattr(job, "status", None) in terminal_statuses:
            issue_kind = "job_status_mismatch"
            line = (
                f"[watchdog] Terminal job still carries stale pending request metadata "
                f"(status={job.status}, pending={pending_types or 'request'})."
            )
        elif has_orphan:
            issue_kind = "orphan_pending_request"
            line = (
                f"[watchdog] Job references a missing pending request record "
                f"(status={job.status}, pending={pending_types or 'request'})."
            )
        else:
            issue_kind = "resume_failed"
            line = (
                f"[watchdog] Pending human decision was already resolved, but the job did not resume "
                f"(status={job.status}, pending={pending_types or 'request'})."
            )

        signature = f"{issue_kind}:{job.status}:{pending_types}"
        if _pending_state_signatures.get(job_id) == signature:
            continue

        emitter = emit_log or _emit_watchdog_log
        await emitter(session, job, line)
        _pending_state_signatures[job_id] = signature
        entry = {"job_id": job_id, "line": line, "issue_kind": issue_kind}
        if auto_normalize and issue_kind in AUTO_NORMALIZE_PENDING_ISSUES:
            auto_result = await _auto_normalize_pending_issue(
                session,
                job,
                issue_kind=issue_kind,
                pending_types=pending_types,
                emit_log=emit_log,
            )
            if auto_result:
                entry.update(
                    {
                        "auto_applied": True,
                        "safe_action": auto_result["safe_action"],
                        "resulting_status": auto_result["resulting_status"],
                        "rollback_target": auto_result["rollback_target"],
                    }
                )
        elif auto_retry_resolved_resume and issue_kind == "resume_failed":
            auto_result = await _auto_retry_resume_failed_issue(
                session,
                job,
                diagnostics=diagnostics,
                pending_types=pending_types,
                emit_log=emit_log,
            )
            if auto_result:
                entry.update(
                    {
                        "auto_applied": True,
                        "safe_action": auto_result["safe_action"],
                        "resulting_status": auto_result["resulting_status"],
                        "rollback_target": auto_result["rollback_target"],
                        "pending_reference_types": auto_result["pending_reference_types"],
                        "resolved_pending_types": auto_result["resolved_pending_types"],
                    }
                )
        emitted.append(entry)

    return emitted


async def _runtime_watchdog_loop() -> None:
    from tune.core.database import get_session_factory

    while True:
        await asyncio.sleep(RUNTIME_WATCHDOG_POLL_INTERVAL_SECONDS)
        try:
            async with get_session_factory()() as session:
                await scan_runtime_health_once(session)
                await scan_pending_request_health_once(session, auto_retry_resolved_resume=True)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("runtime watchdog scan failed")


async def start_runtime_watchdog() -> None:
    global _runtime_watchdog_task
    if _runtime_watchdog_task and not _runtime_watchdog_task.done():
        return
    _runtime_watchdog_task = asyncio.create_task(_runtime_watchdog_loop())


async def stop_runtime_watchdog() -> None:
    global _runtime_watchdog_task
    if not _runtime_watchdog_task:
        return
    _runtime_watchdog_task.cancel()
    try:
        await _runtime_watchdog_task
    except asyncio.CancelledError:
        pass
    _runtime_watchdog_task = None


def reset_runtime_watchdog_state() -> None:
    _stalled_signatures.clear()
    _pending_state_signatures.clear()
