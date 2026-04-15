"""WebSocket endpoints for chat and job log streaming."""
from __future__ import annotations

import asyncio
import json
import logging
import uuid as _uuid_mod
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

log = logging.getLogger(__name__)
router = APIRouter()

# job_id -> set of connected WebSockets (task monitor)
_job_subscribers: dict[str, set[WebSocket]] = {}

# Chat websocket routing indexes
_chat_sockets: set[WebSocket] = set()
_chat_session_by_socket: dict[WebSocket, dict[str, Any]] = {}
_chat_thread_by_socket: dict[WebSocket, str] = {}
_chat_project_by_socket: dict[WebSocket, str] = {}
_thread_chat_sockets: dict[str, set[WebSocket]] = {}
_project_chat_sockets: dict[str, set[WebSocket]] = {}

# Already-authorized command types per job (session-level cache; fine to lose on restart)
_authorized_types: dict[str, set[str]] = {}
_active_session_states: list[dict] = []  # all live chat session_state dicts
_pending_commands: dict[str, str] = {}
_pending_auth_context: dict[str, dict[str, Any]] = {}

# Legacy in-process wait handles kept for compatibility with route imports.
_auth_events: dict[str, asyncio.Event] = {}
_auth_results: dict[str, bool] = {}
_error_recovery_events: dict[str, asyncio.Event] = {}
_error_recovery_results: dict[str, dict[str, Any]] = {}


class AuthorizationPendingError(Exception):
    """Raised by request_authorization() instead of blocking when DB-poll mode is active.

    Signals that a CommandAuthorizationRequest has been persisted and the job has been
    transitioned to waiting_for_authorization.  The worker should catch this, save any
    remaining context it needs, and return — the job will be resumed via defer_async once
    the user makes a decision.
    """

    def __init__(self, auth_request_id: str) -> None:
        self.auth_request_id = auth_request_id
        super().__init__(f"authorization pending: {auth_request_id}")


async def broadcast_job_event(job_id: str, event: dict[str, Any]) -> None:
    """Push an event to all task-monitor WebSockets subscribed to a job."""
    sockets = _job_subscribers.get(job_id, set())
    dead = set()
    for ws in sockets:
        try:
            await ws.send_json(event)
        except Exception:
            dead.add(ws)
    for ws in dead:
        sockets.discard(ws)


def _index_add(index: dict[str, set[WebSocket]], key: str | None, websocket: WebSocket) -> None:
    if key:
        index.setdefault(key, set()).add(websocket)


def _index_remove(index: dict[str, set[WebSocket]], key: str | None, websocket: WebSocket) -> None:
    if not key:
        return
    sockets = index.get(key)
    if not sockets:
        return
    sockets.discard(websocket)
    if not sockets:
        index.pop(key, None)


def _set_chat_socket_thread(websocket: WebSocket, thread_id: str | None) -> None:
    previous = _chat_thread_by_socket.get(websocket)
    if previous == thread_id:
        return
    _index_remove(_thread_chat_sockets, previous, websocket)
    if previous:
        _chat_thread_by_socket.pop(websocket, None)
    if thread_id:
        _chat_thread_by_socket[websocket] = thread_id
        _index_add(_thread_chat_sockets, thread_id, websocket)


def _set_chat_socket_project(websocket: WebSocket, project_id: str | None) -> None:
    previous = _chat_project_by_socket.get(websocket)
    if previous == project_id:
        return
    _index_remove(_project_chat_sockets, previous, websocket)
    if previous:
        _chat_project_by_socket.pop(websocket, None)
    if project_id:
        _chat_project_by_socket[websocket] = project_id
        _index_add(_project_chat_sockets, project_id, websocket)


def _serialize_thread_payload(
    *,
    thread_id: str,
    title: str | None,
    project_id: str | None,
    project_name: str | None,
    created_at,
    updated_at,
) -> dict[str, Any]:
    return {
        "id": thread_id,
        "title": title,
        "project_id": project_id,
        "project_name": project_name,
        "created_at": created_at.isoformat() if hasattr(created_at, "isoformat") else created_at,
        "updated_at": updated_at.isoformat() if hasattr(updated_at, "isoformat") else updated_at,
    }


def _register_chat_session(websocket: WebSocket, state: dict[str, Any]) -> None:
    _chat_sockets.add(websocket)
    _chat_session_by_socket[websocket] = state
    _active_session_states.append(state)
    _set_chat_socket_thread(websocket, state.get("thread_id"))
    _set_chat_socket_project(websocket, state.get("project_id"))


async def _ensure_thread_for_session(
    websocket: WebSocket,
    state: dict[str, Any],
    *,
    initial_user_text: str | None = None,
) -> dict[str, Any] | None:
    thread_id = state.get("thread_id")
    if thread_id:
        _set_chat_socket_thread(websocket, thread_id)
        return None

    from sqlalchemy import select
    from tune.core.database import get_session_factory
    from tune.core.models import Project, Thread

    project_id = state.get("project_id")
    title = None
    if initial_user_text:
        title = initial_user_text[:40]
        if len(initial_user_text) > 40:
            title += "…"

    async with get_session_factory()() as session:
        project_name = None
        if project_id:
            project = (
                await session.execute(select(Project).where(Project.id == project_id))
            ).scalar_one_or_none()
            if project:
                project_name = project.name

        thread = Thread(
            id=str(_uuid_mod.uuid4()),
            project_id=project_id,
            title=title,
        )
        session.add(thread)
        await session.commit()
        await session.refresh(thread)

    state["thread_id"] = thread.id
    _set_chat_socket_thread(websocket, thread.id)
    return _serialize_thread_payload(
        thread_id=thread.id,
        title=thread.title,
        project_id=thread.project_id,
        project_name=project_name,
        created_at=thread.created_at,
        updated_at=thread.updated_at,
    )


async def _sync_thread_project(thread_id: str | None, project_id: str | None) -> dict[str, Any] | None:
    if not thread_id:
        return None

    from sqlalchemy import select
    from tune.core.database import get_session_factory
    from tune.core.models import Project, Thread

    async with get_session_factory()() as session:
        thread = (
            await session.execute(select(Thread).where(Thread.id == thread_id))
        ).scalar_one_or_none()
        if thread is None:
            return None

        project_name = None
        if project_id:
            project = (
                await session.execute(select(Project).where(Project.id == project_id))
            ).scalar_one_or_none()
            if project:
                project_name = project.name

        if thread.project_id == project_id:
            return None

        thread.project_id = project_id
        await session.commit()
        await session.refresh(thread)

        return _serialize_thread_payload(
            thread_id=thread.id,
            title=thread.title,
            project_id=thread.project_id,
            project_name=project_name,
            created_at=thread.created_at,
            updated_at=thread.updated_at,
        )


def _unregister_chat_session(websocket: WebSocket, state: dict[str, Any]) -> None:
    _chat_sockets.discard(websocket)
    _chat_session_by_socket.pop(websocket, None)
    _set_chat_socket_thread(websocket, None)
    _set_chat_socket_project(websocket, None)
    try:
        _active_session_states.remove(state)
    except ValueError:
        pass


def _iter_session_states_for_thread(thread_id: str | None) -> list[dict[str, Any]]:
    if not thread_id:
        return []
    return [
        state
        for ws in _thread_chat_sockets.get(thread_id, set())
        if (state := _chat_session_by_socket.get(ws)) is not None
    ]


def _clear_thread_session_fields(thread_id: str | None, *field_names: str) -> None:
    for state in _iter_session_states_for_thread(thread_id):
        for field_name in field_names:
            state.pop(field_name, None)


def _set_thread_session_field(
    thread_id: str | None,
    field_name: str,
    value: dict[str, Any],
) -> None:
    for state in _iter_session_states_for_thread(thread_id):
        state[field_name] = value.copy()


def _sync_thread_analysis_case_from_pending_plan(
    thread_id: str | None,
    pending_plan: dict[str, Any] | None,
) -> None:
    from tune.core.analysis.analysis_case import build_analysis_case_payload_from_pending_plan

    for state in _iter_session_states_for_thread(thread_id):
        existing_case = state.get("active_analysis_case")
        existing_case_id = (
            str(existing_case.get("analysis_case_id") or "").strip()
            if isinstance(existing_case, dict)
            else None
        )
        payload = build_analysis_case_payload_from_pending_plan(
            pending_plan,
            existing_case_id=existing_case_id,
        )
        if payload is not None:
            state["active_analysis_case"] = payload


def _enrich_pending_plan_with_thread_analysis_case(
    thread_id: str | None,
    pending_plan: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(pending_plan, dict):
        return pending_plan
    if str(pending_plan.get("analysis_case_id") or "").strip():
        return pending_plan

    analysis_case_id = ""
    analysis_family = ""
    for state in _iter_session_states_for_thread(thread_id):
        active_case = state.get("active_analysis_case")
        if isinstance(active_case, dict):
            analysis_case_id = str(active_case.get("analysis_case_id") or "").strip()
            analysis_family = str(active_case.get("analysis_family") or "").strip()
            if analysis_case_id:
                break
        existing_pending = state.get("pending_analysis_plan")
        if isinstance(existing_pending, dict):
            analysis_case_id = str(existing_pending.get("analysis_case_id") or "").strip()
            if analysis_case_id:
                break

    if not analysis_case_id:
        return pending_plan

    enriched = dict(pending_plan)
    enriched["analysis_case_id"] = analysis_case_id
    if analysis_family and not str(enriched.get("analysis_family") or "").strip():
        enriched["analysis_family"] = analysis_family
    return enriched


def clear_chat_state_for_job(field_name: str, job_id: str) -> None:
    for state in list(_active_session_states):
        value = state.get(field_name)
        if isinstance(value, dict) and value.get("job_id") == job_id:
            state.pop(field_name, None)


async def _broadcast_to_chat_sockets(
    sockets: Iterable[WebSocket],
    event: dict[str, Any],
) -> None:
    """Push an event to the provided chat sockets and prune dead connections."""
    dead = set()
    for ws in set(sockets):
        try:
            await ws.send_json(event)
        except Exception:
            dead.add(ws)
    for ws in dead:
        state = _chat_session_by_socket.get(ws)
        if state is not None:
            _unregister_chat_session(ws, state)
        else:
            _chat_sockets.discard(ws)


async def broadcast_global_chat_event(event: dict[str, Any]) -> None:
    """Push an intentionally global chat event to every connected chat session."""
    await _broadcast_to_chat_sockets(_chat_sockets, event)


async def broadcast_thread_chat_event(thread_id: str, event: dict[str, Any]) -> None:
    """Push a chat event only to websocket sessions connected to one thread."""
    await _broadcast_to_chat_sockets(_thread_chat_sockets.get(thread_id, set()), event)


async def broadcast_project_chat_event(project_id: str, event: dict[str, Any]) -> None:
    """Push a chat event only to websocket sessions currently scoped to one project."""
    await _broadcast_to_chat_sockets(_project_chat_sockets.get(project_id, set()), event)


async def _load_job_route(job_id: str) -> tuple[str | None, str | None]:
    from sqlalchemy import select

    from tune.core.database import get_session_factory
    from tune.core.models import AnalysisJob

    async with get_session_factory()() as session:
        job = (
            await session.execute(
                select(AnalysisJob.thread_id, AnalysisJob.project_id).where(AnalysisJob.id == job_id)
            )
        ).one_or_none()
        if not job:
            return None, None
        return job[0], job[1]


async def broadcast_job_chat_event(
    job_id: str,
    event: dict[str, Any],
    *,
    fallback_scope: str = "drop",
    fallback_project_id: str | None = None,
) -> bool:
    """Push a job-facing event to the owning thread, never silently to all sessions."""
    thread_id, project_id = await _load_job_route(job_id)
    if thread_id:
        await broadcast_thread_chat_event(thread_id, event)
        return True

    if fallback_scope == "project" and (fallback_project_id or project_id):
        scoped_project_id = fallback_project_id or project_id
        log.warning(
            "broadcast_job_chat_event: job %s missing thread_id; falling back to project_id=%s for event=%s",
            job_id,
            scoped_project_id,
            event.get("type"),
        )
        await broadcast_project_chat_event(scoped_project_id, event)  # type: ignore[arg-type]
        return True

    log.warning(
        "broadcast_job_chat_event: dropping job-scoped event type=%s for job %s because thread_id is missing",
        event.get("type"),
        job_id,
    )
    return False


async def broadcast_project_task_event(
    job_id: str,
    *,
    reason: str | None = None,
    deleted: bool = False,
    project_id: str | None = None,
    thread_id: str | None = None,
    job_name: str | None = None,
) -> bool:
    """Push a lightweight project-scoped task state update for tray-style UIs."""
    payload_project_id = project_id
    payload_thread_id = thread_id
    payload_job_name = job_name
    payload_status = None
    payload_pending_type = None

    if not deleted:
        from sqlalchemy import select

        from tune.core.database import get_session_factory
        from tune.core.models import AnalysisJob
        from tune.api.routes.jobs import _get_effective_job_state

        async with get_session_factory()() as session:
            job = (
                await session.execute(
                    select(AnalysisJob).where(AnalysisJob.id == job_id)
                )
            ).scalar_one_or_none()
            if not job:
                return False
            if not all(hasattr(job, field) for field in ("status", "project_id", "thread_id", "name")):
                log.warning(
                    "broadcast_project_task_event: skipping malformed job payload for %s (%s)",
                    job_id,
                    type(job).__name__,
                )
                return False

            effective = await _get_effective_job_state(session, job)
            payload_project_id = payload_project_id or job.project_id
            payload_thread_id = payload_thread_id or job.thread_id
            payload_job_name = payload_job_name or job.name
            payload_status = effective.get("status") or job.status
            payload_pending_type = effective.get("pending_interaction_type")

    if not payload_project_id:
        return False

    await broadcast_project_chat_event(
        payload_project_id,
        {
            "type": "project_task_event",
            "reason": reason,
            "deleted": deleted,
            "job": {
                "id": job_id,
                "name": payload_job_name or "analysis",
                "status": payload_status,
                "pending_interaction_type": payload_pending_type,
                "thread_id": payload_thread_id,
                "project_id": payload_project_id,
            },
        },
    )
    return True


async def sync_supervisor_thread_state(
    job_id: str,
    *,
    clear_pending_command_auth: bool = False,
    clear_pending_error_recovery: bool = False,
    clear_resource_clarification: bool = False,
    clear_pending_analysis_plan: bool = False,
    pending_analysis_plan: dict[str, Any] | None = None,
    message: str | None = None,
    emit_job_started: bool = False,
    job_name: str | None = None,
) -> bool:
    """Synchronize active chat sessions after a supervisor action mutates job state."""
    from tune.core.analysis.persistence import patch_session_pending_plan

    thread_id, _project_id = await _load_job_route(job_id)
    if not thread_id:
        return False

    cleared_fields: list[str] = []
    if clear_pending_command_auth:
        clear_chat_state_for_job("pending_command_auth", job_id)
        _pending_commands.pop(job_id, None)
        _pending_auth_context.pop(job_id, None)
        cleared_fields.append("command_auth")
    if clear_pending_error_recovery:
        clear_chat_state_for_job("pending_error_recovery", job_id)
        cleared_fields.append("error_recovery")
    if clear_resource_clarification:
        _clear_thread_session_fields(thread_id, "resource_clarification", "pending_clarification_request")
        cleared_fields.append("resource_clarification")
    if clear_pending_analysis_plan:
        _clear_thread_session_fields(thread_id, "pending_analysis_plan")
        cleared_fields.append("analysis_plan")

    if pending_analysis_plan is not None:
        pending_analysis_plan = _enrich_pending_plan_with_thread_analysis_case(
            thread_id,
            pending_analysis_plan,
        )
        _set_thread_session_field(thread_id, "pending_analysis_plan", pending_analysis_plan)
        _sync_thread_analysis_case_from_pending_plan(thread_id, pending_analysis_plan)
        _clear_thread_session_fields(thread_id, "pending_clarification_request")
        await patch_session_pending_plan(
            thread_id=thread_id,
            project_id=_project_id,
            pending_analysis_plan=pending_analysis_plan,
            event_type="supervisor_pending_plan_set",
        )
    elif clear_pending_analysis_plan:
        await patch_session_pending_plan(
            thread_id=thread_id,
            project_id=_project_id,
            clear_pending_analysis_plan=True,
            event_type="supervisor_pending_plan_cleared",
        )

    if cleared_fields:
        await broadcast_thread_chat_event(
            thread_id,
            {"type": "pending_state_cleared", "job_id": job_id, "fields": cleared_fields},
        )

    if message:
        await broadcast_thread_chat_event(thread_id, {"type": "start"})
        await broadcast_thread_chat_event(thread_id, {"type": "token", "content": message})
        await broadcast_thread_chat_event(thread_id, {"type": "end"})

    if pending_analysis_plan is not None:
        phase = pending_analysis_plan.get("phase") or "abstract"
        plan_payload = (
            pending_analysis_plan.get("review_plan")
            if phase == "execution" and pending_analysis_plan.get("review_plan")
            else pending_analysis_plan.get("plan", [])
        )
        if phase == "execution":
            await broadcast_thread_chat_event(
                thread_id,
                {
                    "type": "execution_plan",
                    "job_id": job_id,
                    "execution_plan_summary": pending_analysis_plan.get("execution_plan_summary"),
                    "execution_confirmation_overview": pending_analysis_plan.get("execution_confirmation_overview"),
                    "execution_decision_source": pending_analysis_plan.get("execution_decision_source"),
                    "execution_ir_review": pending_analysis_plan.get("execution_ir_review"),
                    "execution_plan_delta": pending_analysis_plan.get("execution_plan_delta"),
                    "execution_plan_changes": pending_analysis_plan.get("execution_plan_changes"),
                    "execution_semantic_guardrails": pending_analysis_plan.get("execution_semantic_guardrails"),
                    "requires_confirmation": True,
                },
            )
        await broadcast_thread_chat_event(
            thread_id,
            {
                "type": "plan",
                "plan": plan_payload,
                "requires_confirmation": True,
            },
        )

    if emit_job_started:
        await broadcast_thread_chat_event(
            thread_id,
            {
                "type": "job_started",
                "job_id": job_id,
                "job_name": job_name or "analysis",
            },
        )

    await broadcast_project_task_event(job_id, reason="supervisor_sync")
    return True


async def broadcast_chat_event(event: dict[str, Any]) -> None:
    """Legacy global fan-out helper for intentionally non-threaded notifications only."""
    await broadcast_global_chat_event(event)


def _effective_auth_command(auth_req: Any) -> str:
    return (
        getattr(auth_req, "effective_command", None)
        or getattr(auth_req, "current_command_text", None)
        or getattr(auth_req, "command_text", "")
    )


async def persist_job_pending_interaction(
    job_id: str,
    interaction_type: str,
    payload: dict[str, Any],
) -> None:
    from sqlalchemy import select

    from tune.core.database import get_session_factory
    from tune.core.models import AnalysisJob

    async with get_session_factory()() as session:
        job = (
            await session.execute(select(AnalysisJob).where(AnalysisJob.id == job_id))
        ).scalar_one_or_none()
        if not job:
            log.warning(
                "persist_job_pending_interaction: job %s not found for interaction_type=%s",
                job_id,
                interaction_type,
            )
            return
        job.pending_interaction_type = interaction_type
        job.pending_interaction_payload_json = payload
        await session.commit()


async def clear_job_pending_interaction(
    job_id: str,
    *,
    interaction_type: str | None = None,
) -> None:
    from sqlalchemy import select

    from tune.core.database import get_session_factory
    from tune.core.models import AnalysisJob

    async with get_session_factory()() as session:
        job = (
            await session.execute(select(AnalysisJob).where(AnalysisJob.id == job_id))
        ).scalar_one_or_none()
        if not job:
            return
        if interaction_type and job.pending_interaction_type != interaction_type:
            return
        job.pending_interaction_type = None
        job.pending_interaction_payload_json = None
        await session.commit()


def _extract_plan_steps(plan_data: Any) -> list[dict[str, Any]]:
    if isinstance(plan_data, dict):
        steps = plan_data.get("steps", [])
        return steps if isinstance(steps, list) else []
    if isinstance(plan_data, list):
        return plan_data
    return []


def _serialize_clarification_issue(issue: Any) -> dict[str, Any]:
    data = {
        "id": getattr(issue, "id", ""),
        "kind": getattr(issue, "kind", ""),
        "severity": getattr(issue, "severity", ""),
        "title": getattr(issue, "title", ""),
        "description": getattr(issue, "description", ""),
        "suggestion": getattr(issue, "suggestion", ""),
        "resolution_type": getattr(issue, "resolution_type", ""),
        "affected_resource_ids": getattr(issue, "affected_resource_ids", []),
        "affected_step_keys": getattr(issue, "affected_step_keys", []),
        "candidates": [
            {
                "path": getattr(candidate, "path", ""),
                "organism": getattr(candidate, "organism", None),
                "confidence": getattr(candidate, "confidence", None),
                "genome_build": getattr(candidate, "genome_build", None),
                "source_type": getattr(candidate, "source_type", None),
            }
            for candidate in (getattr(issue, "candidates", None) or [])
        ],
        "details": getattr(issue, "details", {}) or {},
    }
    binding_key = getattr(issue, "binding_key", None)
    if binding_key:
        data["binding_key"] = binding_key
    legacy_issue_text = getattr(issue, "legacy_issue_text", None)
    if legacy_issue_text:
        data["legacy_issue_text"] = legacy_issue_text
    return data


def _normalize_pending_resource_interaction(
    job: Any,
    *,
    language: str = "en",
) -> tuple[dict[str, Any] | None, bool]:
    from tune.core.clarification.service import normalize_resource_clarification_payload

    normalized = normalize_resource_clarification_payload(
        job.pending_interaction_payload_json,
        language=language,
    )
    if not normalized:
        return None, False

    changed = (
        job.status != "resource_clarification_required"
        or job.pending_interaction_type != "resource_clarification"
        or job.pending_interaction_payload_json != normalized
    )
    if changed:
        job.status = "resource_clarification_required"
        job.pending_interaction_type = "resource_clarification"
        job.pending_interaction_payload_json = normalized
    return normalized, changed


async def _load_thread_rehydration_snapshot(thread_id: str) -> dict[str, Any] | None:
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload

    from tune.core.database import get_session_factory
    from tune.core.models import (
        AnalysisJob,
        CommandAuthorizationRequest,
        RepairRequest,
        SessionState,
        Thread,
    )

    async with get_session_factory()() as session:
        thread = (
            await session.execute(
                select(Thread)
                .options(selectinload(Thread.messages))
                .where(Thread.id == thread_id)
            )
        ).scalar_one_or_none()
        if not thread:
            return None

        snapshot: dict[str, Any] = {
            "project_id": thread.project_id,
            "history": [
                {"role": m.role, "content": m.content}
                for m in sorted(thread.messages, key=lambda msg: msg.created_at)[-100:]
            ],
            "session_state_id": None,
            "active_intent_revision_id": None,
            "active_capability_plan_revision_id": None,
            "progress_state": None,
            "analysis_intent_trace": None,
            "last_readiness_assessment": None,
            "last_context_acquisition": None,
            "last_decision_packet": None,
            "pending_clarification_request": None,
            "pending_plan": None,
            "pending_command_auth": None,
            "pending_error_recovery": None,
            "pending_resource_clarification": None,
        }

        try:
            persisted_session_state = (
                await session.execute(
                    select(SessionState).where(SessionState.thread_id == thread.id)
                )
            ).scalar_one_or_none()
        except Exception:
            log.exception(
                "_load_thread_rehydration_snapshot: failed to load SessionState for thread_id=%s",
                thread.id,
            )
            persisted_session_state = None
        if persisted_session_state:
            snapshot["project_id"] = snapshot.get("project_id") or getattr(
                persisted_session_state,
                "project_id",
                None,
            )
            snapshot["session_state_id"] = getattr(persisted_session_state, "id", None)
            snapshot["active_intent_revision_id"] = getattr(
                persisted_session_state,
                "active_intent_revision_id",
                None,
            )
            snapshot["active_capability_plan_revision_id"] = (
                getattr(
                    persisted_session_state,
                    "active_capability_plan_revision_id",
                    None,
                )
            )
            snapshot["progress_state"] = getattr(persisted_session_state, "progress_state_json", None)
            snapshot["analysis_intent_trace"] = getattr(
                persisted_session_state,
                "analysis_intent_trace_json",
                None,
            )
            snapshot["last_readiness_assessment"] = getattr(
                persisted_session_state,
                "latest_readiness_json",
                None,
            )
            snapshot["last_context_acquisition"] = getattr(
                persisted_session_state,
                "latest_context_acquisition_json",
                None,
            )
            snapshot["last_decision_packet"] = getattr(
                persisted_session_state,
                "pending_decision_packet_json",
                None,
            )
            snapshot["pending_clarification_request"] = getattr(
                persisted_session_state,
                "pending_clarification_request_json",
                None,
            )

        pending_plan_job = (
            await session.execute(
                select(AnalysisJob)
                .where(
                    AnalysisJob.thread_id == thread.id,
                    AnalysisJob.status.in_(["awaiting_plan_confirmation", "draft"]),
                )
                .order_by(AnalysisJob.created_at.desc())
                .limit(1)
            )
        ).scalars().first()
        if pending_plan_job:
            snapshot["project_id"] = snapshot.get("project_id") or getattr(
                pending_plan_job,
                "project_id",
                None,
            )
            plan_steps = _extract_plan_steps(
                getattr(pending_plan_job, "plan_draft_json", None)
                or getattr(pending_plan_job, "plan", None)
                or []
            )
            if plan_steps:
                from tune.core.decision_packet import (
                    select_decision_packet_for_state,
                )
                from tune.core.orchestration import (
                    summarize_expanded_dag_for_confirmation,
                )

                has_execution_plan = bool(
                    getattr(pending_plan_job, "execution_ir_json", None)
                    and getattr(pending_plan_job, "expanded_dag_json", None)
                )
                analysis_intent = (
                    (pending_plan_job.plan_draft_json or {}).get("analysis_intent")
                    if isinstance(getattr(pending_plan_job, "plan_draft_json", None), dict)
                    else None
                )
                capability_plan = (
                    (pending_plan_job.plan_draft_json or {}).get("capability_plan")
                    if isinstance(getattr(pending_plan_job, "plan_draft_json", None), dict)
                    else None
                )
                implementation_decisions = (
                    (pending_plan_job.plan_draft_json or {}).get("implementation_decisions")
                    if isinstance(getattr(pending_plan_job, "plan_draft_json", None), dict)
                    else None
                )
                decision_packet = (
                    (pending_plan_job.plan_draft_json or {}).get("decision_packet")
                    if isinstance(getattr(pending_plan_job, "plan_draft_json", None), dict)
                    else None
                )
                snapshot["pending_plan"] = {
                    "job_id": pending_plan_job.id,
                    "goal": pending_plan_job.goal or pending_plan_job.name,
                    "project_id": pending_plan_job.project_id,
                    "plan": plan_steps,
                    "phase": "execution" if has_execution_plan else "abstract",
                    "review_plan": None,
                    "execution_confirmation_overview": None,
                    "execution_decision_source": None,
                    "execution_ir_review": None,
                    "execution_plan_delta": None,
                    "execution_plan_changes": None,
                    "execution_semantic_guardrails": None,
                    "execution_plan_summary": None,
                    "short_name": pending_plan_job.name,
                    "job_backed": True,
                }
                if has_execution_plan:
                    from tune.api.routes.jobs import _serialize_execution_plan

                    execution_payload = _serialize_execution_plan(pending_plan_job)
                    snapshot["pending_plan"]["review_plan"] = summarize_expanded_dag_for_confirmation(
                        getattr(pending_plan_job, "expanded_dag_json", None)
                    )
                    snapshot["pending_plan"]["execution_confirmation_overview"] = execution_payload.get("review_overview")
                    snapshot["pending_plan"]["execution_decision_source"] = execution_payload.get("execution_decision_source")
                    snapshot["pending_plan"]["execution_ir_review"] = execution_payload.get("review_ir")
                    snapshot["pending_plan"]["execution_plan_delta"] = execution_payload.get("review_delta")
                    snapshot["pending_plan"]["execution_plan_changes"] = execution_payload.get("review_changes")
                    snapshot["pending_plan"]["execution_semantic_guardrails"] = execution_payload.get("semantic_guardrails")
                    summary = execution_payload.get("summary") or {}
                    snapshot["pending_plan"]["execution_plan_summary"] = {
                        "has_execution_ir": bool(summary.get("has_execution_ir")),
                        "has_expanded_dag": bool(summary.get("has_expanded_dag")),
                        "node_count": int(summary.get("node_count", 0) or 0),
                        "group_count": int(summary.get("group_count", 0) or 0),
                    }
                if analysis_intent is not None:
                    snapshot["pending_plan"]["analysis_intent"] = analysis_intent
                if capability_plan is not None:
                    snapshot["pending_plan"]["capability_plan"] = capability_plan
                if implementation_decisions is not None:
                    snapshot["pending_plan"]["implementation_decisions"] = implementation_decisions
                if decision_packet is None:
                    selected_packet = select_decision_packet_for_state(
                        {
                            "progress_state": snapshot.get("progress_state"),
                            "pending_analysis_plan": {
                                "active": True,
                                **snapshot["pending_plan"],
                            },
                            "last_decision_packet": snapshot.get("last_decision_packet"),
                        },
                        language=getattr(pending_plan_job, "language", "en") or "en",
                        goal=pending_plan_job.goal or pending_plan_job.name,
                        short_name=pending_plan_job.name,
                        review_plan=snapshot["pending_plan"].get("review_plan") or [],
                        execution_payload={
                            "summary": snapshot["pending_plan"].get("execution_plan_summary"),
                            "execution_decision_source": snapshot["pending_plan"].get(
                                "execution_decision_source"
                            ),
                        },
                    )
                    if selected_packet is not None:
                        decision_packet = selected_packet.model_dump(mode="json")
                if decision_packet is not None:
                    snapshot["pending_plan"]["decision_packet"] = decision_packet
        elif (
            persisted_session_state is not None
            and isinstance(persisted_session_state.pending_analysis_plan_json, dict)
            and persisted_session_state.pending_analysis_plan_json.get("active")
        ):
            from tune.core.decision_packet import select_decision_packet_for_state

            pending_plan = dict(persisted_session_state.pending_analysis_plan_json)
            pending_plan.setdefault("job_backed", False)
            pending_plan.setdefault("phase", "abstract")
            pending_plan.setdefault("review_plan", None)
            pending_plan.setdefault("execution_confirmation_overview", None)
            pending_plan.setdefault("execution_decision_source", None)
            pending_plan.setdefault("execution_ir_review", None)
            pending_plan.setdefault("execution_plan_delta", None)
            pending_plan.setdefault("execution_plan_changes", None)
            pending_plan.setdefault("execution_semantic_guardrails", None)
            pending_plan.setdefault("execution_plan_summary", None)
            if not isinstance(pending_plan.get("decision_packet"), dict):
                selected_packet = select_decision_packet_for_state(
                    {
                        "progress_state": snapshot.get("progress_state"),
                        "pending_analysis_plan": pending_plan,
                        "last_decision_packet": snapshot.get("last_decision_packet"),
                    },
                    language="en",
                    goal=pending_plan.get("goal"),
                    short_name=pending_plan.get("short_name"),
                    review_plan=pending_plan.get("review_plan"),
                    execution_payload={
                        "summary": pending_plan.get("execution_plan_summary"),
                        "execution_decision_source": pending_plan.get("execution_decision_source"),
                    },
                )
                if selected_packet is not None:
                    pending_plan["decision_packet"] = selected_packet.model_dump(mode="json")
            snapshot["pending_plan"] = pending_plan

        pending_auth_job = (
            await session.execute(
                select(AnalysisJob)
                .where(
                    AnalysisJob.thread_id == thread.id,
                    AnalysisJob.status.in_(["waiting_for_authorization", "interrupted", "running"]),
                    AnalysisJob.pending_auth_request_id.is_not(None),
                )
                .order_by(AnalysisJob.created_at.desc())
                .limit(1)
            )
        ).scalars().first()
        if pending_auth_job and pending_auth_job.pending_auth_request_id:
            snapshot["project_id"] = snapshot.get("project_id") or pending_auth_job.project_id
            auth_req = (
                await session.execute(
                    select(CommandAuthorizationRequest).where(
                        CommandAuthorizationRequest.id == pending_auth_job.pending_auth_request_id
                    )
                )
            ).scalar_one_or_none()
            if auth_req and auth_req.status == "pending":
                from tune.core.decision_packet import select_decision_packet_for_state

                snapshot["pending_command_auth"] = {
                    "job_id": pending_auth_job.id,
                    "auth_request_id": auth_req.id,
                    "command": _effective_auth_command(auth_req),
                    "command_type": auth_req.command_template_type,
                    "step": {
                        "step_key": pending_auth_job.pending_step_key,
                    },
                }
                selected_packet = select_decision_packet_for_state(
                    {
                        "progress_state": snapshot.get("progress_state"),
                        "pending_command_auth": snapshot["pending_command_auth"],
                        "last_decision_packet": snapshot.get("last_decision_packet"),
                    },
                    language=getattr(pending_auth_job, "language", "en") or "en",
                    authorization_request=snapshot["pending_command_auth"],
                )
                if selected_packet is not None:
                    snapshot["pending_command_auth"]["decision_packet"] = selected_packet.model_dump(
                        mode="json"
                    )

        pending_repair_job = (
            await session.execute(
                select(AnalysisJob)
                .where(
                    AnalysisJob.thread_id == thread.id,
                    AnalysisJob.status.in_(["waiting_for_repair", "interrupted", "running"]),
                    AnalysisJob.pending_repair_request_id.is_not(None),
                )
                .order_by(AnalysisJob.created_at.desc())
                .limit(1)
            )
        ).scalars().first()
        if pending_repair_job and pending_repair_job.pending_repair_request_id:
            snapshot["project_id"] = snapshot.get("project_id") or pending_repair_job.project_id
            repair_req = (
                await session.execute(
                    select(RepairRequest).where(
                        RepairRequest.id == pending_repair_job.pending_repair_request_id
                    )
                )
            ).scalar_one_or_none()
            if repair_req and repair_req.status == "pending":
                from tune.api.routes.jobs import _serialize_execution_plan
                from tune.core.analysis.execution_evidence import build_execution_evidence_snapshot
                from tune.core.analysis.repair_context import build_pending_repair_payload
                from tune.core.decision_packet import select_decision_packet_for_state

                execution_payload = _serialize_execution_plan(pending_repair_job)
                execution_evidence = await build_execution_evidence_snapshot(
                    session,
                    pending_repair_job,
                )
                pending_payload = build_pending_repair_payload(
                    pending_repair_job,
                    repair_request_id=repair_req.id,
                    failed_command=repair_req.failed_command or "",
                    stderr_excerpt=repair_req.stderr_excerpt or "",
                    execution_payload=execution_payload,
                    execution_evidence=execution_evidence,
                )
                repair_context = pending_payload.get("repair_context") or {}
                attempt_history = pending_payload.get("attempt_history")
                if not isinstance(attempt_history, list):
                    attempt_history = []

                snapshot["pending_error_recovery"] = {
                    "job_id": pending_repair_job.id,
                    "context": {
                        "step": pending_repair_job.pending_step_key or "",
                        "command": repair_req.failed_command or "",
                        "stderr": repair_req.stderr_excerpt or "",
                        "attempt_history": attempt_history,
                        "language": pending_repair_job.language or "en",
                        "repair_request_id": repair_req.id,
                        "repair_context": repair_context,
                    },
                }
                selected_packet = select_decision_packet_for_state(
                    {
                        "progress_state": snapshot.get("progress_state"),
                        "pending_error_recovery": {
                            "job_id": pending_repair_job.id,
                            "repair_request_id": repair_req.id,
                            "step_key": pending_repair_job.pending_step_key or "",
                            "failed_command": repair_req.failed_command or "",
                            "stderr_excerpt": repair_req.stderr_excerpt or "",
                            "repair_context": repair_context,
                        },
                        "last_decision_packet": snapshot.get("last_decision_packet"),
                    },
                    language=getattr(pending_repair_job, "language", "en") or "en",
                    repair_request={
                        "repair_request_id": repair_req.id,
                        "step_key": pending_repair_job.pending_step_key or "",
                        "failed_command": repair_req.failed_command or "",
                        "stderr_excerpt": repair_req.stderr_excerpt or "",
                        "repair_context": repair_context,
                    },
                )
                if selected_packet is not None:
                    snapshot["pending_error_recovery"]["decision_packet"] = selected_packet.model_dump(
                        mode="json"
                    )

        pending_resource_job = (
            await session.execute(
                select(AnalysisJob)
                .where(
                    AnalysisJob.thread_id == thread.id,
                    AnalysisJob.status == "resource_clarification_required",
                )
                .order_by(AnalysisJob.created_at.desc())
                .limit(1)
            )
        ).scalars().first()
        if pending_resource_job:
            snapshot["project_id"] = snapshot.get("project_id") or pending_resource_job.project_id
            normalized_payload, changed = _normalize_pending_resource_interaction(
                pending_resource_job,
                language=getattr(pending_resource_job, "language", None) or "en",
            )
            if normalized_payload:
                snapshot["pending_resource_clarification"] = normalized_payload
                if snapshot.get("pending_clarification_request") is None:
                    snapshot["pending_clarification_request"] = (
                        ((normalized_payload.get("decision_packet") or {}).get("context_payload") or {}).get(
                            "clarification_request"
                        )
                    )
                if changed:
                    commit = getattr(session, "commit", None)
                    if callable(commit):
                        await commit()
            else:
                log.warning(
                    "_load_thread_rehydration_snapshot: job %s is %s but has no normalizable persisted payload",
                    pending_resource_job.id,
                    getattr(pending_resource_job, "status", "unknown"),
                )

        return snapshot


def _apply_thread_rehydration_snapshot(
    state: dict[str, Any],
    snapshot: dict[str, Any],
) -> list[dict[str, Any]]:
    from tune.core.analysis.analysis_case import build_analysis_case_payload_from_pending_plan

    state["project_id"] = snapshot.get("project_id")
    if snapshot.get("session_state_id"):
        state["session_state_id"] = snapshot["session_state_id"]
    if snapshot.get("active_intent_revision_id"):
        state["active_intent_revision_id"] = snapshot["active_intent_revision_id"]
    if snapshot.get("active_capability_plan_revision_id"):
        state["active_capability_plan_revision_id"] = snapshot["active_capability_plan_revision_id"]
    if isinstance(snapshot.get("progress_state"), dict):
        state["progress_state"] = snapshot["progress_state"]
    if isinstance(snapshot.get("analysis_intent_trace"), dict):
        state["analysis_intent_trace"] = snapshot["analysis_intent_trace"]
    if isinstance(snapshot.get("last_readiness_assessment"), dict):
        state["last_readiness_assessment"] = snapshot["last_readiness_assessment"]
    if isinstance(snapshot.get("last_context_acquisition"), dict):
        state["last_context_acquisition"] = snapshot["last_context_acquisition"]
    if isinstance(snapshot.get("last_decision_packet"), dict):
        state["last_decision_packet"] = snapshot["last_decision_packet"]
    if isinstance(snapshot.get("pending_clarification_request"), dict):
        state["pending_clarification_request"] = snapshot["pending_clarification_request"]
    events: list[dict[str, Any]] = [
        {"type": "history", "messages": snapshot.get("history", [])}
    ]

    pending_plan = snapshot.get("pending_plan")
    if pending_plan:
        plan_payload = (
            pending_plan.get("review_plan")
            if pending_plan.get("phase") == "execution" and pending_plan.get("review_plan")
            else pending_plan.get("plan", [])
        )
        state["pending_analysis_plan"] = {
            "active": True,
            **pending_plan,
        }
        analysis_case_payload = build_analysis_case_payload_from_pending_plan(pending_plan)
        if analysis_case_payload is not None:
            state["active_analysis_case"] = analysis_case_payload
        events.append(
            {
                "type": "plan",
                "plan": plan_payload,
                "requires_confirmation": True,
            }
        )
        if pending_plan.get("phase") == "execution":
            events.append(
                {
                    "type": "execution_plan",
                    "job_id": pending_plan.get("job_id"),
                    "execution_plan_summary": pending_plan.get("execution_plan_summary"),
                    "execution_confirmation_overview": pending_plan.get("execution_confirmation_overview"),
                    "execution_decision_source": pending_plan.get("execution_decision_source"),
                    "execution_ir_review": pending_plan.get("execution_ir_review"),
                    "execution_plan_delta": pending_plan.get("execution_plan_delta"),
                    "execution_plan_changes": pending_plan.get("execution_plan_changes"),
                    "execution_semantic_guardrails": pending_plan.get("execution_semantic_guardrails"),
                    "requires_confirmation": True,
                }
            )

    pending_auth = snapshot.get("pending_command_auth")
    if pending_auth:
        job_id = pending_auth["job_id"]
        state["pending_command_auth"] = {"job_id": job_id}
        _pending_commands[job_id] = pending_auth.get("command", "")
        _pending_auth_context[job_id] = {
            "auth_request_id": pending_auth.get("auth_request_id"),
            "command_type": pending_auth.get("command_type", ""),
            "step": pending_auth.get("step", {}),
            "thread_id": state.get("thread_id"),
        }
        events.append(
            {
                "type": "command_auth",
                "command": pending_auth.get("command", ""),
                "command_type": pending_auth.get("command_type", ""),
                "job_id": job_id,
                "auth_request_id": pending_auth.get("auth_request_id"),
            }
        )

    pending_repair = snapshot.get("pending_error_recovery")
    if pending_repair:
        state["pending_error_recovery"] = pending_repair
        events.append(
            {
                "type": "error_recovery_human",
                "job_id": pending_repair.get("job_id", ""),
                **pending_repair.get("context", {}),
            }
        )

    pending_resource = snapshot.get("pending_resource_clarification")
    if pending_resource:
        state["resource_clarification"] = {
            "active": True,
            **pending_resource,
        }
        prompt_text = pending_resource.get("prompt_text")
        if prompt_text:
            events.extend(
                [
                    {"type": "start"},
                    {"type": "token", "content": prompt_text},
                    {"type": "end"},
                ]
            )

    return events


async def start_resource_clarification_chat(
    job_id: str,
    project_id: str,
    issues: list,  # list[ReadinessIssue] — avoid import cycle
    language: str = "en",
    thread_id: str | None = None,
) -> None:
    """Activate resource-clarification mode in the owning thread and stream the first issue prompt.

    Sets session_state["resource_clarification"] so that the next user message is routed to
    engine._advance_resource_clarification instead of normal intent detection.
    """
    from tune.core.clarification.service import ResourceClarificationService, render_issue_prompt
    from tune.core.decision_packet import (
        attach_decision_packet,
        build_resource_clarification_decision_packet,
    )

    if not thread_id:
        thread_id, resolved_project_id = await _load_job_route(job_id)
        project_id = project_id or resolved_project_id or ""
    if not thread_id:
        log.warning(
            "start_resource_clarification_chat: dropping resource-clarification chat for job %s because thread_id is missing",
            job_id,
        )
        await broadcast_project_task_event(
            job_id,
            reason="resource_clarification_pending",
            project_id=project_id or None,
        )
        return

    matching_states = _iter_session_states_for_thread(thread_id)
    svc = ResourceClarificationService()
    issues = await svc.prepare_issues_for_dialogue(project_id, issues)
    blocking = [issue for issue in issues if getattr(issue, "severity", "") == "blocking"]
    initial_prompt = (
        render_issue_prompt(blocking[0], language=language)
        if blocking else
        (
            "请提供完成当前分析所需的资源信息。"
            if language == "zh" else
            "Please provide the required information to continue the analysis."
        )
    )

    payload = attach_decision_packet(
        {
            "job_id": job_id,
            "project_id": project_id,
            "issues": [_serialize_clarification_issue(issue) for issue in issues],
            "context_id": job_id,
            "prompt_text": initial_prompt,
        },
        build_resource_clarification_decision_packet(
            issues=[_serialize_clarification_issue(issue) for issue in issues],
            job_id=job_id,
            project_id=project_id,
            context_id=job_id,
            language=language,
        ),
    )

    await persist_job_pending_interaction(job_id, "resource_clarification", payload)

    await broadcast_thread_chat_event(thread_id, {"type": "start"})
    try:
        async for chunk in svc.start(
            issues=issues,
            job_id=job_id,
            project_id=project_id,
            context_id=job_id,
            session_states=matching_states,
            language=language,
        ):
            if chunk.get("type") == "token":
                await broadcast_thread_chat_event(
                    thread_id,
                    {"type": "token", "content": chunk["content"]},
                )
    except Exception as e:
        log.warning("start_resource_clarification_chat: stream failed: %s", e)
        lang_zh = language == "zh"
        issue_titles = ", ".join(getattr(i, "title", str(i)) for i in issues)
        fallback = (
            f"分析任务因资源问题暂停：{issue_titles}。请提供所需信息。"
            if lang_zh else
            f"Job paused: resource issues detected ({issue_titles}). Please provide the required information."
        )
        fallback_payload = attach_decision_packet(
            {
                "job_id": job_id,
                "project_id": project_id,
                "issues": [_serialize_clarification_issue(issue) for issue in issues],
                "context_id": job_id,
                "prompt_text": fallback,
            },
            build_resource_clarification_decision_packet(
                issues=[_serialize_clarification_issue(issue) for issue in issues],
                job_id=job_id,
                project_id=project_id,
                context_id=job_id,
                language=language,
            ),
        )
        await persist_job_pending_interaction(job_id, "resource_clarification", fallback_payload)
        await broadcast_thread_chat_event(thread_id, {"type": "token", "content": fallback})
    await broadcast_thread_chat_event(thread_id, {"type": "end"})
    await broadcast_project_task_event(job_id, reason="resource_clarification_pending")


async def request_authorization(
    job_id: str, command: str, command_type: str,
    step: dict | None = None, language: str = "en",
    step_id: str | None = None,
    thread_id: str | None = None,
) -> tuple[str, bool]:
    """Ask the user to authorize a command before the worker executes it.

    Phase 1 (DB-poll): creates a CommandAuthorizationRequest record, transitions the
    job to waiting_for_authorization, broadcasts the command_auth event, then raises
    AuthorizationPendingError.  The caller (tasks.py) must catch this exception, clean
    up local state, and return — the job will be re-enqueued via defer_async once the
    user approves or rejects.

    Already-authorized command types return (command, True) immediately without blocking.
    """
    from tune.core.config import get_config

    try:
        if get_config().auto_authorize_commands:
            if command_type:
                _authorized_types.setdefault(job_id, set()).add(command_type)
            return command, True
    except RuntimeError:
        pass

    if command_type in _authorized_types.get(job_id, set()):
        return command, True

    import hashlib
    from tune.core.database import get_session_factory
    from tune.core.models import CommandAuthorizationRequest, AnalysisJob
    from tune.core.workflow import transition_job
    from sqlalchemy import select

    step_key = (step or {}).get("step_key", "") or ""
    fingerprint = hashlib.sha256(command.encode()).hexdigest()[:16]
    auth_req_id = str(_uuid_mod.uuid4())
    resolved_thread_id = thread_id

    try:
        async with get_session_factory()() as session:
            approved_req = (
                await session.execute(
                    select(CommandAuthorizationRequest).where(
                        CommandAuthorizationRequest.job_id == job_id,
                        CommandAuthorizationRequest.command_template_type == command_type,
                        CommandAuthorizationRequest.status == "approved",
                    )
                )
            ).scalars().first()
            if approved_req is not None:
                _authorized_types.setdefault(job_id, set()).add(command_type)
                return command, True

            req = CommandAuthorizationRequest(
                id=auth_req_id,
                job_id=job_id,
                step_id=step_id,
                command_text=command,
                current_command_text=command,
                command_fingerprint=fingerprint,
                command_template_type=command_type,
                revision_history_json=[],
                status="pending",
            )
            session.add(req)

            # Persist the resume context on the job so we can reload it after defer_async
            job = (await session.execute(
                select(AnalysisJob).where(AnalysisJob.id == job_id)
            )).scalar_one_or_none()
            if job:
                job.pending_auth_request_id = auth_req_id
                job.pending_step_key = step_key
                resolved_thread_id = resolved_thread_id or job.thread_id

            _tj_ok = await transition_job(job_id, "waiting_for_authorization", session)
            log.info(
                "request_authorization: transition_job result=%s job_status_now=%s for job %s",
                _tj_ok, job.status if job else "N/A", job_id,
            )
            await session.commit()
            log.info("request_authorization: commit OK — job %s status should be waiting_for_authorization", job_id)
    except Exception:
        log.exception("request_authorization: FAILED to create DB record for job %s", job_id)

    # Notify all active chat sessions that command auth is pending
    _pending_commands[job_id] = command
    _pending_auth_context[job_id] = {
        "auth_request_id": auth_req_id,
        "command_type": command_type,
        "step": step or {},
        "thread_id": resolved_thread_id,
    }
    for ss in _iter_session_states_for_thread(resolved_thread_id):
        ss["pending_command_auth"] = {"job_id": job_id}

    if resolved_thread_id:
        await broadcast_thread_chat_event(resolved_thread_id, {
            "type": "command_auth",
            "command": command,
            "command_type": command_type,
            "job_id": job_id,
            "auth_request_id": auth_req_id,
        })
    else:
        log.warning(
            "request_authorization: job %s entered waiting_for_authorization without thread_id; no chat event delivered",
            job_id,
        )

    await broadcast_project_task_event(job_id, reason="authorization_pending")

    # Raise instead of blocking — caller must catch and exit the task
    raise AuthorizationPendingError(auth_req_id)


async def activate_error_recovery(
    job_id: str,
    repair_request_id: str,
    step_name: str,
    command: str,
    stderr: str,
    attempt_history: list[dict],
    repair_context: dict[str, Any] | None = None,
    language: str = "en",
    thread_id: str | None = None,
) -> None:
    """Activate human-in-the-loop error recovery state in all chat sessions.

    Sets session_state["pending_error_recovery"] and broadcasts the
    error_recovery_human event.  Does NOT block — the worker should call this
    then return immediately.  The user's resolution is written via
    write_repair_resolution(), which triggers defer_async to resume the job.
    """
    context = {
        "step": step_name,
        "command": command,
        "stderr": stderr,
        "attempt_history": attempt_history,
        "language": language,
        "repair_request_id": repair_request_id,
        **({"repair_context": repair_context} if isinstance(repair_context, dict) and repair_context else {}),
    }

    resolved_thread_id = thread_id
    if not resolved_thread_id:
        resolved_thread_id, _ = await _load_job_route(job_id)

    for ss in _iter_session_states_for_thread(resolved_thread_id):
        ss["pending_error_recovery"] = {"job_id": job_id, "context": context}

    if resolved_thread_id:
        await broadcast_thread_chat_event(resolved_thread_id, {
            "type": "error_recovery_human",
            "job_id": job_id,
            "step": step_name,
            "command": command,
            "stderr": stderr,
            "attempt_history": attempt_history,
            "repair_request_id": repair_request_id,
            **({"repair_context": repair_context} if isinstance(repair_context, dict) and repair_context else {}),
        })
    else:
        log.warning(
            "activate_error_recovery: job %s entered waiting_for_repair without thread_id; no chat event delivered",
            job_id,
        )

    await broadcast_project_task_event(job_id, reason="repair_pending")


async def write_repair_resolution(
    job_id: str, new_command: str, should_continue: bool
) -> None:
    """Write the user's repair resolution to the pending RepairRequest and resume the job.

    Loads pending_repair_request_id from the AnalysisJob, writes human_resolution_json,
    updates status, then calls run_analysis_task.defer_async to resume execution.
    """
    from tune.core.database import get_session_factory
    from tune.core.models import AnalysisJob, RepairRequest
    from sqlalchemy import select

    resolved_thread_id: str | None = None
    try:
        async with get_session_factory()() as session:
            job = (await session.execute(
                select(AnalysisJob).where(AnalysisJob.id == job_id)
            )).scalar_one_or_none()
            resolved_thread_id = job.thread_id if job else None
            req_id = job.pending_repair_request_id if job else None

            if req_id:
                req = (await session.execute(
                    select(RepairRequest).where(RepairRequest.id == req_id)
                )).scalar_one_or_none()
                if req and req.status == "pending":
                    req.human_resolution_json = {
                        "command": new_command,
                        "should_continue": should_continue,
                    }
                    req.status = "resolved" if should_continue else "cancelled"
                    req.resolved_at = datetime.now(timezone.utc)
            await session.commit()
    except Exception:
        log.exception("write_repair_resolution: DB update failed for job %s", job_id)

    clear_chat_state_for_job("pending_error_recovery", job_id)
    _clear_thread_session_fields(resolved_thread_id, "pending_error_recovery")

    # Resume the job
    try:
        from tune.workers.tasks import run_analysis_task
        await run_analysis_task.defer_async(job_id=job_id)
    except Exception:
        log.exception("write_repair_resolution: defer_async failed for job %s", job_id)


def resolve_error_recovery(job_id: str, command: str) -> None:
    """Signal the worker with a recovery command (called from engine.py).

    Phase 1: schedules write_repair_resolution as an async task since this
    function is called from a sync context in some paths.
    """
    import asyncio
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(write_repair_resolution(job_id, command, should_continue=True))
    except RuntimeError:
        log.warning("resolve_error_recovery: no event loop for job %s", job_id)
    clear_chat_state_for_job("pending_error_recovery", job_id)


async def resolve_command_authorization(
    job_id: str,
    req_id: str,
    *,
    approved: bool,
) -> None:
    """Persist an authorization decision, clear chat mirrors, and resume the job."""
    if req_id:
        try:
            from sqlalchemy import select

            from tune.core.database import get_session_factory
            from tune.core.models import CommandAuthorizationRequest

            async with get_session_factory()() as session:
                auth_req = (
                    await session.execute(
                        select(CommandAuthorizationRequest).where(
                            CommandAuthorizationRequest.id == req_id
                        )
                    )
                ).scalar_one_or_none()
                if auth_req and auth_req.status == "pending":
                    auth_req.status = "approved" if approved else "rejected"
                    auth_req.resolved_at = datetime.now(timezone.utc)
                    if approved and auth_req.command_template_type:
                        _authorized_types.setdefault(job_id, set()).add(auth_req.command_template_type)
                await session.commit()
        except Exception:
            log.exception("Failed to update CommandAuthorizationRequest %s", req_id)

    clear_chat_state_for_job("pending_command_auth", job_id)
    _pending_commands.pop(job_id, None)
    _pending_auth_context.pop(job_id, None)

    if job_id:
        try:
            from tune.workers.defer import defer_async_with_fallback
            from tune.workers.defer import defer_async_with_fallback
            from tune.workers.tasks import run_analysis_task

            await defer_async_with_fallback(run_analysis_task, job_id=job_id)
        except Exception:
            log.exception(
                "%s_command: defer_async failed for job %s",
                "authorize" if approved else "reject",
                job_id,
            )

    await broadcast_project_task_event(
        job_id,
        reason="authorization_resolved",
    )


@router.websocket("/ws/jobs/{job_id}")
async def job_ws(websocket: WebSocket, job_id: str):
    await websocket.accept()
    _job_subscribers.setdefault(job_id, set()).add(websocket)
    try:
        # Send historical logs first
        from tune.core.database import get_session_factory
        from tune.core.models import AnalysisJob, JobLog
        from sqlalchemy import select

        async with get_session_factory()() as session:
            logs = (
                await session.execute(
                    select(JobLog).where(JobLog.job_id == job_id).order_by(JobLog.ts)
                )
            ).scalars().all()
            for lg in logs:
                await websocket.send_json({"type": "log", "stream": lg.stream, "line": lg.line})

            job = (
                await session.execute(select(AnalysisJob).where(AnalysisJob.id == job_id))
            ).scalar_one_or_none()
            if job:
                await websocket.send_json({"type": "status", "status": job.status})

        # Keep alive
        while True:
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=30)
            except asyncio.TimeoutError:
                await websocket.send_json({"type": "ping"})
    except WebSocketDisconnect:
        pass
    finally:
        _job_subscribers.get(job_id, set()).discard(websocket)


@router.websocket("/ws/chat")
async def chat_ws(websocket: WebSocket, thread_id: str | None = None):
    """Main chat WebSocket — receives user messages, streams AI responses."""
    await websocket.accept()
    session_state: dict[str, Any] = {
        "conversation_id": None,
        "project_id": None,
        "authorized_commands": set(),
        "thread_id": thread_id,
        "_last_user_text": None,
    }
    _register_chat_session(websocket, session_state)

    # Send thread history if thread_id provided
    if thread_id:
        try:
            snapshot = await _load_thread_rehydration_snapshot(thread_id)
            if snapshot:
                for event in _apply_thread_rehydration_snapshot(session_state, snapshot):
                    await websocket.send_json(event)
                _set_chat_socket_project(websocket, session_state.get("project_id"))
        except Exception:
            log.exception("Failed to rehydrate thread session for thread_id=%s", thread_id)

    try:
        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)
            msg_type = msg.get("type", "chat")

            if msg_type == "chat":
                await _handle_chat(websocket, msg, session_state)
            elif msg_type == "authorize_command":
                job_id = msg.get("job_id", "")
                req_id = msg.get("auth_request_id", "")
                await resolve_command_authorization(job_id, req_id, approved=True)
            elif msg_type == "reject_command":
                job_id = msg.get("job_id", "")
                req_id = msg.get("auth_request_id", "")
                await resolve_command_authorization(job_id, req_id, approved=False)
            elif msg_type == "resolve_error_recovery":
                # Frontend sends a specific command to retry with
                job_id = msg.get("job_id", "")
                new_command = msg.get("command", "")
                if job_id:
                    await write_repair_resolution(job_id, new_command, should_continue=True)
            elif msg_type == "terminate_error_recovery":
                # User clicked "Stop Job"
                job_id = msg.get("job_id", "")
                if job_id:
                    await write_repair_resolution(job_id, "", should_continue=False)
            elif msg_type == "save_memory":
                # User confirmed saving a recovery approach to GlobalMemory
                trigger = msg.get("trigger", "")
                approach = msg.get("approach", "")
                if trigger and approach:
                    try:
                        from tune.core.database import get_session_factory
                        from tune.core.memory.global_memory import write_user_memory
                        async with get_session_factory()() as _session:
                            await write_user_memory(_session, trigger, approach)
                    except Exception:
                        log.exception("Failed to save user memory")
            elif msg_type == "set_project":
                new_project_id = msg.get("project_id")
                session_state["project_id"] = new_project_id
                _set_chat_socket_project(websocket, new_project_id)
                thread_payload = None
                if session_state.get("thread_id"):
                    try:
                        thread_payload = await _sync_thread_project(session_state.get("thread_id"), new_project_id)
                    except Exception:
                        log.exception(
                            "Failed to sync thread project for thread_id=%s",
                            session_state.get("thread_id"),
                        )
                # Carry language preference sent alongside set_project (before health prompt)
                if "language" in msg:
                    session_state["language"] = msg["language"]
                await websocket.send_json({"type": "project_set", "project_id": new_project_id})
                if thread_payload is not None:
                    await websocket.send_json({"type": "thread_bound", "thread": thread_payload})
            elif msg_type == "confirm_plan":
                await _handle_confirm_plan(websocket, msg, session_state)
            elif msg_type == "set_current_skill":
                session_state["current_skill_id"] = msg.get("skill_id")
                await websocket.send_json({"type": "skill_set", "skill_id": msg.get("skill_id")})

    except WebSocketDisconnect:
        pass
    finally:
        _unregister_chat_session(websocket, session_state)


async def _handle_confirm_plan(websocket: WebSocket, msg: dict, state: dict) -> None:
    """Handle confirm_plan WebSocket messages — create a job or cancel the pending plan."""
    from tune.core.analysis.persistence import persist_session_snapshot

    lang_zh = state.get("language") == "zh"

    # Check for pending skill edit first
    pending_edit = state.get("pending_skill_edit", {})
    if pending_edit.get("active"):
        state.pop("pending_skill_edit", None)
        if msg.get("confirm"):
            skill_id = pending_edit["skill_id"]
            new_steps = pending_edit["new_steps"]
            try:
                from tune.core.database import get_session_factory
                from tune.core.models import Skill
                from tune.core.skills.registry import create_new_version
                from sqlalchemy import select

                class _StepsUpdate:
                    def __init__(self, steps):
                        self.steps = steps
                        self.input_params = None
                        self.pixi_toml = None
                        self.tags = None

                async with get_session_factory()() as session:
                    s = (await session.execute(select(Skill).where(Skill.id == skill_id))).scalar_one_or_none()
                    if s:
                        await create_new_version(session, s, _StepsUpdate(new_steps))
                        await session.commit()
                confirm_msg = (
                    "技能已更新，新版本已创建。"
                    if lang_zh
                    else "Skill updated — a new version has been saved."
                )
            except Exception as e:
                log.exception("Failed to update skill")
                confirm_msg = (
                    f"更新技能失败：{e}" if lang_zh else f"Failed to update skill: {e}"
                )
        else:
            confirm_msg = (
                "已取消技能修改。" if lang_zh else "Skill edit cancelled."
            )
        await websocket.send_json({"type": "start"})
        await websocket.send_json({"type": "token", "content": confirm_msg})
        await websocket.send_json({"type": "end"})
        return

    pending = state.get("pending_analysis_plan", {})
    if not pending.get("active"):
        return
    phase = pending.get("phase") or "abstract"

    async def _revalidate_pending_plan() -> tuple[list[dict], list[Any]]:
        from tune.core.context.builder import PlannerContextBuilder
        from tune.core.context.models import ContextScope
        from tune.core.database import get_session_factory
        from tune.core.resources.planner_adapter import enforce_planner_constraints

        project_id = pending.get("project_id") or state.get("project_id")
        if not project_id:
            return list(pending.get("plan", [])), []

        async with get_session_factory()() as session:
            planner_context = await PlannerContextBuilder(session).build(
                ContextScope(project_id=project_id)
            )
        result = enforce_planner_constraints(pending.get("plan", []), planner_context)
        return result.amended_plan, result.issues

    async def _persist_pending_plan(plan: list[dict]) -> None:
        if not (pending.get("job_backed") and pending.get("job_id")):
            return
        from sqlalchemy import select

        from tune.core.database import get_session_factory
        from tune.core.models import AnalysisJob

        async with get_session_factory()() as session:
            job = (
                await session.execute(
                    select(AnalysisJob).where(AnalysisJob.id == pending["job_id"])
                )
            ).scalar_one_or_none()
            if not job:
                return
            existing_draft = job.plan_draft_json or {}
            if isinstance(existing_draft, dict):
                existing_draft = dict(existing_draft)
                existing_draft["steps"] = plan
            else:
                existing_draft = {"steps": plan}
            if pending.get("analysis_intent") is not None:
                existing_draft["analysis_intent"] = pending["analysis_intent"]
            if pending.get("capability_plan") is not None:
                existing_draft["capability_plan"] = pending["capability_plan"]
            if pending.get("implementation_decisions") is not None:
                existing_draft["implementation_decisions"] = pending["implementation_decisions"]
            if pending.get("decision_packet") is not None:
                existing_draft["decision_packet"] = pending["decision_packet"]
            job.plan_draft_json = existing_draft
            await session.commit()

    async def _prepare_execution_confirmation(
        *,
        plan: list[dict],
        existing_job_id: str | None = None,
        goal: str = "",
        project_id: str | None = None,
        short_name: str = "analysis",
    ) -> tuple[str, dict[str, Any], list[dict[str, Any]]]:
        from sqlalchemy import select

        from tune.api.routes.jobs import _serialize_execution_plan
        from tune.core.analysis.progress_state import derive_progress_state
        from tune.core.config import get_config
        from tune.core.database import get_session_factory
        from tune.core.decision_packet import (
            attach_decision_packet,
            select_decision_packet_for_state,
        )
        from tune.core.job_output_paths import build_output_dir_path
        from tune.core.models import AnalysisJob, Project
        from tune.core.orchestration import (
            materialize_job_execution_plan,
            summarize_expanded_dag_for_confirmation,
        )
        from tune.core.registry.spec_generation import augment_plan_with_dynamic_specs
        from tune.core.workflow import transition_job

        plan, dynamic_issues = await augment_plan_with_dynamic_specs(
            plan,
            context_hint=f"Goal: {goal}\nProject ID: {project_id or ''}",
        )
        if dynamic_issues:
            raise RuntimeError("; ".join(dynamic_issues))

        async with get_session_factory()() as session:
            project_name = "default"
            if project_id and hasattr(session, "execute"):
                project = (
                    await session.execute(select(Project).where(Project.id == project_id))
                ).scalar_one_or_none()
                if project and getattr(project, "name", None):
                    project_name = project.name
            job = None
            if existing_job_id:
                job = (
                    await session.execute(
                        select(AnalysisJob).where(AnalysisJob.id == existing_job_id)
                    )
                ).scalar_one_or_none()
                if job and job.project_id and not project_id and hasattr(session, "execute"):
                    project = (
                        await session.execute(select(Project).where(Project.id == job.project_id))
                    ).scalar_one_or_none()
                    if project and getattr(project, "name", None):
                        project_name = project.name
            if job is None:
                created_at = datetime.now(tz=timezone.utc)
                job = AnalysisJob(
                    id=str(_uuid_mod.uuid4()),
                    thread_id=state.get("thread_id"),
                    project_id=project_id,
                    name=short_name,
                    goal=goal,
                    plan=plan,
                    status="draft",
                    created_at=created_at,
                    env_status="pending",
                    language=state.get("language", "en"),
                    output_dir=str(
                        build_output_dir_path(
                            get_config().analysis_dir,
                            project_name,
                            short_name,
                            created_at=created_at,
                        )
                    ),
                )
                session.add(job)
                if hasattr(session, "flush"):
                    await session.flush()
            elif not job.output_dir:
                job.output_dir = str(
                    build_output_dir_path(
                        get_config().analysis_dir,
                        project_name,
                        job.name,
                        created_at=job.created_at or datetime.now(tz=timezone.utc),
                    )
                )

            draft_payload = job.plan_draft_json or {}
            if isinstance(draft_payload, dict):
                draft_payload = dict(draft_payload)
                draft_payload["steps"] = plan
            else:
                draft_payload = {"steps": plan}
            if pending.get("analysis_intent") is not None:
                draft_payload["analysis_intent"] = pending["analysis_intent"]
            if pending.get("capability_plan") is not None:
                draft_payload["capability_plan"] = pending["capability_plan"]
            if pending.get("implementation_decisions") is not None:
                draft_payload["implementation_decisions"] = pending["implementation_decisions"]
            if pending.get("decision_packet") is not None:
                draft_payload["decision_packet"] = pending["decision_packet"]
            job.plan_draft_json = draft_payload
            job.plan = plan
            job.session_state_id = state.get("session_state_id")
            job.intent_revision_id = (
                pending.get("intent_revision_id") or state.get("active_intent_revision_id")
            )
            job.capability_plan_revision_id = (
                pending.get("capability_plan_revision_id")
                or state.get("active_capability_plan_revision_id")
            )
            await materialize_job_execution_plan(session, job, draft_payload)
            if state.get("thread_id") and not job.thread_id:
                job.thread_id = state["thread_id"]
            if job.status == "draft":
                ok = await transition_job(job.id, "awaiting_plan_confirmation", session)
                if not ok:
                    raise RuntimeError(
                        f"Cannot move job '{job.id}' to awaiting_plan_confirmation from '{job.status}'"
                    )

            execution_payload = _serialize_execution_plan(job)
            review_plan = summarize_expanded_dag_for_confirmation(job.expanded_dag_json)
            pending_plan_state = {
                "progress_state": derive_progress_state(
                    {
                        "progress_state": state.get("progress_state"),
                        "pending_analysis_plan": {
                            "active": True,
                            "phase": "execution",
                            "goal": goal or getattr(job, "goal", "") or job.name,
                            "short_name": job.name,
                            "analysis_case_id": pending.get("analysis_case_id"),
                            "review_plan": review_plan,
                            "execution_plan_summary": execution_payload.get("summary"),
                            "execution_decision_source": execution_payload.get(
                                "execution_decision_source"
                            ),
                        },
                    }
                ).model_dump(mode="json"),
                "pending_analysis_plan": {
                    "active": True,
                    "phase": "execution",
                    "goal": goal or getattr(job, "goal", "") or job.name,
                    "short_name": job.name,
                    "analysis_case_id": pending.get("analysis_case_id"),
                    "review_plan": review_plan,
                    "execution_plan_summary": execution_payload.get("summary"),
                    "execution_decision_source": execution_payload.get("execution_decision_source"),
                },
            }
            execution_decision_packet = select_decision_packet_for_state(
                pending_plan_state,
                goal=goal or getattr(job, "goal", "") or job.name,
                short_name=job.name,
                review_plan=review_plan,
                execution_payload=execution_payload,
                language=state.get("language", "en"),
            )
            if execution_decision_packet is None:
                raise RuntimeError("Execution confirmation decision packet could not be selected")
            if isinstance(job.plan_draft_json, dict):
                updated_draft = dict(job.plan_draft_json)
                updated_draft["decision_packet"] = execution_decision_packet.model_dump(mode="json")
                job.plan_draft_json = updated_draft
            job.pending_interaction_type = "execution_confirmation"
            job.pending_interaction_payload_json = attach_decision_packet(
                {
                    "phase": "execution",
                    "prompt_text": "Execution graph is ready for final confirmation.",
                    "execution_plan_summary": execution_payload["summary"],
                    "execution_decision_source": execution_payload.get("execution_decision_source"),
                },
                execution_decision_packet,
            )
            await session.commit()
            return job.id, execution_payload, review_plan

    if msg.get("confirm"):
        if phase == "execution":
            if not pending.get("job_backed") or not pending.get("job_id"):
                await websocket.send_json({"type": "start"})
                await websocket.send_json({
                    "type": "token",
                    "content": (
                        "执行图确认状态丢失，无法继续。请重新发起分析。"
                        if lang_zh
                        else "Execution-plan confirmation context was lost. Start the analysis again."
                    ),
                })
                await websocket.send_json({"type": "end"})
                state.pop("pending_analysis_plan", None)
                state.pop("last_decision_packet", None)
                await persist_session_snapshot(
                    state,
                    thread_id=state.get("thread_id") or "",
                    project_id=state.get("project_id"),
                    event_type="execution_confirmation_lost",
                    clear_pending_analysis_plan=True,
                )
                return

            from sqlalchemy import select

            from tune.core.database import get_session_factory
            from tune.core.models import AnalysisJob
            from tune.api.routes.jobs import _serialize_execution_plan
            from tune.core.analysis.analysis_case import update_active_analysis_case
            from tune.core.workflow import transition_job
            from tune.workers.defer import defer_async_with_fallback
            from tune.workers.tasks import run_analysis_task

            job_id = pending["job_id"]
            job_name = pending.get("short_name") or pending.get("goal") or "analysis"
            update_active_analysis_case(
                state,
                status="running",
                current_stage="execution",
                note="execution_confirmed",
            )
            state.pop("pending_analysis_plan", None)
            state.pop("last_decision_packet", None)
            await persist_session_snapshot(
                state,
                thread_id=state.get("thread_id") or "",
                project_id=state.get("project_id"),
                event_type="execution_confirmed",
                clear_pending_analysis_plan=True,
            )

            try:
                async with get_session_factory()() as session:
                    job = (
                        await session.execute(
                            select(AnalysisJob).where(AnalysisJob.id == job_id)
                        )
                    ).scalar_one_or_none()
                    if not job:
                        raise RuntimeError(f"Job '{job_id}' not found")

                    ok = await transition_job(job_id, "queued", session)
                    if not ok:
                        raise RuntimeError(f"Cannot queue job '{job_id}' from status '{job.status}'")
                    execution_plan_summary = _serialize_execution_plan(job)["summary"]
                    await session.commit()
                    job_name = job.name or job_name

                await defer_async_with_fallback(run_analysis_task, job_id=job_id)
                await broadcast_project_task_event(job_id, reason="execution_confirmed")
                await websocket.send_json({
                    "type": "job_started",
                    "job_id": job_id,
                    "job_name": job_name,
                    "execution_plan_summary": execution_plan_summary,
                })
                confirm_msg = (
                    f"好的，执行图已确认，开始执行分析（任务 ID: `{job_id}`）。请在任务监控面板查看进度。"
                    if lang_zh
                    else f"Execution graph confirmed. Analysis started (job ID: `{job_id}`). Check the task monitor for progress."
                )
            except Exception as e:
                log.exception("Failed to queue analysis after execution confirmation")
                confirm_msg = (
                    f"启动分析失败：{e}" if lang_zh else f"Failed to start analysis: {e}"
                )

            await websocket.send_json({"type": "start"})
            await websocket.send_json({"type": "token", "content": confirm_msg})
            await websocket.send_json({"type": "end"})
            return

        amended_plan, feasibility_issues = await _revalidate_pending_plan()
        if amended_plan != pending.get("plan", []):
            from tune.core.analysis.engine import _format_plan
            from tune.core.analysis.progress_state import derive_progress_state
            from tune.core.decision_packet import select_decision_packet_for_state

            pending["plan"] = amended_plan
            state["pending_analysis_plan"]["plan"] = amended_plan
            state["progress_state"] = derive_progress_state(state).model_dump(mode="json")
            refreshed_packet = select_decision_packet_for_state(
                state,
                language=state.get("language", "en"),
            )
            if refreshed_packet is not None:
                state["pending_analysis_plan"]["decision_packet"] = refreshed_packet.model_dump(mode="json")
                state["last_decision_packet"] = state["pending_analysis_plan"]["decision_packet"]
            await _persist_pending_plan(amended_plan)
            await persist_session_snapshot(
                state,
                thread_id=state.get("thread_id") or "",
                project_id=state.get("project_id"),
                event_type="abstract_plan_revalidated",
            )
            plan_text = _format_plan(amended_plan)
            rewrite_msg = (
                f"我根据当前资源状态修正了执行计划，请再次确认：\n\n{plan_text}"
                if lang_zh
                else f"I adjusted the plan to match the current project resources. Please confirm the updated plan:\n\n{plan_text}"
            )
            await websocket.send_json({"type": "start"})
            await websocket.send_json({"type": "token", "content": rewrite_msg})
            await websocket.send_json({"type": "plan", "plan": amended_plan, "requires_confirmation": True})
            await websocket.send_json({"type": "end"})
            return
        if feasibility_issues:
            issue_lines = "\n".join(
                f"- {issue.title}: {issue.suggestion or issue.description}"
                for issue in feasibility_issues
            )
            reject_msg = (
                "当前计划在现有项目状态下仍不可执行，我不会直接启动任务：\n\n"
                f"{issue_lines}\n\n请先补齐资源或修改计划。"
                if lang_zh
                else "This plan is still not executable under the current project state, so I will not start the job:\n\n"
                     f"{issue_lines}\n\nProvide the missing resources or modify the plan first."
            )
            await websocket.send_json({"type": "start"})
            await websocket.send_json({"type": "token", "content": reject_msg})
            await websocket.send_json({"type": "end"})
            return

        try:
            from tune.core.analysis.engine import _format_plan
            from tune.core.analysis.progress_state import derive_progress_state
            from tune.core.analysis.analysis_case import update_active_analysis_case
            from tune.core.decision_packet import select_decision_packet_for_state

            job_id, execution_payload, review_plan = await _prepare_execution_confirmation(
                plan=amended_plan,
                existing_job_id=pending.get("job_id"),
                goal=pending.get("goal", ""),
                project_id=pending.get("project_id") or state.get("project_id"),
                short_name=pending.get("short_name") or pending.get("goal") or "analysis",
            )
            state["pending_analysis_plan"] = {
                **pending,
                "active": True,
                "job_backed": True,
                "job_id": job_id,
                "plan": amended_plan,
                "phase": "execution",
                "review_plan": review_plan,
                "execution_plan_summary": execution_payload["summary"],
                "execution_confirmation_overview": execution_payload.get("review_overview"),
                "execution_decision_source": execution_payload.get("execution_decision_source"),
                "execution_ir_review": execution_payload.get("review_ir"),
                "execution_plan_delta": execution_payload.get("review_delta"),
                "execution_plan_changes": execution_payload.get("review_changes"),
                "execution_semantic_guardrails": execution_payload.get("semantic_guardrails"),
            }
            update_active_analysis_case(
                state,
                status="pending_execution_confirmation",
                current_stage="execution_plan",
                note="execution_plan_pending_confirmation",
            )
            state["progress_state"] = derive_progress_state(state).model_dump(mode="json")
            refreshed_packet = select_decision_packet_for_state(
                state,
                goal=pending.get("goal", ""),
                short_name=pending.get("short_name") or "",
                review_plan=review_plan,
                execution_payload=execution_payload,
                language=state.get("language", "en"),
            )
            if refreshed_packet is not None:
                state["pending_analysis_plan"]["decision_packet"] = refreshed_packet.model_dump(mode="json")
                state["last_decision_packet"] = state["pending_analysis_plan"]["decision_packet"]
            await persist_session_snapshot(
                state,
                thread_id=state.get("thread_id") or "",
                project_id=state.get("project_id"),
                event_type="execution_plan_pending_confirmation",
            )
            await broadcast_project_task_event(job_id, reason="awaiting_execution_confirmation")
            execution_plan_text = (
                "下面是最终执行图的分组视图。请再次确认；你也可以继续用自然语言修改分析步骤，系统会重新编排。\n\n"
                f"{_format_plan(review_plan)}"
                if lang_zh
                else "Below is the grouped execution graph. Confirm once more to start, or continue editing the analysis plan in natural language and I will re-orchestrate it.\n\n"
                     f"{_format_plan(review_plan)}"
            )
            await websocket.send_json({"type": "start"})
            await websocket.send_json({"type": "token", "content": execution_plan_text})
            await websocket.send_json({
                "type": "execution_plan",
                "job_id": job_id,
                "execution_plan": execution_payload,
                "execution_confirmation_overview": execution_payload.get("review_overview"),
                "execution_decision_source": execution_payload.get("execution_decision_source"),
                "execution_ir_review": execution_payload.get("review_ir"),
                "execution_plan_delta": execution_payload.get("review_delta"),
                "execution_plan_changes": execution_payload.get("review_changes"),
                "execution_semantic_guardrails": execution_payload.get("semantic_guardrails"),
                "requires_confirmation": True,
            })
            await websocket.send_json({
                "type": "plan",
                "plan": review_plan,
                "requires_confirmation": True,
            })
            await websocket.send_json({"type": "end"})
        except Exception as e:
            log.exception("Failed to prepare execution confirmation")
            await websocket.send_json({"type": "start"})
            await websocket.send_json({
                "type": "token",
                "content": f"执行图生成失败：{e}" if lang_zh else f"Failed to prepare execution graph: {e}",
            })
            await websocket.send_json({"type": "end"})
    else:
        # User cancelled
        if pending.get("job_backed") and pending.get("job_id"):
            try:
                from sqlalchemy import select

                from tune.core.database import get_session_factory
                from tune.core.models import AnalysisJob
                from tune.core.workflow import transition_job

                async with get_session_factory()() as session:
                    job = (
                        await session.execute(
                            select(AnalysisJob).where(AnalysisJob.id == pending["job_id"])
                        )
                    ).scalar_one_or_none()
                    if job:
                        await transition_job(job.id, "cancelled", session)
                        await session.commit()
            except Exception:
                log.exception(
                    "Failed to cancel job-backed pending analysis plan for job %s",
                    pending.get("job_id"),
                )

        state.pop("pending_analysis_plan", None)
        state.pop("last_decision_packet", None)
        await persist_session_snapshot(
            state,
            thread_id=state.get("thread_id") or "",
            project_id=state.get("project_id"),
            event_type="analysis_cancelled",
            clear_pending_analysis_plan=True,
        )
        cancel_msg = (
            "已取消分析。如需重新开始，请再次描述您的分析需求。"
            if lang_zh
            else "Analysis cancelled. Describe your analysis goal again whenever you're ready."
        )
        await websocket.send_json({"type": "start"})
        await websocket.send_json({"type": "token", "content": cancel_msg})
        await websocket.send_json({"type": "end"})


async def _handle_chat(websocket: WebSocket, msg: dict, state: dict) -> None:
    from tune.core.analysis.engine import handle_chat_message
    user_text = msg.get("content", "")
    if not user_text.strip():
        return

    async def _safe_send(payload: dict[str, Any]) -> bool:
        try:
            await websocket.send_json(payload)
            return True
        except (WebSocketDisconnect, RuntimeError):
            return False

    # Propagate language preference into session state
    if "language" in msg:
        state["language"] = msg["language"]

    # Propagate project_id from message into session state (D6 fix)
    if msg.get("project_id") is not None:
        state["project_id"] = msg["project_id"]
        _set_chat_socket_project(websocket, state.get("project_id"))

    thread_payload = None
    if not state.get("thread_id"):
        try:
            thread_payload = await _ensure_thread_for_session(
                websocket,
                state,
                initial_user_text=user_text,
            )
        except Exception:
            log.exception("Failed to ensure thread for chat session")
    elif msg.get("project_id") is not None:
        try:
            thread_payload = await _sync_thread_project(state.get("thread_id"), state.get("project_id"))
        except Exception:
            log.exception(
                "Failed to sync thread project during chat for thread_id=%s",
                state.get("thread_id"),
            )

    state["_prev_user_text"] = state.get("_last_user_text")
    state["_prev_assistant_text"] = state.get("_last_assistant_text")
    state["_last_user_text"] = user_text

    if thread_payload is not None:
        if not await _safe_send({"type": "thread_bound", "thread": thread_payload}):
            return

    if not await _safe_send({"type": "start"}):
        return
    assistant_chunks: list[str] = []
    try:
        async for chunk in handle_chat_message(user_text, state):
            if not await _safe_send(chunk):
                return
            if chunk.get("type") == "token":
                assistant_chunks.append(chunk.get("content", ""))
    except Exception as e:
        log.exception("Chat error")
        if not await _safe_send({"type": "error", "message": str(e)}):
            return
    finally:
        await _safe_send({"type": "end"})

    # Persist to thread if thread_id is set
    thread_id = state.get("thread_id")
    if assistant_chunks:
        assistant_content = "".join(assistant_chunks)
        state["_last_assistant_text"] = assistant_content
    if thread_id and assistant_chunks:
        try:
            from tune.core.database import get_session_factory
            from tune.core.models import Thread, ThreadMessage
            from sqlalchemy import select
            import uuid as _uuid_mod

            async with get_session_factory()() as session:
                thread = (
                    await session.execute(select(Thread).where(Thread.id == thread_id))
                ).scalar_one_or_none()
                if thread:
                    # Auto-title from first user message
                    if not thread.title:
                        title = user_text[:40]
                        if len(user_text) > 40:
                            title += "…"
                        thread.title = title

                    session.add(ThreadMessage(
                        id=str(_uuid_mod.uuid4()),
                        thread_id=thread_id,
                        role="user",
                        content=user_text,
                    ))
                    session.add(ThreadMessage(
                        id=str(_uuid_mod.uuid4()),
                        thread_id=thread_id,
                        role="assistant",
                        content=assistant_content,
                    ))
                    await session.commit()
        except Exception:
            log.exception("Failed to persist messages for thread_id=%s", thread_id)
