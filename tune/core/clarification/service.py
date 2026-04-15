"""ResourceClarificationService — semantic resource clarification dialogue.

Provides a structured, resource-semantic dialogue with ClarificationAction
structured output for clean intent extraction from user replies.

Session state schema:
    session_state["resource_clarification"] = {
        "active": bool,
        "job_id": str,
        "project_id": str,
        "issues": list[ReadinessIssue],   # JSON-serializable dicts
        "context_id": str,
    }
"""
from __future__ import annotations

import logging
import os
from collections.abc import AsyncGenerator
import json as _json
import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from tune.core.binding.semantic_retrieval import retrieve_semantic_candidates
from tune.core.context.builder import PlannerContextBuilder
from tune.core.context.models import ContextScope
from tune.core.database import get_session_factory
from tune.core.llm.gateway import LLMMessage, get_gateway
from tune.core.metadata.sample_inference import _detect_read_number
from tune.core.models import AnalysisJob, Experiment, FileRun, KnownPath
from tune.core.registry.steps import SlotDefinition
from tune.core.resources.graph_builder import ResourceGraphBuilder
from tune.core.resources.models import (
    ReadinessIssue,
    ReadinessReport,
    ResourceCandidate,
    ResourceGraph,
)
from tune.core.resources.readiness import ReadinessChecker

log = logging.getLogger(__name__)

_ISSUE_KIND_TO_KNOWN_PATH_KEY = {
    "missing_reference": "reference_fasta",
    "ambiguous_reference": "reference_fasta",
    "missing_annotation": "annotation_gtf",
    "ambiguous_annotation": "annotation_gtf",
}

# ---------------------------------------------------------------------------
# Structured output schema for ClarificationAction extraction
# ---------------------------------------------------------------------------

CLARIFICATION_ACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "action_type": {
            "type": "string",
            "enum": [
                "select_candidate",
                "provide_path",
                "confirm_auto_build",
                "link_experiment",
                "unknown",
            ],
            "description": "The type of clarification action the user is taking.",
        },
        "issue_id": {
            "type": "string",
            "description": "ID of the issue being resolved.",
        },
        "candidate_index": {
            "type": "integer",
            "description": "0-based index of the selected candidate (for select_candidate actions).",
        },
        "path": {
            "type": "string",
            "description": "Filesystem path provided by the user (for provide_path actions).",
        },
        "confirmed": {
            "type": "boolean",
            "description": "Whether the user confirmed an auto-build action.",
        },
        "experiment_id": {
            "type": "string",
            "description": "Experiment ID selected for a read-linkage clarification.",
        },
        "file_id": {
            "type": "string",
            "description": "File ID selected for a read-linkage clarification.",
        },
        "read_number": {
            "type": "integer",
            "description": "Read number for paired-end linkage: 1 or 2.",
        },
        "read_role": {
            "type": "string",
            "enum": ["R1", "R2", "single"],
            "description": "Explicit read role confirmation for read-linkage clarification.",
        },
    },
    "required": ["action_type"],
}


def _issue_prompt_en(issue: ReadinessIssue) -> str:
    """Generate user-facing prompt text for a blocking issue."""
    prompt = f"**{issue.title}**\n{issue.description}"
    if issue.resolution_type == "link_experiment":
        prompt += _render_link_experiment_prompt(issue, language="en")
    elif issue.candidates:
        prompt += "\n\nPlease select one of the following candidates:"
        for i, c in enumerate(issue.candidates):
            label = c.path
            if c.organism:
                label += f" [{c.organism}"
                if c.genome_build:
                    label += f", {c.genome_build}"
                label += "]"
            prompt += f"\n  {i + 1}. {label}"
        prompt += "\n\nReply with the number of your choice, or type a different path."
    elif issue.resolution_type == "confirm_auto_build":
        prompt += "\n\nWould you like to proceed with the automatic build? (yes/no)"
    elif issue.resolution_type == "provide_path":
        prompt += f"\n\n{issue.suggestion}"
    return prompt


def _issue_prompt_zh(issue: ReadinessIssue) -> str:
    """Generate Chinese user-facing prompt text for a blocking issue."""
    prompt = f"**{issue.title}**\n{issue.description}"
    if issue.resolution_type == "link_experiment":
        prompt += _render_link_experiment_prompt(issue, language="zh")
    elif issue.candidates:
        prompt += "\n\n请从以下候选项中选择一个："
        for i, c in enumerate(issue.candidates):
            label = c.path
            if c.organism:
                label += f"【{c.organism}"
                if c.genome_build:
                    label += f"，{c.genome_build}"
                label += "】"
            prompt += f"\n  {i + 1}. {label}"
        prompt += "\n\n请回复对应的编号，或输入其他路径。"
    elif issue.resolution_type == "confirm_auto_build":
        prompt += "\n\n是否允许系统自动构建？（是/否）"
    elif issue.resolution_type == "provide_path":
        prompt += f"\n\n{issue.suggestion}"
    return prompt


def render_issue_prompt(issue: ReadinessIssue, language: str = "en") -> str:
    """Maps IssueKind → human-readable prompt string in EN/ZH."""
    if language == "zh":
        return _issue_prompt_zh(issue)
    return _issue_prompt_en(issue)


def _render_link_experiment_prompt(issue: ReadinessIssue, *, language: str) -> str:
    details = issue.details or {}
    files = details.get("files") or []

    if language == "zh":
        if files:
            lines = ["\n\n待链接的 FASTQ 文件："]
            for idx, file_info in enumerate(files, start=1):
                label = file_info.get("filename") or file_info.get("path") or file_info.get("file_id")
                suggestion = _read_role_hint(file_info, language="zh")
                lines.append(f"  {idx}. {label}{suggestion}")
            prompt = "\n".join(lines)
        else:
            prompt = ""

        if issue.candidates:
            prompt += "\n\n请选择要归属的实验："
            for i, c in enumerate(issue.candidates):
                prompt += f"\n  {i + 1}. {c.path}"
            prompt += (
                "\n\n请回复实验编号。"
                "如果需要指定读段方向，可回复“1，R1”或“1，R2”。"
            )
            return prompt

        return (
            prompt
            + "\n\n当前项目还没有可用的实验记录，无法完成 FASTQ 归属。"
            "请先在 Samples / Experiments 中创建实验，再继续。"
        )

    if files:
        lines = ["\n\nFASTQ file to link:"]
        for idx, file_info in enumerate(files, start=1):
            label = file_info.get("filename") or file_info.get("path") or file_info.get("file_id")
            suggestion = _read_role_hint(file_info, language="en")
            lines.append(f"  {idx}. {label}{suggestion}")
        prompt = "\n".join(lines)
    else:
        prompt = ""

    if issue.candidates:
        prompt += "\n\nSelect the target experiment:"
        for i, c in enumerate(issue.candidates):
            prompt += f"\n  {i + 1}. {c.path}"
        prompt += (
            "\n\nReply with the experiment number."
            " If the read role must be specified, reply like '1, R1' or '1, R2'."
        )
        return prompt

    return (
        prompt
        + "\n\nNo experiment exists in this project yet, so this FASTQ cannot be linked here."
        " Create an experiment first, then continue."
    )


def _read_role_hint(file_info: dict, *, language: str) -> str:
    suggested = file_info.get("suggested_read_number")
    if suggested not in {1, 2}:
        return ""
    if language == "zh":
        return f"（文件名提示可能是 R{suggested}）"
    return f" (filename suggests R{suggested})"


class ResourceClarificationService:
    """Manages the resource clarification dialogue loop."""

    async def prepare_issues_for_dialogue(
        self,
        project_id: str,
        issues: list[ReadinessIssue],
        db: AsyncSession | None = None,
        *,
        job_id: str | None = None,
    ) -> list[ReadinessIssue]:
        """Enrich issues with any DB-backed dialogue payload before prompting users."""
        if not project_id or not issues:
            return issues
        try:
            if db is not None:
                ctx = await PlannerContextBuilder(db).build(ContextScope(project_id=project_id))
                issues = await self._enrich_link_experiment_issues(project_id, issues, db, ctx=ctx)
                return await self._enrich_semantic_resource_issues(
                    project_id,
                    issues,
                    db,
                    ctx=ctx,
                    job_id=job_id,
                )
            async with get_session_factory()() as session:
                ctx = await PlannerContextBuilder(session).build(ContextScope(project_id=project_id))
                issues = await self._enrich_link_experiment_issues(project_id, issues, session, ctx=ctx)
                return await self._enrich_semantic_resource_issues(
                    project_id,
                    issues,
                    session,
                    ctx=ctx,
                    job_id=job_id,
                )
        except Exception:
            log.exception(
                "ResourceClarificationService: failed to prepare clarification issues for project %s",
                project_id,
            )
            return issues

    async def _recompute_blocking_issues(
        self,
        job_id: str,
        project_id: str,
    ) -> list[ReadinessIssue] | None:
        """Rebuild semantic readiness from persisted state for the blocked job."""
        if not job_id or not project_id:
            return []

        try:
            async with get_session_factory()() as session:
                job = (
                    await session.execute(select(AnalysisJob).where(AnalysisJob.id == job_id))
                ).scalar_one_or_none()
                if not job:
                    return []

                plan = (
                    job.resolved_plan_json
                    or job.plan_draft_json
                    or job.plan
                    or []
                )
                if isinstance(plan, dict):
                    plan = plan.get("steps") or []
                if not isinstance(plan, list):
                    plan = []

                ctx = await PlannerContextBuilder(session).build(
                    ContextScope(project_id=project_id)
                )
                graph = getattr(ctx, "resource_graph", None)
                if graph is None:
                    graph = await ResourceGraphBuilder().build(ctx, session)

                try:
                    graph_payload = {
                        "nodes": {
                            nid: {
                                "id": node.id,
                                "kind": node.kind,
                                "status": node.status,
                                "label": node.label,
                                "resolved_path": node.resolved_path,
                                "source_type": node.source_type,
                            }
                            for nid, node in graph.nodes.items()
                        },
                        "by_kind": graph.by_kind,
                    }
                    job.resource_graph_json = _json.dumps(graph_payload)
                    await session.commit()
                except Exception:
                    log.debug(
                        "ResourceClarificationService: failed to persist refreshed resource_graph_json for job %s",
                        job_id,
                    )

                report = ReadinessChecker().check(plan, graph)
                return [issue for issue in report.issues if issue.severity == "blocking"]
        except Exception:
            log.exception(
                "ResourceClarificationService: failed to recompute readiness for job %s",
                job_id,
            )
            return None

    async def _enrich_link_experiment_issues(
        self,
        project_id: str,
        issues: list[ReadinessIssue],
        db: AsyncSession,
        *,
        ctx=None,
    ) -> list[ReadinessIssue]:
        read_link_issues = [
            issue
            for issue in issues
            if issue.resolution_type == "link_experiment"
            or issue.kind in {"unbound_reads", "missing_experiment_link"}
        ]
        if not read_link_issues:
            return issues

        ctx = ctx or await PlannerContextBuilder(db).build(ContextScope(project_id=project_id))
        sample_by_id = {sample.id: sample for sample in ctx.samples}
        file_by_id = {file_info.id: file_info for file_info in ctx.files}
        experiment_candidates = [
            _experiment_candidate_payload(exp, sample_by_id.get(exp.sample_id), ctx)
            for exp in ctx.experiments
        ]

        for issue in read_link_issues:
            details = dict(issue.details or {})
            raw_files = details.get("files") or []
            file_ids = [
                file_id
                for file_id in (
                    file_info.get("file_id")
                    for file_info in raw_files
                    if isinstance(file_info, dict)
                )
                if file_id
            ]
            if not file_ids:
                file_ids = [
                    resource_id.split(":", 1)[1]
                    for resource_id in (issue.affected_resource_ids or [])
                    if isinstance(resource_id, str) and resource_id.startswith("reads:")
                ]
            resolved_files = []
            for file_id in file_ids:
                file_info = file_by_id.get(file_id)
                if not file_info:
                    continue
                resolved_files.append(
                    {
                        "resource_id": f"reads:{file_id}",
                        "file_id": file_info.id,
                        "filename": file_info.filename,
                        "path": file_info.path,
                        "suggested_read_number": _detect_read_number(file_info.filename),
                    }
                )

            details["files"] = resolved_files
            details["experiment_candidates"] = experiment_candidates
            details["no_experiments_available"] = not experiment_candidates
            issue.details = details
            issue.candidates = [
                ResourceCandidate(
                    path=candidate["display_label"],
                    organism=candidate.get("organism"),
                    source_type="filerun_db",
                    confidence=0.9,
                )
                for candidate in experiment_candidates
            ]
            if experiment_candidates:
                issue.suggestion = (
                    "选择正确的实验；如需指定读段方向，请同时说明 R1 或 R2。"
                    if issue.kind in {"unbound_reads", "missing_experiment_link"}
                    else issue.suggestion
                )
            else:
                issue.suggestion = (
                    "当前项目没有可供 FASTQ 归属的实验，请先创建实验。"
                    if issue.kind in {"unbound_reads", "missing_experiment_link"}
                    else issue.suggestion
                )
        return issues

    async def _enrich_semantic_resource_issues(
        self,
        project_id: str,
        issues: list[ReadinessIssue],
        db: AsyncSession,
        *,
        ctx=None,
        job_id: str | None = None,
    ) -> list[ReadinessIssue]:
        issue_binding_keys = {
            issue.id: await _resolve_issue_binding_key(issue, job_id=job_id, db=db)
            for issue in issues
        }
        semantic_issues = [issue for issue in issues if issue_binding_keys.get(issue.id) is not None]
        if not semantic_issues:
            return issues

        ctx = ctx or await PlannerContextBuilder(db).build(ContextScope(project_id=project_id))
        project_files = _planner_context_project_files(ctx)
        kp_bindings = _planner_context_known_path_bindings(ctx)

        for issue in semantic_issues:
            binding_key = issue_binding_keys.get(issue.id)
            slot = _binding_key_to_clarification_slot(binding_key)
            if not binding_key or slot is None:
                continue

            candidates = await retrieve_semantic_candidates(
                job_id="",
                dep_keys=[],
                slot=slot,
                project_id=project_id,
                project_files=project_files,
                kp_bindings=kp_bindings,
                db=db,
            )
            if not candidates:
                continue

            issue.candidates = [
                _resource_candidate_from_semantic_candidate(candidate)
                for candidate in candidates[:6]
            ]
            if issue.resolution_type in {None, "provide_path"}:
                issue.resolution_type = "select_candidate"
            setattr(issue, "binding_key", binding_key)

            details = dict(issue.details or {})
            details["semantic_candidates"] = [
                {
                    "path": candidate.get("file_path"),
                    "source_type": candidate.get("source_type"),
                    "score": candidate.get("score"),
                    "organism": candidate.get("organism"),
                    "genome_build": candidate.get("genome_build"),
                }
                for candidate in candidates[:6]
            ]
            issue.details = details

            if issue.kind in {
                "missing_reference",
                "missing_annotation",
                "missing_index",
                "missing_input_slot",
                "missing_concrete_path",
            }:
                issue.suggestion = (
                    "Select one of the detected candidates, or provide a different valid path."
                )
        return issues

    async def start(
        self,
        issues: list[ReadinessIssue],
        job_id: str,
        project_id: str,
        context_id: str,
        session_states: list[dict],
        language: str = "en",
    ) -> AsyncGenerator[dict, None]:
        """Activate resource_clarification state in all project WS sessions
        and stream the first issue prompt.
        """
        issues = await self.prepare_issues_for_dialogue(project_id, issues, job_id=job_id)
        # Activate state in all sessions
        issues_data = [_issue_to_dict(i) for i in issues]
        for state in session_states:
            state["resource_clarification"] = {
                "active": True,
                "job_id": job_id,
                "project_id": project_id,
                "issues": issues_data,
                "context_id": context_id,
            }

        # Yield the first blocking issue prompt
        blocking = [i for i in issues if i.severity == "blocking"]
        if blocking:
            first = blocking[0]
            prompt = render_issue_prompt(first, language=language)
            yield {"type": "token", "content": prompt}
        return
        yield  # make this an async generator

    async def advance(
        self,
        user_reply: str,
        state: dict,
        db: AsyncSession,
    ) -> AsyncGenerator[dict, None]:
        """Process a user reply during resource clarification.

        Extracts a ClarificationAction, dispatches to the appropriate handler,
        then re-runs ReadinessChecker.  If all blocking issues resolved,
        transitions job to queued.
        """
        rc = state.get("resource_clarification", {})
        job_id: str = rc.get("job_id", "")
        project_id: str = rc.get("project_id", "")
        thread_id: str | None = state.get("thread_id")
        lang = state.get("language", "en")
        raw_issues = rc.get("issues") or []
        issues: list[ReadinessIssue] = [
            _dict_to_issue(d) for d in raw_issues if isinstance(d, dict)
        ]
        issues = await self.prepare_issues_for_dialogue(project_id, issues, db, job_id=job_id)
        blocking = [i for i in issues if i.severity == "blocking"]

        if not blocking:
            state["resource_clarification"] = {"active": False}
            from tune.api.ws import _clear_thread_session_fields, clear_job_pending_interaction

            if job_id:
                await clear_job_pending_interaction(job_id, interaction_type="resource_clarification")
            _clear_thread_session_fields(thread_id, "resource_clarification")
            yield {"type": "token", "content": "All resources resolved." if lang != "zh" else "所有资源已就绪。"}
            return

        current_issue = blocking[0]

        # --- Extract ClarificationAction via structured LLM ---
        gw = get_gateway()
        try:
            action = await gw.structured_output(
                messages=[LLMMessage("user", user_reply)],
                schema=CLARIFICATION_ACTION_SCHEMA,
                system=(
                    ("请用中文回复。" if lang == "zh" else "")
                    + "Extract a clarification action from the user's reply. "
                    + f"The current issue is: {current_issue.title}. "
                    + f"Issue ID: {current_issue.id}. "
                    + "Resolution type: "
                    + (current_issue.resolution_type or "provide_path")
                    + ". "
                    + "Issue payload: "
                    + _json.dumps(_issue_to_dict(current_issue), ensure_ascii=False)
                ),
            )
        except Exception:
            log.exception("ResourceClarificationService: action extraction failed")
            action = {"action_type": "unknown"}

        action_type = action.get("action_type", "unknown")

        issue_resolved = False
        response_chunks: list[dict] = []

        if action_type == "select_candidate" and current_issue.resolution_type != "link_experiment":
            issue_resolved, response_chunks = await self._handle_select_candidate(
                action, current_issue, project_id, db
            )
        elif action_type == "provide_path":
            issue_resolved, response_chunks = await self._handle_provide_path(
                action, current_issue, project_id, db, lang
            )
        elif action_type == "confirm_auto_build":
            issue_resolved, response_chunks = await self._handle_confirm_auto_build(
                action, current_issue, issues, lang
            )
        elif action_type == "link_experiment" or (
            action_type == "select_candidate"
            and current_issue.resolution_type == "link_experiment"
        ):
            issue_resolved, response_chunks = await self._handle_link_experiment(
                action, current_issue, project_id, db, lang
            )
        else:
            # Unknown/failed extraction — re-present the issue
            prompt = render_issue_prompt(current_issue, language=lang)
            if lang == "zh":
                yield {"type": "token", "content": f"抱歉，我没有理解您的回复。{prompt}"}
            else:
                yield {"type": "token", "content": f"I didn't understand that. {prompt}"}
            return

        for chunk in response_chunks:
            yield chunk

        if not issue_resolved:
            # Action failed (e.g. invalid path) — stay in clarification state
            return

        # Refresh semantic readiness after the successful mutation has been committed.
        await db.commit()
        recomputed = await self._recompute_blocking_issues(job_id, project_id)
        remaining = (
            recomputed
            if recomputed is not None
            else [i for i in issues if i.id != current_issue.id and i.severity == "blocking"]
        )
        remaining = await self.prepare_issues_for_dialogue(project_id, remaining, db, job_id=job_id)

        if remaining:
            # Update session state with remaining issues and prompt next
            rc["issues"] = [_issue_to_dict(i) for i in remaining]
            state["resource_clarification"] = rc
            next_issue = remaining[0]
            next_prompt = render_issue_prompt(next_issue, language=lang)
            rc["prompt_text"] = next_prompt
            state["resource_clarification"]["prompt_text"] = next_prompt
            from tune.api.ws import _set_thread_session_field, persist_job_pending_interaction

            await persist_job_pending_interaction(
                job_id,
                "resource_clarification",
                {
                    "job_id": job_id,
                    "project_id": project_id,
                    "issues": rc["issues"],
                    "context_id": rc.get("context_id", job_id),
                    "prompt_text": next_prompt,
                },
            )
            _set_thread_session_field(
                thread_id,
                "resource_clarification",
                {
                    "active": True,
                    "job_id": job_id,
                    "project_id": project_id,
                    "issues": rc["issues"],
                    "context_id": rc.get("context_id", job_id),
                    "prompt_text": next_prompt,
                },
            )
            yield {"type": "token", "content": next_prompt}
        else:
            # All blocking issues resolved → re-queue the job
            state["resource_clarification"] = {"active": False}
            async for chunk in self._requeue_job(job_id, db, lang):
                yield chunk

    async def _handle_select_candidate(
        self,
        action: dict,
        issue: ReadinessIssue,
        project_id: str,
        db: AsyncSession,
    ) -> tuple[bool, list[dict]]:
        """User selected a candidate from the list — write to KnownPath.

        Returns (resolved, chunks).
        """
        idx = action.get("candidate_index", 0)
        if not issue.candidates or idx >= len(issue.candidates):
            return False, [{"type": "token", "content": "Invalid selection. Please try again."}]

        candidate = issue.candidates[idx]
        kp_key = getattr(issue, "binding_key", None) or _issue_kind_to_kp_key(issue.kind)
        if kp_key and candidate.path:
            await _upsert_known_path(project_id, kp_key, candidate.path, db)
            await _queue_resource_clarification_memory(
                db,
                project_id=project_id,
                issue=issue,
                resolution=(
                    f"User selected candidate path '{candidate.path}' for binding_key={kp_key}; "
                    f"source_type={candidate.source_type or 'unknown'}"
                ),
            )
        return True, [{"type": "token", "content": f"✓ Selected: `{candidate.path}`"}]

    async def _handle_provide_path(
        self,
        action: dict,
        issue: ReadinessIssue,
        project_id: str,
        db: AsyncSession,
        lang: str = "en",
    ) -> tuple[bool, list[dict]]:
        """User provided a path — validate and write to KnownPath.

        Returns (resolved, chunks).
        """
        path = (action.get("path") or "").strip()
        kp_key = getattr(issue, "binding_key", None) or _issue_kind_to_kp_key(issue.kind)
        if not path or not _path_exists_for_binding_key(kp_key, path):
            msg = (
                f"路径不存在：`{path}`。请提供有效路径。"
                if lang == "zh"
                else f"Path does not exist: `{path}`. Please provide a valid path."
            )
            return False, [{"type": "token", "content": msg}]

        if kp_key:
            await _upsert_known_path(project_id, kp_key, path, db)
            await _queue_resource_clarification_memory(
                db,
                project_id=project_id,
                issue=issue,
                resolution=f"User provided path '{path}' for binding_key={kp_key}.",
            )
        return True, [{"type": "token", "content": f"✓ Registered: `{path}`"}]

    async def _handle_confirm_auto_build(
        self,
        action: dict,
        issue: ReadinessIssue,
        all_issues: list[ReadinessIssue],
        lang: str = "en",
    ) -> tuple[bool, list[dict]]:
        """User confirmed auto-build for a derivable index — mark as resolved.

        Returns (resolved, chunks).
        """
        confirmed = action.get("confirmed", True)
        if not confirmed:
            msg = (
                "好的，已取消自动构建。"
                if lang == "zh"
                else "Auto-build cancelled. Please provide the index path manually."
            )
            return False, [{"type": "token", "content": msg}]

        msg = (
            "好的，系统将在分析开始时自动构建索引。"
            if lang == "zh"
            else "Auto-build confirmed. The index will be built as part of the analysis."
        )
        return True, [{"type": "token", "content": msg}]

    async def _handle_link_experiment(
        self,
        action: dict,
        issue: ReadinessIssue,
        project_id: str,
        db: AsyncSession,
        lang: str = "en",
    ) -> tuple[bool, list[dict]]:
        """Link an unbound FASTQ to an existing experiment via FileRun."""
        details = issue.details or {}
        files = details.get("files") or []
        if not files:
            msg = (
                "当前阻塞问题没有携带可链接的 FASTQ 文件信息，暂时无法自动修复。"
                if lang == "zh"
                else "This clarification issue does not contain enough FASTQ context to complete the linkage."
            )
            return False, [{"type": "token", "content": msg}]

        file_info = None
        action_file_id = action.get("file_id")
        if action_file_id:
            file_info = next((item for item in files if item.get("file_id") == action_file_id), None)
        if file_info is None:
            file_info = files[0]

        experiment_candidates = details.get("experiment_candidates") or []
        if not experiment_candidates:
            msg = (
                "当前项目还没有可链接的实验记录。请先在 Samples / Experiments 中创建实验。"
                if lang == "zh"
                else "No experiment exists in this project yet. Create an experiment first, then retry."
            )
            return False, [{"type": "token", "content": msg}]

        experiment_id = action.get("experiment_id")
        if not experiment_id:
            idx = action.get("candidate_index")
            if isinstance(idx, int) and 0 <= idx < len(experiment_candidates):
                experiment_id = experiment_candidates[idx]["experiment_id"]
        if not experiment_id and len(experiment_candidates) == 1:
            experiment_id = experiment_candidates[0]["experiment_id"]

        experiment = next(
            (candidate for candidate in experiment_candidates if candidate["experiment_id"] == experiment_id),
            None,
        )
        if not experiment:
            prompt = render_issue_prompt(issue, language=lang)
            msg = (
                "请先选择要归属的实验编号。\n\n" + prompt
                if lang == "zh"
                else "Select which experiment this FASTQ belongs to first.\n\n" + prompt
            )
            return False, [{"type": "token", "content": msg}]

        read_number = _resolve_read_number_for_linkage(
            action=action,
            file_info=file_info,
            experiment=experiment,
        )
        if read_number is _READ_ROLE_REQUIRED:
            prompt = render_issue_prompt(issue, language=lang)
            msg = (
                "该实验是双端测序，但当前还不能安全判断这个 FASTQ 是 R1 还是 R2。"
                "请回复实验编号并附上 R1 或 R2，例如“1，R1”。\n\n"
                + prompt
                if lang == "zh"
                else "This experiment is paired-end, but the read role is still ambiguous."
                " Reply with the experiment number and R1 or R2, for example '1, R1'.\n\n"
                + prompt
            )
            return False, [{"type": "token", "content": msg}]

        if read_number is _READ_SLOT_CONFLICT:
            msg = (
                "目标实验的读段槽位已经占满，无法再把这个 FASTQ 归进去。请检查该实验现有的 FileRun 记录。"
                if lang == "zh"
                else "The target experiment already has conflicting read assignments. Check its existing FileRun records."
            )
            return False, [{"type": "token", "content": msg}]

        conflict = await _find_conflicting_file_run(
            experiment_id=experiment["experiment_id"],
            file_id=file_info.get("file_id"),
            read_number=read_number,
            db=db,
        )
        if conflict:
            msg = (
                f"该实验的 R{read_number} 已经绑定到其他 FASTQ 文件，不能重复占用。"
                if lang == "zh" and read_number in {1, 2}
                else "The target experiment already uses that read slot for a different FASTQ file."
            )
            if read_number is None and lang == "zh":
                msg = "该单端实验已经绑定到其他 FASTQ 文件，不能重复归属。"
            elif read_number is None:
                msg = "The target single-end experiment is already linked to a different FASTQ file."
            return False, [{"type": "token", "content": msg}]

        await _upsert_file_run(
            experiment_id=experiment["experiment_id"],
            file_id=file_info.get("file_id"),
            read_number=read_number,
            filename=file_info.get("filename"),
            db=db,
        )
        await _queue_resource_clarification_memory(
            db,
            project_id=project_id,
            issue=issue,
            resolution=(
                f"Linked file_id={file_info.get('file_id')} to experiment_id={experiment['experiment_id']}; "
                f"read_number={read_number if read_number is not None else 'single'}"
            ),
        )

        read_label = "single-end" if read_number is None else f"R{read_number}"
        msg = (
            f"已将 `{file_info.get('filename') or file_info.get('file_id')}` 归属到实验 `{experiment['display_label']}`"
            f"（{read_label}）。"
            if lang == "zh"
            else f"Linked `{file_info.get('filename') or file_info.get('file_id')}` to experiment "
                 f"`{experiment['display_label']}` ({read_label})."
        )
        return True, [{"type": "token", "content": msg}]

    async def _requeue_job(
        self,
        job_id: str,
        db: AsyncSession,
        lang: str = "en",
    ) -> AsyncGenerator[dict, None]:
        """Set job status to queued and defer the Procrastinate task."""
        from tune.api.ws import _clear_thread_session_fields, clear_job_pending_interaction

        thread_id: str | None = None
        async with get_session_factory()() as session:
            job = (
                await session.execute(select(AnalysisJob).where(AnalysisJob.id == job_id))
            ).scalar_one_or_none()
            if job:
                thread_id = job.thread_id
                job.status = "queued"
                job.error_message = None
                job.pending_interaction_type = None
                job.pending_interaction_payload_json = None
                await session.commit()
            else:
                await clear_job_pending_interaction(job_id, interaction_type="resource_clarification")
        _clear_thread_session_fields(thread_id, "resource_clarification")

        try:
            from tune.workers.defer import defer_async_with_fallback
            from tune.workers.tasks import run_analysis_task

            await defer_async_with_fallback(run_analysis_task, job_id=job_id)
        except Exception:
            log.exception("ResourceClarificationService: failed to re-defer job %s", job_id)

        msg = (
            "所有资源已就绪！任务已重新排入队列，即将开始执行。"
            if lang == "zh"
            else "All resources resolved! The job has been re-queued and will start shortly."
        )
        yield {"type": "token", "content": msg}
        return
        yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_READ_ROLE_REQUIRED = object()
_READ_SLOT_CONFLICT = object()


def _experiment_candidate_payload(exp, sample, ctx) -> dict:
    occupied = sorted(
        {
            file_info.read_number
            for file_info in ctx.files
            if file_info.linked_experiment_id == exp.id and file_info.read_number in {1, 2}
        }
    )
    return {
        "experiment_id": exp.id,
        "sample_id": exp.sample_id,
        "sample_name": sample.sample_name if sample else "",
        "organism": sample.organism if sample else None,
        "library_strategy": exp.library_strategy,
        "library_layout": exp.library_layout,
        "platform": exp.platform,
        "occupied_read_numbers": occupied,
        "display_label": _format_experiment_candidate_label(exp, sample, occupied),
    }


def _format_experiment_candidate_label(exp, sample, occupied_read_numbers: list[int]) -> str:
    parts = []
    sample_name = sample.sample_name if sample else exp.id
    parts.append(sample_name)
    parts.append(f"exp {exp.id[:8]}")
    meta = [part for part in (exp.library_strategy, exp.library_layout) if part]
    if meta:
        parts.append(f"[{', '.join(meta)}]")
    if occupied_read_numbers:
        parts.append("slots:" + ",".join(f"R{num}" for num in occupied_read_numbers))
    return " ".join(parts)


def _resolve_read_number_for_linkage(
    *,
    action: dict,
    file_info: dict,
    experiment: dict,
):
    explicit_role = action.get("read_role")
    if explicit_role == "single":
        return None
    if explicit_role == "R1":
        return 1
    if explicit_role == "R2":
        return 2

    explicit_number = action.get("read_number")
    if explicit_number in {1, 2}:
        return explicit_number

    layout = (experiment.get("library_layout") or "").upper()
    occupied = set(experiment.get("occupied_read_numbers") or [])
    if layout == "SINGLE":
        return None
    if layout == "PAIRED":
        if occupied == {1, 2}:
            return _READ_SLOT_CONFLICT
        if occupied == {1}:
            return 2
        if occupied == {2}:
            return 1
        return _READ_ROLE_REQUIRED

    if occupied == {1, 2}:
        return _READ_SLOT_CONFLICT

    # Filename hints are not authoritative enough to auto-assign, but they are
    # useful to display back to the user in the prompt.
    if file_info.get("suggested_read_number") in {1, 2}:
        return _READ_ROLE_REQUIRED
    return _READ_ROLE_REQUIRED


async def _find_conflicting_file_run(
    *,
    experiment_id: str,
    file_id: str | None,
    read_number: int | None,
    db: AsyncSession,
) -> FileRun | None:
    if not experiment_id or not file_id:
        return None

    stmt = select(FileRun).where(FileRun.experiment_id == experiment_id)
    if read_number is None:
        stmt = stmt.where(FileRun.read_number.is_(None))
    else:
        stmt = stmt.where(FileRun.read_number == read_number)
    existing = (await db.execute(stmt)).scalars().all()
    for row in existing:
        if row.file_id != file_id:
            return row
    return None


async def _upsert_known_path(
    project_id: str, key: str, path: str, db: AsyncSession
) -> None:
    """Upsert a KnownPath record for the given project+key."""
    from sqlalchemy.dialects.postgresql import insert

    stmt = (
        insert(KnownPath)
        .values(
            id=__import__("uuid").uuid4().__str__(),
            project_id=project_id,
            key=key,
            path=path,
        )
        .on_conflict_do_update(
            constraint="uq_known_paths_project_id_key"
            if hasattr(KnownPath, "__table_args__")
            else None,
            index_elements=["project_id", "key"],
            set_={"path": path},
        )
    )
    await db.execute(stmt)


async def _upsert_file_run(
    *,
    experiment_id: str,
    file_id: str | None,
    read_number: int | None,
    filename: str | None,
    db: AsyncSession,
) -> None:
    if not file_id:
        raise ValueError("file_id is required to create a FileRun")

    existing = (
        await db.execute(select(FileRun).where(FileRun.file_id == file_id))
    ).scalar_one_or_none()
    if existing:
        existing.experiment_id = experiment_id
        existing.read_number = read_number
        existing.filename = filename
        return

    exp = (
        await db.execute(select(Experiment).where(Experiment.id == experiment_id))
    ).scalar_one_or_none()
    if not exp:
        raise ValueError(f"experiment not found: {experiment_id}")

    db.add(
        FileRun(
            id=str(uuid.uuid4()),
            experiment_id=experiment_id,
            file_id=file_id,
            read_number=read_number,
            filename=filename,
            attrs={},
        )
    )


def _issue_kind_to_kp_key(kind: str) -> str | None:
    return _ISSUE_KIND_TO_KNOWN_PATH_KEY.get(kind)


def _issue_binding_key(issue: ReadinessIssue) -> str | None:
    return getattr(issue, "binding_key", None) or _issue_kind_to_kp_key(issue.kind)


async def _resolve_issue_binding_key(
    issue: ReadinessIssue,
    *,
    job_id: str | None,
    db: AsyncSession,
) -> str | None:
    binding_key = _issue_binding_key(issue)
    if binding_key is not None:
        return binding_key
    if issue.kind not in {"missing_index", "ambiguous_index"} or not job_id:
        return None
    inferred = await _infer_index_binding_key_from_job_plan(
        job_id=job_id,
        step_keys=issue.affected_step_keys or [],
        db=db,
    )
    if inferred:
        setattr(issue, "binding_key", inferred)
    return inferred


async def _infer_index_binding_key_from_job_plan(
    *,
    job_id: str,
    step_keys: list[str],
    db: AsyncSession,
) -> str | None:
    job = (
        await db.execute(select(AnalysisJob).where(AnalysisJob.id == job_id))
    ).scalar_one_or_none()
    if job is None:
        return None
    raw_plan = job.resolved_plan_json or job.plan_draft_json or job.plan or []
    if isinstance(raw_plan, dict):
        plan = raw_plan.get("steps") or []
    elif isinstance(raw_plan, list):
        plan = raw_plan
    else:
        plan = []

    binding_keys: set[str] = set()
    for step in plan:
        if not isinstance(step, dict):
            continue
        if step_keys and step.get("step_key") not in step_keys:
            continue
        step_type = str(step.get("step_type") or "")
        binding_key = _index_binding_key_for_step_type(step_type)
        if binding_key:
            binding_keys.add(binding_key)
    if len(binding_keys) == 1:
        return next(iter(binding_keys))
    return None


def _index_binding_key_for_step_type(step_type: str) -> str | None:
    mapping = {
        "align.hisat2": "hisat2_index",
        "align.star": "star_genome_dir",
        "align.bwa": "bwa_index",
        "align.bowtie2": "bowtie2_index",
    }
    return mapping.get(step_type)


def _binding_key_to_clarification_slot(binding_key: str | None) -> SlotDefinition | None:
    if binding_key == "reference_fasta":
        return SlotDefinition(
            "reference_fasta",
            "Reference FASTA",
            ["fa", "fasta", "fna", "fa.gz", "fasta.gz"],
            accepted_roles=["reference_fasta"],
        )
    if binding_key == "annotation_gtf":
        return SlotDefinition(
            "annotation_gtf",
            "Annotation GTF/GFF",
            ["gtf", "gff", "gff3", "gtf.gz", "gff.gz", "gff3.gz"],
            accepted_roles=["annotation_gtf"],
        )
    if binding_key in {"hisat2_index", "bwa_index", "bowtie2_index"}:
        return SlotDefinition(
            "index_prefix",
            "Aligner index prefix",
            ["*"],
            accepted_roles=[binding_key],
        )
    if binding_key == "star_genome_dir":
        return SlotDefinition(
            "genome_dir",
            "STAR genome directory",
            ["*"],
            accepted_roles=["star_genome_dir"],
        )
    return None


def _planner_context_project_files(ctx) -> list[dict]:
    sample_by_id = {sample.id: sample for sample in getattr(ctx, "samples", [])}
    project_files = []
    for file_info in getattr(ctx, "files", []):
        sample_id = getattr(file_info, "linked_sample_id", None)
        sample = sample_by_id.get(sample_id)
        project_files.append(
            {
                "id": file_info.id,
                "path": file_info.path,
                "filename": file_info.filename,
                "file_type": file_info.file_type,
                "linked_sample_id": sample_id,
                "linked_experiment_id": getattr(file_info, "linked_experiment_id", None),
                "sample_name": getattr(sample, "sample_name", None) if sample is not None else None,
                "read_number": getattr(file_info, "read_number", None),
            }
        )
    return project_files


def _planner_context_known_path_bindings(ctx) -> dict[str, str]:
    project = getattr(ctx, "project", None)
    if project is None:
        return {}
    return {
        item.get("key"): item.get("path")
        for item in (getattr(project, "known_paths", None) or [])
        if item.get("key") and item.get("path")
    }


def _resource_candidate_from_semantic_candidate(candidate: dict) -> ResourceCandidate:
    return ResourceCandidate(
        path=candidate.get("file_path", ""),
        organism=candidate.get("organism"),
        genome_build=candidate.get("genome_build"),
        source_type=candidate.get("source_type"),
        confidence=max(min(float(candidate.get("score", 0)) / 100.0, 1.0), 0.0),
    )


async def _queue_resource_clarification_memory(
    db: AsyncSession,
    *,
    project_id: str,
    issue: ReadinessIssue,
    resolution: str,
) -> None:
    if not project_id:
        return
    try:
        from tune.core.memory.project_memory import queue_execution_event

        await queue_execution_event(
            db,
            project_id=project_id,
            event_type="resource_clarification_resolved",
            description=(
                f"Resolved clarification issue '{issue.kind}'"
                f" ({issue.title or issue.description or 'resource clarification'})."
            ),
            resolution=resolution,
            user_contributed=True,
            metadata_json={
                "issue_id": issue.id,
                "issue_kind": issue.kind,
                "issue_title": issue.title,
                "affected_step_keys": list(issue.affected_step_keys or []),
                "resolution_type": issue.resolution_type,
            },
        )
    except Exception:
        log.debug(
            "ResourceClarificationService: failed to queue project memory event for project %s",
            project_id,
            exc_info=True,
        )


def _issue_to_dict(issue: ReadinessIssue) -> dict:
    data = {
        "id": issue.id,
        "kind": issue.kind,
        "severity": issue.severity,
        "title": issue.title,
        "description": issue.description,
        "suggestion": issue.suggestion,
        "affected_resource_ids": issue.affected_resource_ids,
        "affected_step_keys": issue.affected_step_keys,
        "resolution_type": issue.resolution_type,
        "candidates": [
            {
                "path": c.path,
                "organism": c.organism,
                "genome_build": c.genome_build,
                "source_type": c.source_type,
                "confidence": c.confidence,
            }
            for c in issue.candidates
        ],
        "details": issue.details or {},
    }
    binding_key = getattr(issue, "binding_key", None)
    if binding_key:
        data["binding_key"] = binding_key
    return data


def _dict_to_issue(d: dict) -> ReadinessIssue:
    issue = ReadinessIssue(
        id=d.get("id", ""),
        kind=d.get("kind", "missing_reference"),  # type: ignore[arg-type]
        severity=d.get("severity", "blocking"),    # type: ignore[arg-type]
        title=d.get("title", ""),
        description=d.get("description", ""),
        suggestion=d.get("suggestion", ""),
        affected_resource_ids=d.get("affected_resource_ids") or [],
        affected_step_keys=d.get("affected_step_keys") or [],
        resolution_type=d.get("resolution_type"),  # type: ignore[arg-type]
        candidates=[
            ResourceCandidate(
                path=c.get("path", ""),
                organism=c.get("organism"),
                genome_build=c.get("genome_build"),
                source_type=c.get("source_type"),   # type: ignore[arg-type]
                confidence=c.get("confidence", 0.5),
            )
            for c in (d.get("candidates") or [])
        ],
        details=d.get("details") or {},
    )
    binding_key = d.get("binding_key")
    if binding_key:
        setattr(issue, "binding_key", binding_key)
    return issue


def _path_exists_for_binding_key(binding_key: str | None, path: str) -> bool:
    if not path:
        return False
    if binding_key in {"hisat2_index", "bwa_index", "bowtie2_index", "star_genome_dir"}:
        import glob

        return bool(glob.glob(path + "*")) or os.path.exists(path)
    return os.path.exists(path)


def normalize_resource_clarification_payload(
    payload: dict | None,
    *,
    language: str = "en",
) -> dict | None:
    from tune.core.decision_packet import (
        attach_decision_packet,
        build_resource_clarification_decision_packet,
    )

    if not payload:
        return None
    issues = payload.get("issues") or []
    normalized = dict(payload)
    normalized["issues"] = [issue for issue in issues if isinstance(issue, dict)]
    if not normalized["issues"]:
        return None
    if not normalized.get("prompt_text") and normalized["issues"]:
        normalized["prompt_text"] = render_issue_prompt(
            _dict_to_issue(normalized["issues"][0]),
            language=language,
        )
    return attach_decision_packet(
        normalized,
        build_resource_clarification_decision_packet(
            issues=normalized["issues"],
            job_id=str(normalized.get("job_id") or ""),
            project_id=str(normalized.get("project_id") or ""),
            context_id=str(normalized.get("context_id") or ""),
            language=language,
        ),
    )
