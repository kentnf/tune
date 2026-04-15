"""Unified user-facing decision packet helpers."""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class DecisionOption(BaseModel):
    option_id: str
    label: str
    action: str


class ClarificationOption(BaseModel):
    option_id: str
    label: str
    value: str
    description: str = ""


class ClarificationQuestion(BaseModel):
    question_id: str
    prompt: str
    response_kind: str
    required: bool = True
    allows_free_text: bool = False
    free_text_hint: str = ""
    options: list[ClarificationOption] = Field(default_factory=list)
    context: dict[str, Any] = Field(default_factory=dict)


class ClarificationRequest(BaseModel):
    request_id: str
    request_type: str
    prompt: str
    questions: list[ClarificationQuestion] = Field(default_factory=list)
    context: dict[str, Any] = Field(default_factory=dict)


class DecisionPacket(BaseModel):
    decision_type: str
    stage: str
    blocking: bool
    summary: str
    reason: str = ""
    required_user_action: str = ""
    options: list[DecisionOption] = Field(default_factory=list)
    context_payload: dict[str, Any] = Field(default_factory=dict)


_PROGRESS_FOCUS_TO_DECISION_TYPE = {
    "capability_gap": "capability_gap",
    "progress_readiness": "progress_readiness",
    "plan_confirmation": "plan_confirmation",
    "execution_confirmation": "execution_confirmation",
    "authorization": "authorization",
    "repair": "repair",
    "resource_clarification": "resource_clarification",
}


def generate_decision_packet(
    decision_type: str,
    *,
    language: str = "en",
    **kwargs: Any,
) -> DecisionPacket:
    normalized_type = str(decision_type or "").strip()
    if normalized_type == "capability_gap":
        return build_capability_gap_decision_packet(kwargs["gap"], language=language)
    if normalized_type == "plan_confirmation":
        return build_plan_confirmation_decision_packet(
            goal=kwargs["goal"],
            plan=kwargs["plan"],
            short_name=kwargs.get("short_name", ""),
            language=language,
        )
    if normalized_type == "execution_confirmation":
        return build_execution_confirmation_decision_packet(
            goal=kwargs["goal"],
            review_plan=kwargs["review_plan"],
            execution_payload=kwargs["execution_payload"],
            short_name=kwargs.get("short_name", ""),
            language=language,
        )
    if normalized_type == "resource_clarification":
        return build_resource_clarification_decision_packet(
            issues=kwargs["issues"],
            job_id=kwargs.get("job_id", ""),
            project_id=kwargs.get("project_id", ""),
            context_id=kwargs.get("context_id", ""),
            language=language,
        )
    if normalized_type == "progress_readiness":
        return build_progress_readiness_decision_packet(
            kwargs["assessment"],
            language=language,
        )
    if normalized_type == "authorization":
        return build_authorization_decision_packet(
            auth_request_id=kwargs["auth_request_id"],
            command=kwargs["command"],
            command_type=kwargs.get("command_type", ""),
            step_key=kwargs.get("step_key", ""),
            language=language,
        )
    if normalized_type == "repair":
        return build_repair_decision_packet(
            repair_request_id=kwargs["repair_request_id"],
            step_key=kwargs.get("step_key", ""),
            failed_command=kwargs.get("failed_command", ""),
            stderr_excerpt=kwargs.get("stderr_excerpt", ""),
            repair_context=kwargs.get("repair_context"),
            language=language,
        )
    raise ValueError(f"Unsupported decision packet type: {normalized_type}")


def build_capability_gap_decision_packet(
    gap,
    *,
    language: str = "en",
) -> DecisionPacket:
    from tune.core.analysis.capability_gap import CapabilityGap, format_capability_gap

    normalized_gap = gap if isinstance(gap, CapabilityGap) else CapabilityGap.model_validate(gap)
    clarification_request = build_capability_gap_clarification_request(
        uncovered_goal_fragments=list(normalized_gap.uncovered_goal_fragments),
        suggested_clarifications=list(normalized_gap.suggested_clarifications),
        language=language,
    )
    return DecisionPacket(
        decision_type="capability_gap",
        stage="capability_gap",
        blocking=True,
        summary=format_capability_gap(normalized_gap, language=language),
        reason=normalized_gap.reason,
        required_user_action=(
            (
                "联系开发人员，或通过 GitHub 提交 issue"
                if language == "zh"
                else "Contact the developers or submit an issue via GitHub"
            )
            if normalized_gap.gap_stage == "intent_coverage"
            else (
                "补充澄清信息或接受当前 capability 边界"
                if language == "zh"
                else "Provide clarifications or accept the current capability boundary"
            )
        ),
        options=_options_for(language, kind="gap"),
        context_payload={
            "covered_goal_fragments": list(normalized_gap.covered_goal_fragments),
            "uncovered_goal_fragments": list(normalized_gap.uncovered_goal_fragments),
            "suggested_clarifications": list(normalized_gap.suggested_clarifications),
            "suggested_experimental_capability": normalized_gap.suggested_experimental_capability,
            "clarification_request": clarification_request.model_dump(mode="json"),
        },
    )


def build_plan_confirmation_decision_packet(
    *,
    goal: str,
    plan: list[dict[str, Any]],
    short_name: str = "",
    language: str = "en",
) -> DecisionPacket:
    summary = (
        "当前抽象分析计划等待确认。"
        if language == "zh"
        else "Abstract analysis plan is waiting for confirmation."
    )
    return DecisionPacket(
        decision_type="plan_confirmation",
        stage="abstract_plan",
        blocking=True,
        summary=summary,
        reason=(
            "需要先确认抽象分析计划，才能继续编译执行层。"
            if language == "zh"
            else "The abstract analysis plan must be confirmed before execution planning can proceed."
        ),
        required_user_action=(
            "确认、取消，或继续修改分析计划"
            if language == "zh"
            else "Confirm, cancel, or continue editing the analysis plan"
        ),
        options=_options_for(language, kind="confirmation"),
        context_payload={
            "phase": "abstract",
            "goal": goal,
            "short_name": short_name,
            "plan_step_count": len(plan),
        },
    )


def build_execution_confirmation_decision_packet(
    *,
    goal: str,
    review_plan: list[dict[str, Any]],
    execution_payload: dict[str, Any],
    short_name: str = "",
    language: str = "en",
) -> DecisionPacket:
    summary = (
        "最终执行图等待最后确认。"
        if language == "zh"
        else "Execution graph is waiting for final confirmation."
    )
    return DecisionPacket(
        decision_type="execution_confirmation",
        stage="execution_plan",
        blocking=True,
        summary=summary,
        reason=(
            "执行层已经生成，但仍需要最后一次人工确认。"
            if language == "zh"
            else "The execution layer has been materialized but still requires final human confirmation."
        ),
        required_user_action=(
            "确认启动执行，或继续修改计划"
            if language == "zh"
            else "Confirm execution or continue editing the plan"
        ),
        options=_options_for(language, kind="confirmation"),
        context_payload={
            "phase": "execution",
            "goal": goal,
            "short_name": short_name,
            "execution_plan_summary": execution_payload.get("summary"),
            "execution_decision_source": execution_payload.get("execution_decision_source"),
            "review_group_count": len(review_plan),
        },
    )


def build_resource_clarification_decision_packet(
    *,
    issues: list[dict[str, Any]],
    job_id: str = "",
    project_id: str = "",
    context_id: str = "",
    language: str = "en",
) -> DecisionPacket:
    blocking_issue_count = sum(1 for issue in issues if issue.get("severity") == "blocking")
    clarification_request = build_resource_clarification_request(
        issues=issues,
        job_id=job_id,
        project_id=project_id,
        context_id=context_id,
        language=language,
    )
    summary = (
        "当前分析因资源澄清而暂停。"
        if language == "zh"
        else "Analysis is paused pending resource clarification."
    )
    return DecisionPacket(
        decision_type="resource_clarification",
        stage="resource_binding",
        blocking=True,
        summary=summary,
        reason=(
            f"当前有 {blocking_issue_count or len(issues)} 个资源问题需要确认。"
            if language == "zh"
            else f"{blocking_issue_count or len(issues)} resource issue(s) must be clarified before execution can continue."
        ),
        required_user_action=(
            "补充缺失资源信息或从候选项中确认正确资源"
            if language == "zh"
            else "Provide missing resource information or confirm the correct candidate"
        ),
        options=_options_for(language, kind="resource_clarification"),
        context_payload={
            "job_id": job_id,
            "project_id": project_id,
            "context_id": context_id,
            "issue_count": len(issues),
            "blocking_issue_count": blocking_issue_count,
            "issue_kinds": [str(issue.get("kind") or "") for issue in issues if issue.get("kind")],
            "clarification_request": clarification_request.model_dump(mode="json"),
        },
    )


def build_progress_readiness_decision_packet(
    assessment,
    *,
    language: str = "en",
) -> DecisionPacket:
    from tune.core.analysis.readiness import ReadinessAssessment

    normalized = (
        assessment
        if isinstance(assessment, ReadinessAssessment)
        else ReadinessAssessment.model_validate(assessment)
    )
    clarification_request = build_readiness_clarification_request(
        assessment=normalized,
        language=language,
    )
    return DecisionPacket(
        decision_type="progress_readiness",
        stage="progress_readiness",
        blocking=True,
        summary=_render_progress_readiness_summary(
            normalized,
            clarification_request=clarification_request,
            language=language,
        ),
        reason=normalized.reasoning_summary,
        required_user_action=(
            "补充阻塞信息或收敛当前分析目标"
            if language == "zh"
            else "Provide the missing blockers or tighten the current analysis goal"
        ),
        options=_options_for(language, kind="gap"),
        context_payload={
            "ready_to_proceed": normalized.ready_to_proceed,
            "readiness_score": normalized.readiness_score,
            "recommended_next_action": normalized.recommended_next_action,
            "blocking_issues": [
                issue.model_dump(mode="json") for issue in normalized.blocking_issues
            ],
            "clarification_request": clarification_request.model_dump(mode="json"),
        },
    )


def build_authorization_decision_packet(
    *,
    auth_request_id: str,
    command: str,
    command_type: str = "",
    step_key: str = "",
    language: str = "en",
) -> DecisionPacket:
    summary = (
        "命令执行正在等待授权。"
        if language == "zh"
        else "Command execution is waiting for authorization."
    )
    return DecisionPacket(
        decision_type="authorization",
        stage="step_execution",
        blocking=True,
        summary=summary,
        reason=(
            "当前步骤需要显式人工授权后才能继续执行。"
            if language == "zh"
            else "The current step requires explicit human approval before it can continue."
        ),
        required_user_action=(
            "批准或拒绝本次命令执行"
            if language == "zh"
            else "Approve or reject this command execution"
        ),
        options=_options_for(language, kind="authorization"),
        context_payload={
            "auth_request_id": auth_request_id,
            "command": command,
            "command_type": command_type,
            "step_key": step_key,
        },
    )


def build_repair_decision_packet(
    *,
    repair_request_id: str,
    step_key: str = "",
    failed_command: str = "",
    stderr_excerpt: str = "",
    repair_context: dict[str, Any] | None = None,
    language: str = "en",
) -> DecisionPacket:
    summary = (
        "执行失败，等待人工修复决策。"
        if language == "zh"
        else "Execution failed and is waiting for a human repair decision."
    )
    return DecisionPacket(
        decision_type="repair",
        stage="step_execution",
        blocking=True,
        summary=summary,
        reason=(
            "当前步骤执行失败，系统不能安全自动恢复。"
            if language == "zh"
            else "The current step failed and the system cannot safely recover automatically."
        ),
        required_user_action=(
            "提供修复命令或停止当前执行"
            if language == "zh"
            else "Provide a repair command or stop the current execution"
        ),
        options=_options_for(language, kind="repair"),
        context_payload={
            "repair_request_id": repair_request_id,
            "step_key": step_key,
            "failed_command": failed_command,
            "stderr_excerpt": stderr_excerpt,
            **({"repair_context": repair_context} if isinstance(repair_context, dict) and repair_context else {}),
        },
    )


def ensure_decision_packet(payload: dict[str, Any] | None) -> DecisionPacket | None:
    if not isinstance(payload, dict):
        return None
    packet = payload.get("decision_packet")
    if not isinstance(packet, dict):
        return None
    return DecisionPacket.model_validate(packet)


def ensure_clarification_request(payload: dict[str, Any] | None) -> ClarificationRequest | None:
    if not isinstance(payload, dict):
        return None
    return _coerce_clarification_request(payload)


def attach_decision_packet(
    payload: dict[str, Any] | None,
    packet: DecisionPacket | dict[str, Any] | None,
) -> dict[str, Any]:
    normalized_payload = dict(payload or {})
    if packet is None:
        return normalized_payload
    normalized_packet = (
        packet if isinstance(packet, DecisionPacket) else DecisionPacket.model_validate(packet)
    )
    normalized_payload["decision_packet"] = normalized_packet.model_dump(mode="json")
    normalized_payload.setdefault("prompt_text", normalized_packet.summary)
    return normalized_payload


def infer_decision_type_for_state(
    state: dict[str, Any] | None,
    *,
    progress_state: dict[str, Any] | None = None,
) -> str | None:
    normalized_state = state if isinstance(state, dict) else {}
    normalized_progress = (
        progress_state
        if isinstance(progress_state, dict)
        else (
            normalized_state.get("progress_state")
            if isinstance(normalized_state.get("progress_state"), dict)
            else {}
        )
    )

    focus = str(normalized_progress.get("decision_focus") or "").strip()
    requires_user_decision = bool(normalized_progress.get("requires_user_decision"))
    if requires_user_decision and focus in _PROGRESS_FOCUS_TO_DECISION_TYPE:
        return _PROGRESS_FOCUS_TO_DECISION_TYPE[focus]

    pending_plan = normalized_state.get("pending_analysis_plan")
    if isinstance(pending_plan, dict) and pending_plan.get("active"):
        phase = str(pending_plan.get("phase") or "abstract").strip() or "abstract"
        return "execution_confirmation" if phase == "execution" else "plan_confirmation"

    resource_clarification = normalized_state.get("resource_clarification")
    if isinstance(resource_clarification, dict) and resource_clarification.get("active"):
        return "resource_clarification"

    if isinstance(normalized_state.get("pending_command_auth"), dict):
        return "authorization"
    if isinstance(normalized_state.get("pending_error_recovery"), dict):
        return "repair"

    existing = normalized_state.get("last_decision_packet")
    if isinstance(existing, dict):
        decision_type = str(existing.get("decision_type") or "").strip()
        if decision_type:
            return decision_type
    return None


def select_decision_packet_for_state(
    state: dict[str, Any] | None,
    *,
    language: str = "en",
    progress_state: dict[str, Any] | None = None,
    goal: str | None = None,
    short_name: str | None = None,
    capability_gap: Any = None,
    readiness_assessment: Any = None,
    review_plan: list[dict[str, Any]] | None = None,
    execution_payload: dict[str, Any] | None = None,
    authorization_request: dict[str, Any] | None = None,
    repair_request: dict[str, Any] | None = None,
    resource_clarification: dict[str, Any] | None = None,
) -> DecisionPacket | None:
    normalized_state = state if isinstance(state, dict) else {}
    selected_type = infer_decision_type_for_state(
        normalized_state,
        progress_state=progress_state,
    )

    existing_packet = _coerce_decision_packet(normalized_state.get("last_decision_packet"))
    if not selected_type:
        return existing_packet

    if selected_type == "capability_gap":
        normalized_gap = capability_gap or normalized_state.get("capability_gap")
        if normalized_gap is None:
            return existing_packet
        return build_capability_gap_decision_packet(normalized_gap, language=language)

    if selected_type == "progress_readiness":
        normalized_assessment = readiness_assessment or normalized_state.get("last_readiness_assessment")
        if normalized_assessment is None:
            return existing_packet
        return build_progress_readiness_decision_packet(normalized_assessment, language=language)

    if selected_type in {"plan_confirmation", "execution_confirmation"}:
        pending_plan = (
            normalized_state.get("pending_analysis_plan")
            if isinstance(normalized_state.get("pending_analysis_plan"), dict)
            else {}
        )
        resolved_goal = (
            goal
            or pending_plan.get("goal")
            or ((normalized_state.get("analysis_intent_trace") or {}).get("finalized") or {}).get(
                "intent",
                {},
            ).get("user_goal")
            or ""
        )
        resolved_short_name = (
            short_name
            or pending_plan.get("short_name")
            or ""
        )
        if selected_type == "plan_confirmation":
            plan = pending_plan.get("plan") or []
            return build_plan_confirmation_decision_packet(
                goal=str(resolved_goal),
                plan=list(plan) if isinstance(plan, list) else [],
                short_name=str(resolved_short_name),
                language=language,
            )

        resolved_review_plan = (
            review_plan
            if isinstance(review_plan, list)
            else (
                pending_plan.get("review_plan")
                if isinstance(pending_plan.get("review_plan"), list)
                else pending_plan.get("plan") or []
            )
        )
        resolved_execution_payload = (
            execution_payload
            if isinstance(execution_payload, dict)
            else {
                "summary": pending_plan.get("execution_plan_summary"),
                "execution_decision_source": pending_plan.get("execution_decision_source"),
            }
        )
        return build_execution_confirmation_decision_packet(
            goal=str(resolved_goal),
            review_plan=list(resolved_review_plan) if isinstance(resolved_review_plan, list) else [],
            execution_payload=resolved_execution_payload,
            short_name=str(resolved_short_name),
            language=language,
        )

    if selected_type == "authorization":
        normalized_auth = authorization_request or normalized_state.get("pending_command_auth") or {}
        auth_request_id = str(normalized_auth.get("auth_request_id") or "").strip()
        command = str(normalized_auth.get("command") or "").strip()
        if not auth_request_id or not command:
            return existing_packet
        step_key = str(
            normalized_auth.get("step_key")
            or ((normalized_auth.get("step") or {}).get("step_key"))
            or ""
        ).strip()
        return build_authorization_decision_packet(
            auth_request_id=auth_request_id,
            command=command,
            command_type=str(normalized_auth.get("command_type") or "").strip(),
            step_key=step_key,
            language=language,
        )

    if selected_type == "repair":
        normalized_repair = repair_request or normalized_state.get("pending_error_recovery") or {}
        repair_context = normalized_repair.get("repair_context")
        if not isinstance(repair_context, dict):
            repair_context = (
                (normalized_repair.get("context") or {}).get("repair_context")
                if isinstance(normalized_repair.get("context"), dict)
                else None
            )
        repair_request_id = str(
            normalized_repair.get("repair_request_id")
            or ((normalized_repair.get("context") or {}).get("repair_request_id"))
            or ""
        ).strip()
        if not repair_request_id:
            return existing_packet
        return build_repair_decision_packet(
            repair_request_id=repair_request_id,
            step_key=str(
                normalized_repair.get("step_key")
                or ((normalized_repair.get("context") or {}).get("step"))
                or ""
            ).strip(),
            failed_command=str(
                normalized_repair.get("failed_command")
                or ((normalized_repair.get("context") or {}).get("command"))
                or ""
            ).strip(),
            stderr_excerpt=str(
                normalized_repair.get("stderr_excerpt")
                or ((normalized_repair.get("context") or {}).get("stderr"))
                or ""
            ).strip(),
            repair_context=repair_context if isinstance(repair_context, dict) else None,
            language=language,
        )

    if selected_type == "resource_clarification":
        normalized_resource = resource_clarification or normalized_state.get("resource_clarification") or {}
        issues = normalized_resource.get("issues") or []
        if not isinstance(issues, list) or not issues:
            return existing_packet
        return build_resource_clarification_decision_packet(
            issues=[issue for issue in issues if isinstance(issue, dict)],
            job_id=str(normalized_resource.get("job_id") or ""),
            project_id=str(normalized_resource.get("project_id") or ""),
            context_id=str(normalized_resource.get("context_id") or ""),
            language=language,
        )

    return existing_packet


def select_clarification_request_for_state(
    state: dict[str, Any] | None,
    *,
    language: str = "en",
    progress_state: dict[str, Any] | None = None,
    capability_gap: Any = None,
    readiness_assessment: Any = None,
    resource_clarification: dict[str, Any] | None = None,
) -> ClarificationRequest | None:
    normalized_state = state if isinstance(state, dict) else {}
    selected_type = infer_decision_type_for_state(
        normalized_state,
        progress_state=progress_state,
    )

    if selected_type == "resource_clarification":
        normalized_resource = resource_clarification or normalized_state.get("resource_clarification") or {}
        issues = normalized_resource.get("issues") or []
        if not issues:
            return _clarification_request_from_packet(
                _coerce_decision_packet(normalized_state.get("last_decision_packet"))
            )
        return build_resource_clarification_request(
            issues=[issue for issue in issues if isinstance(issue, dict)],
            job_id=str(normalized_resource.get("job_id") or ""),
            project_id=str(normalized_resource.get("project_id") or ""),
            context_id=str(normalized_resource.get("context_id") or ""),
            language=language,
        )

    if selected_type == "progress_readiness":
        normalized_assessment = readiness_assessment or normalized_state.get("last_readiness_assessment")
        if normalized_assessment is not None:
            return build_readiness_clarification_request(
                assessment=normalized_assessment,
                language=language,
            )

    if selected_type == "capability_gap":
        if capability_gap is not None:
            if isinstance(capability_gap, dict):
                uncovered_goal_fragments = list(capability_gap.get("uncovered_goal_fragments") or [])
                suggested_clarifications = list(capability_gap.get("suggested_clarifications") or [])
            else:
                uncovered_goal_fragments = list(
                    getattr(capability_gap, "uncovered_goal_fragments", None) or []
                )
                suggested_clarifications = list(
                    getattr(capability_gap, "suggested_clarifications", None) or []
                )
            return build_capability_gap_clarification_request(
                uncovered_goal_fragments=uncovered_goal_fragments,
                suggested_clarifications=suggested_clarifications,
                language=language,
            )
        return _clarification_request_from_packet(
            _coerce_decision_packet(normalized_state.get("last_decision_packet"))
        )

    return None


def _coerce_decision_packet(raw_packet: Any) -> DecisionPacket | None:
    if not isinstance(raw_packet, dict):
        return None
    try:
        return DecisionPacket.model_validate(raw_packet)
    except Exception:
        return None


def _coerce_clarification_request(raw_request: Any) -> ClarificationRequest | None:
    if not isinstance(raw_request, dict):
        return None
    try:
        return ClarificationRequest.model_validate(raw_request)
    except Exception:
        return None


def _clarification_request_from_packet(packet: DecisionPacket | None) -> ClarificationRequest | None:
    if packet is None:
        return None
    return _coerce_clarification_request((packet.context_payload or {}).get("clarification_request"))


def build_capability_gap_clarification_request(
    *,
    uncovered_goal_fragments: list[str],
    suggested_clarifications: list[str],
    language: str = "en",
) -> ClarificationRequest:
    prompts = [item.strip() for item in suggested_clarifications if item and item.strip()]
    fallback_fragments = [item.strip() for item in uncovered_goal_fragments if item and item.strip()]
    active_prompt = prompts[0] if prompts else (fallback_fragments[0] if fallback_fragments else "")
    active_fragment = fallback_fragments[0] if fallback_fragments else ""
    questions = []
    if active_prompt:
        questions.append(
            ClarificationQuestion(
                question_id="gap_active",
                prompt=active_prompt,
                response_kind="free_text",
                allows_free_text=True,
                free_text_hint=(
                    "请直接补充该问题所需的信息。"
                    if language == "zh"
                    else "Reply with the missing information directly."
                ),
                context={
                    "target_fragment": active_fragment,
                },
            )
        )
    return ClarificationRequest(
        request_id="capability_gap",
        request_type="goal_clarification",
        prompt=(
            "请先补充当前最关键的一项信息，以便继续缩小 capability gap。"
            if language == "zh"
            else "Please clarify the most important missing point so the capability gap can be narrowed."
        ),
        questions=questions,
        context={
            "uncovered_goal_fragments": [item for item in uncovered_goal_fragments if item],
            "active_target_fragment": active_fragment,
            "clarification_count": len(prompts) or len(fallback_fragments),
        },
    )


def build_readiness_clarification_request(
    *,
    assessment,
    language: str = "en",
) -> ClarificationRequest:
    from tune.core.analysis.readiness import ReadinessAssessment

    normalized = (
        assessment
        if isinstance(assessment, ReadinessAssessment)
        else ReadinessAssessment.model_validate(assessment)
    )
    active_issue = _select_active_readiness_issue(normalized.blocking_issues)
    prompt_text = _render_readiness_issue_prompt(active_issue, language=language)
    free_text_hint = _render_readiness_issue_hint(active_issue, language=language)
    return ClarificationRequest(
        request_id="progress_readiness",
        request_type="readiness_clarification",
        prompt=(
            "请先补充当前最关键的阻塞信息，再继续推进分析。"
            if language == "zh"
            else "Resolve the current key blocker before continuing the analysis."
        ),
        questions=(
            [
                ClarificationQuestion(
                    question_id="readiness_active",
                    prompt=prompt_text,
                    response_kind="free_text",
                    allows_free_text=True,
                    free_text_hint=free_text_hint,
                    context={
                        "issue_code": active_issue.issue_code,
                        "suggested_action": active_issue.suggested_action,
                        "evidence": list(active_issue.evidence),
                        "raw_message": active_issue.message,
                        "active_issue": (
                            dict((getattr(active_issue, "details", {}) or {}).get("active_issue") or {})
                        ),
                    },
                )
            ]
            if active_issue is not None
            else []
        ),
        context={
            "readiness_score": normalized.readiness_score,
            "recommended_next_action": normalized.recommended_next_action,
            "blocking_issue_count": len(normalized.blocking_issues),
            "active_issue_code": active_issue.issue_code if active_issue is not None else "",
        },
    )


def _render_progress_readiness_summary(
    assessment,
    *,
    clarification_request: ClarificationRequest,
    language: str = "en",
) -> str:
    active_question = clarification_request.questions[0] if clarification_request.questions else None
    active_issue = _select_active_readiness_issue(assessment.blocking_issues)

    if language == "zh":
        if active_question is not None:
            return (
                "当前还不能继续执行，我先确认最关键的一点：\n\n"
                f"{active_question.prompt}"
            )
        if active_issue is not None:
            return (
                "当前还不能继续执行，我先确认最关键的一点：\n\n"
                f"{active_issue.message}"
            )
        return "当前还不能继续执行，请先补充当前最关键的阻塞信息。"

    if active_question is not None:
        return (
            "Execution cannot continue yet. I need to confirm the most important blocker first:\n\n"
            f"{active_question.prompt}"
        )
    if active_issue is not None:
        return (
            "Execution cannot continue yet. I need to confirm the most important blocker first:\n\n"
            f"{active_issue.message}"
        )
    return "Execution cannot continue yet. Please clarify the current key blocker first."


def _select_active_readiness_issue(
    issues: list[Any],
):
    if not issues:
        return None

    def _score(issue) -> tuple[int, int, int]:
        issue_code = str(getattr(issue, "issue_code", "") or "").strip()
        details = getattr(issue, "details", {}) or {}
        question_kind = str(details.get("question_kind") or "").strip()
        evidence = list(getattr(issue, "evidence", []) or [])
        message = str(getattr(issue, "message", "") or "").strip()

        base_priority = {
            "group_semantics_undefined": 100,
            "analysis_goal_revision": 95,
            "contrast_definition": 90,
            "resource_role": 70,
            "analysis_family_ambiguity": 60,
            "blocking_ambiguity": 55,
            "capability_plan_open_questions": 45,
            "missing_information": 40,
        }.get(question_kind or issue_code, 10)

        if issue_code == "missing_contrast_definition":
            base_priority = max(base_priority, 90)
        if issue_code.startswith(("missing_", "ambiguous_")):
            base_priority = max(base_priority, 70)
        if issue_code == "group_semantics_undefined":
            base_priority = max(base_priority, 100)

        return (
            base_priority,
            len(evidence),
            len(message),
        )

    return max(issues, key=_score)


def _render_readiness_issue_prompt(issue, *, language: str = "en") -> str:
    if issue is None:
        return (
            "请补充当前最关键的阻塞信息。"
            if language == "zh"
            else "Please provide the current key blocker."
        )

    message = str(getattr(issue, "message", "") or "").strip()
    issue_code = str(getattr(issue, "issue_code", "") or "").strip()
    details = getattr(issue, "details", {}) or {}
    active_issue = details.get("active_issue") if isinstance(details.get("active_issue"), dict) else {}
    question_kind = str(details.get("question_kind") or "").strip()
    resource_role = str(details.get("resource_role") or "").strip()
    resource_status = str(details.get("resource_status") or "").strip()
    evidence = [str(item or "").strip() for item in list(getattr(issue, "evidence", []) or []) if str(item or "").strip()]

    if language == "zh":
        if question_kind == "analysis_goal_revision" or issue_code == "analysis_goal_revision_required":
            return _render_analysis_goal_revision_prompt(active_issue, language=language)
        if issue_code == "group_semantics_undefined":
            return "请先说明 R、H、S 分别代表什么生物学分组，以及本次差异分析要比较哪些组（例如 H vs R、S vs R）。"
        if question_kind == "contrast_definition" or issue_code == "missing_contrast_definition":
            return "请明确本次差异表达分析的对比设计：样本分组分别是什么，要比较哪几组？"
        if question_kind == "resource_role":
            return _render_resource_role_prompt(
                resource_role=resource_role,
                resource_status=resource_status,
                language=language,
            )
        if question_kind == "analysis_family_ambiguity":
            return "请先明确这次分析属于哪一类实验或科学目标，再继续推进。"
        if question_kind == "blocking_ambiguity":
            return "请先澄清当前最关键的项目歧义信息，再继续推进分析。"
        if question_kind == "capability_plan_open_questions" or issue_code == "capability_plan_open_questions":
            if evidence:
                return evidence[0]
            return "请先补充当前 capability plan 里最关键的一项待确认信息，系统会据此重写 CapabilityPlan。"
        if evidence:
            return evidence[0]
        return message or "请补充当前最关键的阻塞信息。"

    if issue_code == "group_semantics_undefined":
        return (
            "Please explain what the R, H, and S groups mean biologically, and which comparison(s) "
            "you want for differential expression, for example H vs R or S vs R."
        )
    if question_kind == "analysis_goal_revision" or issue_code == "analysis_goal_revision_required":
        return _render_analysis_goal_revision_prompt(active_issue, language=language)
    if question_kind == "contrast_definition" or issue_code == "missing_contrast_definition":
        return "Please define the differential-expression contrast: what are the groups, and which comparison(s) should be tested?"
    if question_kind == "resource_role":
        return _render_resource_role_prompt(
            resource_role=resource_role,
            resource_status=resource_status,
            language=language,
        )
    if question_kind == "analysis_family_ambiguity":
        return "Please clarify which assay or scientific goal this analysis belongs to before continuing."
    if question_kind == "blocking_ambiguity":
        return "Please clarify the current key project ambiguity before continuing the analysis."
    if question_kind == "capability_plan_open_questions" or issue_code == "capability_plan_open_questions":
        if evidence:
            return evidence[0]
        return "Please answer the current key open question in the capability plan, and the system will rewrite the CapabilityPlan."
    if evidence:
        return evidence[0]
    return message or "Please provide the current key blocker."


def _render_readiness_issue_hint(issue, *, language: str = "en") -> str:
    if issue is None:
        return (
            "请补充缺失信息，或说明你希望如何调整当前分析目标。"
            if language == "zh"
            else "Provide the missing information or explain how the current goal should be adjusted."
        )

    suggested_action = str(getattr(issue, "suggested_action", "") or "").strip()
    issue_code = str(getattr(issue, "issue_code", "") or "").strip()
    details = getattr(issue, "details", {}) or {}
    active_issue = details.get("active_issue") if isinstance(details.get("active_issue"), dict) else {}
    question_kind = str(details.get("question_kind") or "").strip()
    resource_role = str(details.get("resource_role") or "").strip()

    if language == "zh":
        if question_kind == "analysis_goal_revision" or issue_code == "analysis_goal_revision_required":
            expected_action = str(active_issue.get("expected_user_action") or "").strip()
            return expected_action or "你可以直接说这些当前不支持的分析内容不做，然后写出你现在真正希望继续的分析目标；系统会按你的最新表达来重写 AnalysisIntent。"
        if issue_code == "group_semantics_undefined":
            return "请直接写出每个分组的生物学含义，并明确本次要比较的组别。"
        if question_kind == "contrast_definition" or issue_code == "missing_contrast_definition":
            return "请直接写出分组定义和对比方案，例如“R=对照，H=处理1，S=处理2；先做 H vs R 和 S vs R”。"
        if question_kind == "resource_role":
            return _render_resource_role_hint(resource_role=resource_role, language=language)
        return suggested_action or "请补充缺失信息，或说明你希望如何调整当前分析目标。"

    if issue_code == "group_semantics_undefined":
        return "State the biological meaning of each group directly, and specify which comparison(s) should be tested."
    if question_kind == "analysis_goal_revision" or issue_code == "analysis_goal_revision_required":
        expected_action = str(active_issue.get("expected_user_action") or "").strip()
        return expected_action or "You can say directly that you do not want the currently unsupported analyses, then state the revised goal. The system will rewrite the AnalysisIntent from your latest instruction."
    if question_kind == "contrast_definition" or issue_code == "missing_contrast_definition":
        return "State the group definitions and the exact contrast(s), for example: R=control, H=treatment1, S=treatment2; test H vs R and S vs R."
    if question_kind == "resource_role":
        return _render_resource_role_hint(resource_role=resource_role, language=language)
    return suggested_action or "Provide the missing information or explain how the current goal should be adjusted."


def _render_analysis_goal_revision_prompt(
    active_issue: dict[str, Any] | None,
    *,
    language: str = "en",
) -> str:
    normalized = active_issue if isinstance(active_issue, dict) else {}
    title = str(normalized.get("title") or "").strip()
    description = str(normalized.get("description") or "").strip()
    user_options = [
        str(item or "").strip()
        for item in list(normalized.get("user_options") or [])
        if str(item or "").strip()
    ]
    expected_action = str(normalized.get("expected_user_action") or "").strip()

    if not title and not description and not user_options and not expected_action:
        return (
            "如果你不需要这些当前不支持的分析内容，可以直接说明这些分析不做。请基于当前项目，直接写出你现在真正想继续推进的分析目标，系统会据此重写 AnalysisIntent。"
            if language == "zh"
            else "If you do not need those currently unsupported analyses, say that directly. Please state the analysis goal you actually want to continue with for this project, and the system will rewrite the AnalysisIntent from your latest instruction."
        )

    option_header = "可选处理方式：" if language == "zh" else "Options:"
    lines: list[str] = []
    if title:
        lines.append(title)
    if description:
        lines.append(description)
    if user_options:
        lines.append(option_header)
        lines.extend(f"- {item}" for item in user_options)
    if expected_action:
        lines.append(expected_action)
    return "\n\n".join(lines)


def _render_resource_role_prompt(
    *,
    resource_role: str,
    resource_status: str,
    language: str = "en",
) -> str:
    if language == "zh":
        if resource_role in {"spliced_aligner_index", "dna_aligner_index", "transcript_index"}:
            if resource_status == "ambiguous":
                return "请确认应使用哪个索引目录。"
            return "请确认索引目录的真实路径。"
        if resource_role == "annotation_gtf":
            if resource_status == "ambiguous":
                return "请确认应使用哪个注释文件。"
            return "请确认注释文件的真实路径。"
        if resource_role == "reference_fasta":
            if resource_status == "ambiguous":
                return "请确认应使用哪个参考基因组文件。"
            return "请确认参考基因组文件的真实路径。"
        if resource_role == "reads":
            return "请确认本次分析应使用哪些测序 reads。"
        return "请确认当前缺失资源的具体信息。"

    if resource_role in {"spliced_aligner_index", "dna_aligner_index", "transcript_index"}:
        if resource_status == "ambiguous":
            return "Please confirm which index directory should be used."
        return "Please confirm the real path of the required index directory."
    if resource_role == "annotation_gtf":
        if resource_status == "ambiguous":
            return "Please confirm which annotation file should be used."
        return "Please confirm the real path of the annotation file."
    if resource_role == "reference_fasta":
        if resource_status == "ambiguous":
            return "Please confirm which reference genome file should be used."
        return "Please confirm the real path of the reference genome file."
    if resource_role == "reads":
        return "Please confirm which sequencing reads should be used for this analysis."
    return "Please confirm the current missing resource detail."


def _render_resource_role_hint(
    *,
    resource_role: str,
    language: str = "en",
) -> str:
    if language == "zh":
        if resource_role in {"spliced_aligner_index", "dna_aligner_index", "transcript_index"}:
            return "请直接提供索引目录路径；如果还没有，也请说明是否允许系统现在构建。"
        if resource_role in {"annotation_gtf", "reference_fasta"}:
            return "请直接提供文件路径；如果有多个候选，也请明确应使用哪一个。"
        if resource_role == "reads":
            return "请直接说明要使用的样本或文件集合。"
        return "请直接补充当前缺失资源的具体信息。"

    if resource_role in {"spliced_aligner_index", "dna_aligner_index", "transcript_index"}:
        return "Provide the directory path directly, or say whether the system should build it now."
    if resource_role in {"annotation_gtf", "reference_fasta"}:
        return "Provide the file path directly, or specify which candidate should be used."
    if resource_role == "reads":
        return "State which samples or files should be used."
    return "Provide the missing resource detail directly."


def build_resource_clarification_request(
    *,
    issues: list[dict[str, Any]],
    job_id: str = "",
    project_id: str = "",
    context_id: str = "",
    language: str = "en",
) -> ClarificationRequest:
    active_issue = _select_active_resource_issue(issues)
    return ClarificationRequest(
        request_id=context_id or job_id or "resource_clarification",
        request_type="resource_clarification",
        prompt=(
            "请先补充或确认当前这一项资源信息。"
            if language == "zh"
            else "Please provide or confirm the current resource detail."
        ),
        questions=(
            [_clarification_question_from_issue(active_issue, idx=0, language=language)]
            if active_issue is not None
            else []
        ),
        context={
            "job_id": job_id,
            "project_id": project_id,
            "context_id": context_id,
            "issue_count": len([issue for issue in issues if isinstance(issue, dict)]),
            "active_issue_id": str(active_issue.get("id") or "") if active_issue is not None else "",
            "active_issue_kind": str(active_issue.get("kind") or "") if active_issue is not None else "",
        },
    )


def _select_active_resource_issue(issues: list[dict[str, Any]]) -> dict[str, Any] | None:
    normalized_issues = [issue for issue in issues if isinstance(issue, dict)]
    if not normalized_issues:
        return None
    for issue in normalized_issues:
        if str(issue.get("severity") or "").strip() == "blocking":
            return issue
    return normalized_issues[0]


def _clarification_question_from_issue(
    issue: dict[str, Any],
    *,
    idx: int,
    language: str,
) -> ClarificationQuestion:
    resolution_type = str(issue.get("resolution_type") or "provide_path").strip() or "provide_path"
    response_kind = {
        "select_candidate": "select_option",
        "provide_path": "provide_path",
        "confirm_auto_build": "boolean",
        "link_experiment": "select_option",
    }.get(resolution_type, "free_text")
    candidates = issue.get("candidates") or []
    options = [
        ClarificationOption(
            option_id=f"candidate_{candidate_idx + 1}",
            label=_candidate_label(candidate),
            value=str(candidate.get("path") or candidate.get("experiment_id") or ""),
            description=_candidate_description(candidate),
        )
        for candidate_idx, candidate in enumerate(candidates)
        if isinstance(candidate, dict)
    ]
    if resolution_type == "confirm_auto_build":
        if language == "zh":
            options = [
                ClarificationOption(option_id="yes", label="允许自动构建", value="yes"),
                ClarificationOption(option_id="no", label="不要自动构建", value="no"),
            ]
        else:
            options = [
                ClarificationOption(option_id="yes", label="Allow auto-build", value="yes"),
                ClarificationOption(option_id="no", label="Do not auto-build", value="no"),
            ]
    return ClarificationQuestion(
        question_id=str(issue.get("id") or f"issue_{idx + 1}"),
        prompt=str(issue.get("title") or issue.get("description") or issue.get("kind") or "Clarify"),
        response_kind=response_kind,
        required=str(issue.get("severity") or "").strip() != "warning",
        allows_free_text=resolution_type in {"provide_path", "select_candidate", "link_experiment"},
        free_text_hint=_free_text_hint(resolution_type, language=language),
        options=options,
        context={
            "kind": str(issue.get("kind") or ""),
            "description": str(issue.get("description") or ""),
            "suggestion": str(issue.get("suggestion") or ""),
            "resolution_type": resolution_type,
            "binding_key": str(issue.get("binding_key") or ""),
            "affected_step_keys": list(issue.get("affected_step_keys") or []),
        },
    )


def _candidate_label(candidate: dict[str, Any]) -> str:
    label = str(candidate.get("path") or candidate.get("label") or "").strip()
    organism = str(candidate.get("organism") or "").strip()
    genome_build = str(candidate.get("genome_build") or "").strip()
    if organism and genome_build:
        return f"{label} [{organism}, {genome_build}]"
    if organism:
        return f"{label} [{organism}]"
    return label


def _candidate_description(candidate: dict[str, Any]) -> str:
    fragments = [
        str(candidate.get("source_type") or "").strip(),
    ]
    confidence = candidate.get("confidence")
    if confidence is not None and str(confidence) != "":
        fragments.append(f"confidence={confidence}")
    return "; ".join(fragment for fragment in fragments if fragment)


def _free_text_hint(resolution_type: str, *, language: str) -> str:
    if resolution_type == "provide_path":
        return (
            "请输入可以直接使用的资源路径。"
            if language == "zh"
            else "Provide a usable filesystem path."
        )
    if resolution_type == "select_candidate":
        return (
            "可以选择候选项，也可以直接提供其他路径。"
            if language == "zh"
            else "Choose one of the candidates or provide a different path."
        )
    if resolution_type == "link_experiment":
        return (
            "请选择实验，必要时补充读段方向。"
            if language == "zh"
            else "Choose the experiment, and include read direction if needed."
        )
    return ""


def _options_for(language: str, *, kind: str) -> list[DecisionOption]:
    if kind == "gap":
        if language == "zh":
            return [
                DecisionOption(option_id="clarify", label="补充信息", action="clarify"),
                DecisionOption(option_id="revise", label="修改目标", action="revise_goal"),
            ]
        return [
            DecisionOption(option_id="clarify", label="Clarify", action="clarify"),
            DecisionOption(option_id="revise", label="Revise goal", action="revise_goal"),
        ]
    if kind == "resource_clarification":
        if language == "zh":
            return [
                DecisionOption(option_id="provide_info", label="补充资源信息", action="provide_clarification"),
                DecisionOption(option_id="cancel", label="取消", action="cancel"),
            ]
        return [
            DecisionOption(option_id="provide_info", label="Provide info", action="provide_clarification"),
            DecisionOption(option_id="cancel", label="Cancel", action="cancel"),
        ]
    if kind == "authorization":
        if language == "zh":
            return [
                DecisionOption(option_id="approve", label="批准", action="approve"),
                DecisionOption(option_id="reject", label="拒绝", action="reject"),
            ]
        return [
            DecisionOption(option_id="approve", label="Approve", action="approve"),
            DecisionOption(option_id="reject", label="Reject", action="reject"),
        ]
    if kind == "repair":
        if language == "zh":
            return [
                DecisionOption(option_id="repair", label="提供修复方案", action="submit_repair"),
                DecisionOption(option_id="stop", label="停止执行", action="stop_execution"),
            ]
        return [
            DecisionOption(option_id="repair", label="Provide repair", action="submit_repair"),
            DecisionOption(option_id="stop", label="Stop execution", action="stop_execution"),
        ]
    if language == "zh":
        return [
            DecisionOption(option_id="confirm", label="确认", action="confirm"),
            DecisionOption(option_id="modify", label="修改计划", action="modify_plan"),
            DecisionOption(option_id="cancel", label="取消", action="cancel"),
        ]
    return [
        DecisionOption(option_id="confirm", label="Confirm", action="confirm"),
        DecisionOption(option_id="modify", label="Modify plan", action="modify_plan"),
        DecisionOption(option_id="cancel", label="Cancel", action="cancel"),
    ]
