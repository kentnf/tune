"""Project-scoped execution event memory."""
from __future__ import annotations

import logging
import re
import uuid
from typing import Any
from collections import Counter, defaultdict

log = logging.getLogger(__name__)

_SAFE_ACTION_RE = re.compile(r"\bsafe_action=([a-z_]+)\b")
_SAFE_ACTION_APPLIED_RE = re.compile(r"Applied safe action '([a-z_]+)'")
_ROLLBACK_LEVEL_RE = re.compile(r"\brollback_level=([a-z_]+)\b")
_ROLLBACK_TARGET_RE = re.compile(r"\brollback_target=([A-Za-z0-9_:-]+)\b")
_BINDING_KEY_RE = re.compile(r"\bbinding_key=([A-Za-z0-9_:-]+)\b")
_PATH_RE = re.compile(r"'([^']+)'")
_FILE_ID_RE = re.compile(r"\bfile_id=([A-Za-z0-9_:-]+)\b")
_EXPERIMENT_ID_RE = re.compile(r"\bexperiment_id=([A-Za-z0-9_:-]+)\b")
_READ_NUMBER_RE = re.compile(r"\bread_number=([A-Za-z0-9_:-]+)\b")
_ANALYSIS_FAMILY_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("transcriptomics", ("rna-seq", "rnaseq", "deseq2", "featurecounts", "salmon", "transcript")),
    ("small_rna", ("mirna", "small-rna", "small rna")),
    ("epigenomics", ("chip-seq", "chipseq", "atac-seq", "atacseq", "macs2", "peak")),
    ("genomics", ("wgs", "wes", "variant", "vcf", "bcftools", "gatk", "amplicon")),
    ("proteomics", ("proteomics", "proteome", "mass spectrometry")),
    ("metabolomics", ("metabolomics", "metabolome")),
)


def _extract_safe_action(resolution: str | None) -> str | None:
    text = str(resolution or "").strip()
    if not text:
        return None
    match = _SAFE_ACTION_APPLIED_RE.search(text)
    if match:
        return match.group(1)
    match = _SAFE_ACTION_RE.search(text)
    if match:
        return match.group(1)
    return None


def _extract_rollback_level(resolution: str | None) -> str | None:
    text = str(resolution or "").strip()
    if not text:
        return None
    match = _ROLLBACK_LEVEL_RE.search(text)
    if match:
        return match.group(1)
    return None


def _extract_rollback_target(resolution: str | None) -> str | None:
    text = str(resolution or "").strip()
    if not text:
        return None
    match = _ROLLBACK_TARGET_RE.search(text)
    if match:
        return match.group(1)
    return None


def _infer_analysis_family(*parts: str | None) -> str | None:
    text = " ".join(str(part or "").strip().lower() for part in parts if str(part or "").strip())
    if not text:
        return None
    for family, tokens in _ANALYSIS_FAMILY_RULES:
        if any(token in text for token in tokens):
            return family
    return None


def _serialize_execution_event(event) -> dict:
    if isinstance(event, dict):
        payload = dict(event)
        resolution = str(payload.get("resolution") or "").strip()
        description = str(payload.get("description") or "").strip()
        event_type = str(payload.get("event_type") or "").strip()
        created_at = payload.get("created_at")
        payload.setdefault("id", payload.get("id"))
        payload.setdefault("project_id", payload.get("project_id"))
        payload["event_type"] = event_type
        payload["description"] = description
        payload["resolution"] = resolution
        payload.setdefault("safe_action", _extract_safe_action(resolution))
        payload.setdefault("rollback_level", _extract_rollback_level(resolution))
        payload.setdefault("rollback_target", _extract_rollback_target(resolution))
        payload.setdefault(
            "analysis_family",
            _infer_analysis_family(description, resolution, event_type),
        )
        payload["user_contributed"] = bool(payload.get("user_contributed", False))
        payload["created_at"] = created_at.isoformat() if hasattr(created_at, "isoformat") else created_at
        return payload

    resolution = str(getattr(event, "resolution", None) or "").strip()
    description = str(getattr(event, "description", None) or "").strip()
    event_type = str(getattr(event, "event_type", None) or "").strip()
    user_contributed = bool(getattr(event, "user_contributed", False))
    created_at = getattr(event, "created_at", None)
    return {
        "id": getattr(event, "id", None),
        "project_id": getattr(event, "project_id", None),
        "event_type": event_type,
        "description": description,
        "resolution": resolution,
        "safe_action": _extract_safe_action(resolution),
        "rollback_level": _extract_rollback_level(resolution),
        "rollback_target": _extract_rollback_target(resolution),
        "analysis_family": _infer_analysis_family(description, resolution, event_type),
        "user_contributed": user_contributed,
        "created_at": created_at.isoformat() if created_at else None,
    }


def extract_project_memory_facts(events: list[object | dict] | None, *, project_id: str | None = None) -> list[dict]:
    """Extract deterministic memory facts from project execution events.

    This is a logic-only bridge until dedicated memory_facts persistence exists.
    Facts are derived conservatively from user-confirmed clarification outcomes.
    """
    facts_by_key: dict[str, dict] = {}

    for event in events or []:
        payload = _serialize_execution_event(event)
        resolved_project_id = project_id or payload.get("project_id")
        event_type = str(payload.get("event_type") or "").strip()
        resolution = str(payload.get("resolution") or "").strip()
        description = str(payload.get("description") or "").strip()
        if event_type != "resource_clarification_resolved" or not resolution:
            continue

        binding_key_match = _BINDING_KEY_RE.search(resolution)
        quoted_paths = _PATH_RE.findall(resolution)
        if binding_key_match and quoted_paths:
            binding_key = binding_key_match.group(1)
            path = quoted_paths[0]
            fact_key = f"resource_binding:{binding_key}"
            facts_by_key[fact_key] = {
                "project_id": resolved_project_id,
                "fact_key": fact_key,
                "fact_type": "resource_decision",
                "title": f"Resolved resource binding {binding_key}",
                "statement": (
                    f"Use path '{path}' for binding key '{binding_key}' based on user-confirmed clarification."
                ),
                "binding_key": binding_key,
                "path": path,
                "source_event_type": event_type,
                "description": description,
            }
            continue

        file_id_match = _FILE_ID_RE.search(resolution)
        experiment_id_match = _EXPERIMENT_ID_RE.search(resolution)
        if file_id_match and experiment_id_match:
            file_id = file_id_match.group(1)
            experiment_id = experiment_id_match.group(1)
            read_number_match = _READ_NUMBER_RE.search(resolution)
            read_number = read_number_match.group(1) if read_number_match else None
            fact_key = f"experiment_link:{file_id}"
            facts_by_key[fact_key] = {
                "project_id": resolved_project_id,
                "fact_key": fact_key,
                "fact_type": "experiment_link_decision",
                "title": f"Resolved experiment linkage {file_id}",
                "statement": (
                    f"Link file '{file_id}' to experiment '{experiment_id}'"
                    + (f" with read_number '{read_number}'." if read_number else ".")
                ),
                "file_id": file_id,
                "experiment_id": experiment_id,
                "read_number": read_number,
                "source_event_type": event_type,
                "description": description,
            }

    return sorted(
        facts_by_key.values(),
        key=lambda item: (str(item.get("fact_type") or ""), str(item.get("fact_key") or "")),
    )


def build_project_memory_profile(events: list[object | dict] | None) -> dict:
    items: list[dict] = []
    for event in events or []:
        items.append(_serialize_execution_event(event))

    if not items:
        return {
            "episode_count": 0,
            "event_type_counts": {},
            "analysis_family_counts": {},
            "safe_action_patterns": [],
            "rollback_patterns": [],
            "preferences": {},
            "user_validated_episode_count": 0,
        }

    event_type_counts: Counter[str] = Counter()
    analysis_family_counts: Counter[str] = Counter()
    safe_action_counts: Counter[str] = Counter()
    rollback_level_counts: Counter[str] = Counter()
    safe_action_user_counts: Counter[str] = Counter()
    rollback_level_user_counts: Counter[str] = Counter()
    safe_action_meta: dict[str, dict[str, Counter[str]]] = defaultdict(
        lambda: {
            "incident_types": Counter(),
            "rollback_levels": Counter(),
            "analysis_families": Counter(),
        }
    )
    rollback_meta: dict[str, dict[str, Counter[str]]] = defaultdict(
        lambda: {
            "safe_actions": Counter(),
            "analysis_families": Counter(),
            "incident_types": Counter(),
        }
    )
    user_validated_episode_count = 0

    for item in items:
        event_type = str(item.get("event_type") or "").strip()
        safe_action = str(item.get("safe_action") or "").strip()
        rollback_level = str(item.get("rollback_level") or "").strip()
        analysis_family = str(item.get("analysis_family") or "").strip()
        user_contributed = bool(item.get("user_contributed"))
        if event_type:
            event_type_counts[event_type] += 1
        if analysis_family:
            analysis_family_counts[analysis_family] += 1
        if safe_action:
            safe_action_counts[safe_action] += 1
            if event_type:
                safe_action_meta[safe_action]["incident_types"][event_type] += 1
            if rollback_level:
                safe_action_meta[safe_action]["rollback_levels"][rollback_level] += 1
            if analysis_family:
                safe_action_meta[safe_action]["analysis_families"][analysis_family] += 1
        if rollback_level:
            rollback_level_counts[rollback_level] += 1
            if safe_action:
                rollback_meta[rollback_level]["safe_actions"][safe_action] += 1
            if analysis_family:
                rollback_meta[rollback_level]["analysis_families"][analysis_family] += 1
            if event_type:
                rollback_meta[rollback_level]["incident_types"][event_type] += 1
        if user_contributed:
            user_validated_episode_count += 1
            if safe_action:
                safe_action_user_counts[safe_action] += 1
            if rollback_level:
                rollback_level_user_counts[rollback_level] += 1

    def _top_list(counter: Counter[str], *, limit: int = 3) -> list[str]:
        return [key for key, _count in counter.most_common(limit)]

    safe_action_patterns = [
        {
            "safe_action": safe_action,
            "support_count": count,
            "incident_types": _top_list(safe_action_meta[safe_action]["incident_types"]),
            "rollback_levels": _top_list(safe_action_meta[safe_action]["rollback_levels"]),
            "analysis_families": _top_list(safe_action_meta[safe_action]["analysis_families"]),
            "user_validated_count": int(safe_action_user_counts.get(safe_action, 0)),
        }
        for safe_action, count in safe_action_counts.most_common(3)
    ]
    rollback_patterns = [
        {
            "rollback_level": rollback_level,
            "support_count": count,
            "safe_actions": _top_list(rollback_meta[rollback_level]["safe_actions"]),
            "analysis_families": _top_list(rollback_meta[rollback_level]["analysis_families"]),
            "incident_types": _top_list(rollback_meta[rollback_level]["incident_types"]),
            "user_validated_count": int(rollback_level_user_counts.get(rollback_level, 0)),
        }
        for rollback_level, count in rollback_level_counts.most_common(3)
    ]

    preferences: dict[str, object] = {}
    if safe_action_user_counts:
        preferred_safe_action, support_count = safe_action_user_counts.most_common(1)[0]
        preferences["preferred_safe_action"] = preferred_safe_action
        preferences["preferred_safe_action_basis"] = "user_validated"
        preferences["preferred_safe_action_support"] = support_count
    elif safe_action_counts:
        preferred_safe_action, support_count = safe_action_counts.most_common(1)[0]
        preferences["preferred_safe_action"] = preferred_safe_action
        preferences["preferred_safe_action_basis"] = "project_history"
        preferences["preferred_safe_action_support"] = support_count
    if rollback_level_user_counts:
        preferred_rollback_level, support_count = rollback_level_user_counts.most_common(1)[0]
        preferences["preferred_rollback_level"] = preferred_rollback_level
        preferences["preferred_rollback_level_basis"] = "user_validated"
        preferences["preferred_rollback_level_support"] = support_count
    elif rollback_level_counts:
        preferred_rollback_level, support_count = rollback_level_counts.most_common(1)[0]
        preferences["preferred_rollback_level"] = preferred_rollback_level
        preferences["preferred_rollback_level_basis"] = "project_history"
        preferences["preferred_rollback_level_support"] = support_count
    if analysis_family_counts:
        preferred_analysis_family, support_count = analysis_family_counts.most_common(1)[0]
        preferences["preferred_analysis_family"] = preferred_analysis_family
        preferences["preferred_analysis_family_support"] = support_count

    return {
        "episode_count": len(items),
        "event_type_counts": dict(event_type_counts),
        "analysis_family_counts": dict(analysis_family_counts),
        "safe_action_patterns": safe_action_patterns,
        "rollback_patterns": rollback_patterns,
        "preferences": preferences,
        "user_validated_episode_count": user_validated_episode_count,
    }


def build_project_memory_patterns(profile: dict | None) -> list[dict]:
    payload = dict(profile or {})
    entries: list[dict] = []

    for pattern in list(payload.get("safe_action_patterns") or []):
        safe_action = str(pattern.get("safe_action") or "").strip()
        if not safe_action:
            continue
        support_count = int(pattern.get("support_count") or 0)
        user_validated_count = int(pattern.get("user_validated_count") or 0)
        confidence = "high" if support_count >= 3 else "medium" if support_count >= 2 else "low"
        entries.append(
            {
                "memory_layer": "memory_patterns",
                "pattern_type": "safe_action",
                "pattern_key": f"safe_action:{safe_action}",
                "title": f"Safe action pattern {safe_action}",
                "recommended_value": safe_action,
                "support_count": support_count,
                "user_validated_count": user_validated_count,
                "confidence": confidence,
                "incident_types": list(pattern.get("incident_types") or []),
                "rollback_levels": list(pattern.get("rollback_levels") or []),
                "analysis_families": list(pattern.get("analysis_families") or []),
            }
        )

    for pattern in list(payload.get("rollback_patterns") or []):
        rollback_level = str(pattern.get("rollback_level") or "").strip()
        if not rollback_level:
            continue
        support_count = int(pattern.get("support_count") or 0)
        user_validated_count = int(pattern.get("user_validated_count") or 0)
        confidence = "high" if support_count >= 3 else "medium" if support_count >= 2 else "low"
        entries.append(
            {
                "memory_layer": "memory_patterns",
                "pattern_type": "rollback_level",
                "pattern_key": f"rollback_level:{rollback_level}",
                "title": f"Rollback pattern {rollback_level}",
                "recommended_value": rollback_level,
                "support_count": support_count,
                "user_validated_count": user_validated_count,
                "confidence": confidence,
                "safe_actions": list(pattern.get("safe_actions") or []),
                "incident_types": list(pattern.get("incident_types") or []),
                "analysis_families": list(pattern.get("analysis_families") or []),
            }
        )

    return entries


def build_project_memory_preferences(profile: dict | None) -> list[dict]:
    payload = dict(profile or {})
    preferences = dict(payload.get("preferences") or {})
    entries: list[dict] = []

    preference_specs = [
        ("preferred_safe_action", "safe_action", "Preferred safe action"),
        ("preferred_rollback_level", "rollback_level", "Preferred rollback level"),
        ("preferred_analysis_family", "analysis_family", "Preferred analysis family"),
    ]

    for key, preference_type, title in preference_specs:
        value = str(preferences.get(key) or "").strip()
        if not value:
            continue
        basis = str(preferences.get(f"{key}_basis") or "").strip() or None
        support_count = int(preferences.get(f"{key}_support") or 0)
        confidence = "high" if support_count >= 3 else "medium" if support_count >= 2 else "low"
        entries.append(
            {
                "memory_layer": "memory_preferences",
                "preference_key": key,
                "preference_type": preference_type,
                "title": title,
                "value": value,
                "basis": basis,
                "support_count": support_count,
                "confidence": confidence,
            }
        )

    return entries


def _memory_pattern_row_to_payload(row: Any) -> dict[str, Any]:
    payload = dict(getattr(row, "payload_json", None) or {})
    payload.setdefault("id", getattr(row, "id", None))
    payload["project_id"] = getattr(row, "project_id", None)
    payload["memory_layer"] = "memory_patterns"
    payload["pattern_key"] = getattr(row, "pattern_key", None)
    payload["pattern_type"] = getattr(row, "pattern_type", None)
    payload["title"] = getattr(row, "title", None)
    payload["recommended_value"] = getattr(row, "recommended_value", None)
    payload["support_count"] = getattr(row, "support_count", None)
    payload["user_validated_count"] = getattr(row, "user_validated_count", None)
    payload["confidence"] = getattr(row, "confidence", None)
    payload["source_episode_id"] = getattr(row, "source_episode_id", None)
    return payload


def _memory_preference_row_to_payload(row: Any) -> dict[str, Any]:
    payload = dict(getattr(row, "payload_json", None) or {})
    payload.setdefault("id", getattr(row, "id", None))
    payload["project_id"] = getattr(row, "project_id", None)
    payload["memory_layer"] = "memory_preferences"
    payload["preference_key"] = getattr(row, "preference_key", None)
    payload["preference_type"] = getattr(row, "preference_type", None)
    payload["title"] = getattr(row, "title", None)
    payload["value"] = getattr(row, "value", None)
    payload["basis"] = getattr(row, "basis", None)
    payload["support_count"] = getattr(row, "support_count", None)
    payload["confidence"] = getattr(row, "confidence", None)
    payload["source_episode_id"] = getattr(row, "source_episode_id", None)
    return payload


def _memory_link_row_to_payload(row: Any) -> dict[str, Any]:
    return {
        "id": getattr(row, "id", None),
        "project_id": getattr(row, "project_id", None),
        "memory_type": getattr(row, "memory_type", None),
        "memory_id": getattr(row, "memory_id", None),
        "entity_type": getattr(row, "entity_type", None),
        "entity_id": getattr(row, "entity_id", None),
        "link_role": getattr(row, "link_role", None),
        "strength": getattr(row, "strength", None),
        "last_confirmed_at": (
            getattr(row, "last_confirmed_at", None).isoformat()
            if getattr(row, "last_confirmed_at", None) is not None
            and hasattr(getattr(row, "last_confirmed_at", None), "isoformat")
            else getattr(row, "last_confirmed_at", None)
        ),
    }


def _memory_fact_row_to_payload(row: Any) -> dict[str, Any]:
    payload = dict(getattr(row, "payload_json", None) or {})
    payload.setdefault("id", getattr(row, "id", None))
    payload["project_id"] = getattr(row, "project_id", None)
    payload["fact_key"] = getattr(row, "fact_key", None)
    payload["fact_type"] = getattr(row, "fact_type", None)
    payload["title"] = getattr(row, "title", None)
    payload["statement"] = getattr(row, "statement", None)
    payload["source_event_id"] = getattr(row, "source_event_id", None)
    payload["source_episode_id"] = getattr(row, "source_episode_id", None)
    return payload


async def query_project_memory_facts(session, project_id: str, limit: int = 20) -> list[dict]:
    from sqlalchemy import desc, select

    from tune.core.models import MemoryFact

    try:
        rows = (
            await session.execute(
                select(MemoryFact)
                .where(MemoryFact.project_id == project_id)
                .order_by(desc(MemoryFact.updated_at), desc(MemoryFact.created_at))
                .limit(limit)
            )
        ).scalars().all()
        if rows:
            return [_memory_fact_row_to_payload(row) for row in rows]
    except Exception:
        log.debug("MemoryFact query unavailable for project %s; falling back to derived facts", project_id, exc_info=True)
        try:
            await session.rollback()
        except Exception:
            log.debug("MemoryFact fallback rollback failed for project %s", project_id, exc_info=True)

    events = await query_recent_project_events(session, project_id, limit=limit)
    return extract_project_memory_facts(events, project_id=project_id)


async def query_project_memory_patterns(session, project_id: str, limit: int = 20) -> list[dict]:
    from sqlalchemy import desc, select

    from tune.core.models import MemoryPattern

    try:
        rows = (
            await session.execute(
                select(MemoryPattern)
                .where(MemoryPattern.project_id == project_id)
                .order_by(
                    desc(MemoryPattern.support_count),
                    desc(MemoryPattern.updated_at),
                    desc(MemoryPattern.created_at),
                )
                .limit(limit)
            )
        ).scalars().all()
        if rows:
            return [_memory_pattern_row_to_payload(row) for row in rows]
    except Exception:
        log.debug(
            "MemoryPattern query unavailable for project %s; falling back to derived patterns",
            project_id,
            exc_info=True,
        )
        try:
            await session.rollback()
        except Exception:
            log.debug("MemoryPattern fallback rollback failed for project %s", project_id, exc_info=True)

    profile = await summarize_project_memory(session, project_id, limit=max(limit, 20))
    return build_project_memory_patterns(profile)[:limit]


async def query_project_memory_preferences(session, project_id: str, limit: int = 20) -> list[dict]:
    from sqlalchemy import desc, select

    from tune.core.models import MemoryPreference

    try:
        rows = (
            await session.execute(
                select(MemoryPreference)
                .where(MemoryPreference.project_id == project_id)
                .order_by(
                    desc(MemoryPreference.support_count),
                    desc(MemoryPreference.updated_at),
                    desc(MemoryPreference.created_at),
                )
                .limit(limit)
            )
        ).scalars().all()
        if rows:
            return [_memory_preference_row_to_payload(row) for row in rows]
    except Exception:
        log.debug(
            "MemoryPreference query unavailable for project %s; falling back to derived preferences",
            project_id,
            exc_info=True,
        )
        try:
            await session.rollback()
        except Exception:
            log.debug("MemoryPreference fallback rollback failed for project %s", project_id, exc_info=True)

    profile = await summarize_project_memory(session, project_id, limit=max(limit, 20))
    return build_project_memory_preferences(profile)[:limit]


async def query_project_memory_links(
    session,
    project_id: str,
    *,
    memory_type: str | None = None,
    limit: int = 50,
) -> list[dict]:
    from sqlalchemy import desc, select

    from tune.core.models import MemoryLink

    try:
        statement = select(MemoryLink).where(MemoryLink.project_id == project_id)
        if memory_type:
            statement = statement.where(MemoryLink.memory_type == memory_type)
        rows = (
            await session.execute(
                statement
                .order_by(desc(MemoryLink.updated_at), desc(MemoryLink.created_at))
                .limit(limit)
            )
        ).scalars().all()
        return [_memory_link_row_to_payload(row) for row in rows]
    except Exception:
        log.debug(
            "MemoryLink query unavailable for project %s; returning empty link set",
            project_id,
            exc_info=True,
        )
        try:
            await session.rollback()
        except Exception:
            log.debug("MemoryLink fallback rollback failed for project %s", project_id, exc_info=True)
        return []


async def query_project_events(session, project_id: str, query_text: str, top_k: int = 3):
    """Return top-k ProjectExecutionEvent entries for the project, relevant to query_text."""
    from sqlalchemy import desc, select

    from tune.core.memory.global_memory import embed_text
    from tune.core.models import ProjectExecutionEvent

    query_embedding = await embed_text(query_text)
    if query_embedding is not None:
        results = (
            await session.execute(
                select(ProjectExecutionEvent)
                .where(
                    ProjectExecutionEvent.project_id == project_id,
                    ProjectExecutionEvent.embedding.isnot(None),
                )
                .order_by(
                    ProjectExecutionEvent.embedding.cosine_distance(query_embedding)
                )
                .limit(top_k)
            )
        ).scalars().all()
        if not results:
            results = (
                await session.execute(
                    select(ProjectExecutionEvent)
                    .where(ProjectExecutionEvent.project_id == project_id)
                    .order_by(desc(ProjectExecutionEvent.created_at))
                    .limit(top_k)
                )
            ).scalars().all()
    else:
        results = (
            await session.execute(
                select(ProjectExecutionEvent)
                .where(ProjectExecutionEvent.project_id == project_id)
                .order_by(desc(ProjectExecutionEvent.created_at))
                .limit(top_k)
            )
        ).scalars().all()

    return results


async def query_recent_project_episodes(session, project_id: str, limit: int = 20):
    from sqlalchemy import desc, select

    from tune.core.models import MemoryEpisode

    try:
        rows = (
            await session.execute(
                select(MemoryEpisode)
                .where(MemoryEpisode.project_id == project_id)
                .order_by(desc(MemoryEpisode.created_at))
                .limit(limit)
            )
        ).scalars().all()
        if rows:
            return rows
    except Exception:
        log.debug("MemoryEpisode query unavailable for project %s; falling back to execution events", project_id, exc_info=True)
        try:
            await session.rollback()
        except Exception:
            log.debug("MemoryEpisode fallback rollback failed for project %s", project_id, exc_info=True)

    return await query_recent_project_events(session, project_id, limit=limit)


async def query_recent_project_events(session, project_id: str, limit: int = 20):
    from sqlalchemy import desc, select

    from tune.core.models import ProjectExecutionEvent

    return (
        await session.execute(
            select(ProjectExecutionEvent)
            .where(ProjectExecutionEvent.project_id == project_id)
            .order_by(desc(ProjectExecutionEvent.created_at))
            .limit(limit)
        )
    ).scalars().all()


async def summarize_project_memory(session, project_id: str, limit: int = 20) -> dict:
    if not project_id:
        return build_project_memory_profile([])
    try:
        rows = await query_recent_project_episodes(session, project_id, limit=limit)
    except Exception:
        log.exception("Failed to summarize project memory for project %s", project_id)
        return build_project_memory_profile([])
    return build_project_memory_profile(rows)


async def sync_project_memory_layers(
    session,
    project_id: str,
    *,
    source_episode_id: str | None = None,
    limit: int = 50,
) -> dict[str, list[dict]]:
    from sqlalchemy import delete
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    from tune.core.models import MemoryPattern, MemoryPreference

    if not project_id:
        return {"memory_patterns": [], "memory_preferences": []}

    events = await query_recent_project_events(session, project_id, limit=limit)
    profile = build_project_memory_profile(events)
    patterns = build_project_memory_patterns(profile)
    preferences = build_project_memory_preferences(profile)

    pattern_keys = {
        str(item.get("pattern_key") or "").strip()
        for item in patterns
        if str(item.get("pattern_key") or "").strip()
    }
    preference_keys = {
        str(item.get("preference_key") or "").strip()
        for item in preferences
        if str(item.get("preference_key") or "").strip()
    }

    pattern_id_by_key: dict[str, str] = {}
    preference_id_by_key: dict[str, str] = {}

    try:
        for pattern in patterns:
            pattern_key = str(pattern.get("pattern_key") or "").strip()
            if not pattern_key:
                continue
            inserted_id = str(uuid.uuid4())
            payload_json = {
                key: value
                for key, value in pattern.items()
                if key
                not in {
                    "id",
                    "project_id",
                    "memory_layer",
                    "pattern_key",
                    "pattern_type",
                    "title",
                    "recommended_value",
                    "support_count",
                    "user_validated_count",
                    "confidence",
                    "source_episode_id",
                }
            }
            result = await session.execute(
                pg_insert(MemoryPattern)
                .values(
                    id=inserted_id,
                    project_id=project_id,
                    pattern_key=pattern_key,
                    pattern_type=str(pattern.get("pattern_type") or "pattern"),
                    title=str(pattern.get("title") or pattern_key),
                    recommended_value=str(pattern.get("recommended_value") or ""),
                    support_count=int(pattern.get("support_count") or 0),
                    user_validated_count=int(pattern.get("user_validated_count") or 0),
                    confidence=str(pattern.get("confidence") or "").strip() or None,
                    payload_json=payload_json or None,
                    source_episode_id=source_episode_id,
                )
                .on_conflict_do_update(
                    constraint="uq_memory_patterns_project_pattern_key",
                    set_={
                        "pattern_type": str(pattern.get("pattern_type") or "pattern"),
                        "title": str(pattern.get("title") or pattern_key),
                        "recommended_value": str(pattern.get("recommended_value") or ""),
                        "support_count": int(pattern.get("support_count") or 0),
                        "user_validated_count": int(pattern.get("user_validated_count") or 0),
                        "confidence": str(pattern.get("confidence") or "").strip() or None,
                        "payload_json": payload_json or None,
                        "source_episode_id": source_episode_id,
                    },
                )
                .returning(MemoryPattern.id)
            )
            pattern_id_by_key[pattern_key] = _scalar_result_or_default(result, inserted_id)

        for preference in preferences:
            preference_key = str(preference.get("preference_key") or "").strip()
            if not preference_key:
                continue
            inserted_id = str(uuid.uuid4())
            payload_json = {
                key: value
                for key, value in preference.items()
                if key
                not in {
                    "id",
                    "project_id",
                    "memory_layer",
                    "preference_key",
                    "preference_type",
                    "title",
                    "value",
                    "basis",
                    "support_count",
                    "confidence",
                    "source_episode_id",
                }
            }
            result = await session.execute(
                pg_insert(MemoryPreference)
                .values(
                    id=inserted_id,
                    project_id=project_id,
                    preference_key=preference_key,
                    preference_type=str(preference.get("preference_type") or "preference"),
                    title=str(preference.get("title") or preference_key),
                    value=str(preference.get("value") or ""),
                    basis=str(preference.get("basis") or "").strip() or None,
                    support_count=int(preference.get("support_count") or 0),
                    confidence=str(preference.get("confidence") or "").strip() or None,
                    payload_json=payload_json or None,
                    source_episode_id=source_episode_id,
                )
                .on_conflict_do_update(
                    constraint="uq_memory_preferences_project_preference_key",
                    set_={
                        "preference_type": str(preference.get("preference_type") or "preference"),
                        "title": str(preference.get("title") or preference_key),
                        "value": str(preference.get("value") or ""),
                        "basis": str(preference.get("basis") or "").strip() or None,
                        "support_count": int(preference.get("support_count") or 0),
                        "confidence": str(preference.get("confidence") or "").strip() or None,
                        "payload_json": payload_json or None,
                        "source_episode_id": source_episode_id,
                    },
                )
                .returning(MemoryPreference.id)
            )
            preference_id_by_key[preference_key] = _scalar_result_or_default(result, inserted_id)

        if pattern_keys:
            await session.execute(
                delete(MemoryPattern).where(
                    MemoryPattern.project_id == project_id,
                    MemoryPattern.pattern_key.not_in(sorted(pattern_keys)),
                )
            )
        else:
            await session.execute(delete(MemoryPattern).where(MemoryPattern.project_id == project_id))

        if preference_keys:
            await session.execute(
                delete(MemoryPreference).where(
                    MemoryPreference.project_id == project_id,
                    MemoryPreference.preference_key.not_in(sorted(preference_keys)),
                )
            )
        else:
            await session.execute(delete(MemoryPreference).where(MemoryPreference.project_id == project_id))
    except Exception:
        log.debug(
            "Structured memory layer sync unavailable for project %s; keeping derived layers only",
            project_id,
            exc_info=True,
        )
        return {
            "memory_patterns": patterns,
            "memory_preferences": preferences,
            "pattern_id_by_key": {},
            "preference_id_by_key": {},
        }

    return {
        "memory_patterns": patterns,
        "memory_preferences": preferences,
        "pattern_id_by_key": pattern_id_by_key,
        "preference_id_by_key": preference_id_by_key,
    }


def _scalar_result_or_default(result: Any, default: str) -> str:
    try:
        scalar = result.scalar_one_or_none()
        if scalar:
            return str(scalar)
    except Exception:
        pass
    return default


def _normalize_linked_entity_ids(value: Any) -> list[str]:
    ids: list[str] = []

    def _append(candidate: Any) -> None:
        text = str(candidate or "").strip()
        if text:
            ids.append(text)

    if value is None:
        return ids
    if isinstance(value, str):
        _append(value)
        return ids
    if isinstance(value, dict):
        for key in (
            "id",
            "entity_id",
            "resource_entity_id",
            "artifact_record_id",
        ):
            if value.get(key):
                _append(value.get(key))
        for key in (
            "ids",
            "entity_ids",
            "resource_entity_ids",
            "artifact_record_ids",
        ):
            nested = value.get(key)
            if nested:
                ids.extend(_normalize_linked_entity_ids(nested))
        return ids
    if isinstance(value, (list, tuple, set)):
        for item in value:
            ids.extend(_normalize_linked_entity_ids(item))
        return ids
    _append(value)
    return ids


def _dedupe_preserving_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _collect_memory_link_targets(
    metadata_json: dict[str, Any] | None,
    *,
    explicit_resource_entity_ids: list[str] | None = None,
    explicit_artifact_record_ids: list[str] | None = None,
) -> dict[str, list[dict[str, str]]]:
    resource_roles: dict[str, list[str]] = defaultdict(list)
    artifact_roles: dict[str, list[str]] = defaultdict(list)

    def _append_targets(target_map: dict[str, list[str]], role: str, raw_value: Any) -> None:
        for entity_id in _normalize_linked_entity_ids(raw_value):
            target_map[role].append(entity_id)

    if explicit_resource_entity_ids:
        _append_targets(resource_roles, "context_resource", explicit_resource_entity_ids)
    if explicit_artifact_record_ids:
        _append_targets(artifact_roles, "context_artifact", explicit_artifact_record_ids)

    for key, value in dict(metadata_json or {}).items():
        key_lower = str(key or "").strip().lower()
        if not key_lower or value is None:
            continue

        if "resource_entity" in key_lower or key_lower == "resource_entities":
            role = "context_resource"
            if "resolved" in key_lower:
                role = "resolved_resource"
            elif "candidate" in key_lower or "ambigu" in key_lower:
                role = "candidate_resource"
            _append_targets(resource_roles, role, value)
            continue

        if "artifact_record" in key_lower or key_lower == "artifact_records":
            role = "context_artifact"
            if any(token in key_lower for token in ("produced", "output", "result")):
                role = "produced_artifact"
            _append_targets(artifact_roles, role, value)

    return {
        "resource_targets": [
            {"entity_id": entity_id, "link_role": role}
            for role, entity_ids in resource_roles.items()
            for entity_id in _dedupe_preserving_order(entity_ids)
        ],
        "artifact_targets": [
            {"entity_id": entity_id, "link_role": role}
            for role, entity_ids in artifact_roles.items()
            for entity_id in _dedupe_preserving_order(entity_ids)
        ],
    }


async def sync_project_memory_links(
    session,
    project_id: str,
    *,
    episode_id: str | None = None,
    event_id: str | None = None,
    thread_id: str | None = None,
    job_id: str | None = None,
    step_id: str | None = None,
    fact_id_by_key: dict[str, str] | None = None,
    pattern_id_by_key: dict[str, str] | None = None,
    preference_id_by_key: dict[str, str] | None = None,
    resource_entity_ids: list[str] | None = None,
    artifact_record_ids: list[str] | None = None,
    metadata_json: dict[str, Any] | None = None,
) -> None:
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    from tune.core.models import MemoryLink

    edges: list[dict[str, Any]] = []
    link_targets = _collect_memory_link_targets(
        metadata_json,
        explicit_resource_entity_ids=resource_entity_ids,
        explicit_artifact_record_ids=artifact_record_ids,
    )

    if episode_id:
        if event_id:
            edges.append(
                {
                    "memory_type": "memory_episode",
                    "memory_id": episode_id,
                    "entity_type": "project_execution_event",
                    "entity_id": event_id,
                    "link_role": "derived_from_event",
                    "strength": 1.0,
                }
            )
        for entity_type, entity_id, link_role in (
            ("thread", thread_id, "context_thread"),
            ("analysis_job", job_id, "context_job"),
            ("analysis_step_run", step_id, "context_step"),
        ):
            if entity_id:
                edges.append(
                    {
                        "memory_type": "memory_episode",
                        "memory_id": episode_id,
                        "entity_type": entity_type,
                        "entity_id": entity_id,
                        "link_role": link_role,
                        "strength": 0.9,
                    }
                )
        for target in link_targets.get("resource_targets", []):
            edges.append(
                {
                    "memory_type": "memory_episode",
                    "memory_id": episode_id,
                    "entity_type": "resource_entity",
                    "entity_id": target["entity_id"],
                    "link_role": target["link_role"],
                    "strength": 0.9,
                }
            )
        for target in link_targets.get("artifact_targets", []):
            edges.append(
                {
                    "memory_type": "memory_episode",
                    "memory_id": episode_id,
                    "entity_type": "artifact_record",
                    "entity_id": target["entity_id"],
                    "link_role": target["link_role"],
                    "strength": 0.9,
                }
            )

    for fact_key, fact_id in dict(fact_id_by_key or {}).items():
        if not fact_id:
            continue
        if episode_id:
            edges.append(
                {
                    "memory_type": "memory_fact",
                    "memory_id": fact_id,
                    "entity_type": "memory_episode",
                    "entity_id": episode_id,
                    "link_role": "derived_from_episode",
                    "strength": 1.0,
                }
            )
        if event_id:
            edges.append(
                {
                    "memory_type": "memory_fact",
                    "memory_id": fact_id,
                    "entity_type": "project_execution_event",
                    "entity_id": event_id,
                    "link_role": "derived_from_event",
                    "strength": 1.0,
                }
            )
        for target in link_targets.get("resource_targets", []):
            edges.append(
                {
                    "memory_type": "memory_fact",
                    "memory_id": fact_id,
                    "entity_type": "resource_entity",
                    "entity_id": target["entity_id"],
                    "link_role": "supports_resource_decision",
                    "strength": 0.8,
                }
            )

    for pattern_key, pattern_id in dict(pattern_id_by_key or {}).items():
        if pattern_id and episode_id:
            edges.append(
                {
                    "memory_type": "memory_pattern",
                    "memory_id": pattern_id,
                    "entity_type": "memory_episode",
                    "entity_id": episode_id,
                    "link_role": "aggregated_from_episode",
                    "strength": 0.8,
                }
            )

    for preference_key, preference_id in dict(preference_id_by_key or {}).items():
        if preference_id and episode_id:
            edges.append(
                {
                    "memory_type": "memory_preference",
                    "memory_id": preference_id,
                    "entity_type": "memory_episode",
                    "entity_id": episode_id,
                    "link_role": "aggregated_from_episode",
                    "strength": 0.8,
                }
            )

    if not edges:
        return

    try:
        for edge in edges:
            await session.execute(
                pg_insert(MemoryLink)
                .values(
                    id=str(uuid.uuid4()),
                    project_id=project_id,
                    memory_type=edge["memory_type"],
                    memory_id=edge["memory_id"],
                    entity_type=edge["entity_type"],
                    entity_id=edge["entity_id"],
                    link_role=edge["link_role"],
                    strength=edge.get("strength"),
                )
                .on_conflict_do_update(
                    constraint="uq_memory_links_edge",
                    set_={
                        "strength": edge.get("strength"),
                    },
                )
            )

    except Exception:
        log.debug(
            "MemoryLink sync unavailable for project %s; continuing without persisted link graph",
            project_id,
            exc_info=True,
        )


async def write_execution_event(
    session,
    project_id: str,
    event_type: str,
    description: str,
    resolution: str,
    user_contributed: bool,
    *,
    thread_id: str | None = None,
    job_id: str | None = None,
    step_id: str | None = None,
    metadata_json: dict[str, Any] | None = None,
):
    """Write a ProjectExecutionEvent with embedding and commit it."""
    await queue_execution_event(
        session,
        project_id=project_id,
        event_type=event_type,
        description=description,
        resolution=resolution,
        user_contributed=user_contributed,
        thread_id=thread_id,
        job_id=job_id,
        step_id=step_id,
        metadata_json=metadata_json,
    )
    await session.commit()


async def queue_execution_event(
    session,
    project_id: str,
    event_type: str,
    description: str,
    resolution: str,
    user_contributed: bool,
    *,
    thread_id: str | None = None,
    job_id: str | None = None,
    step_id: str | None = None,
    metadata_json: dict[str, Any] | None = None,
):
    """Queue a ProjectExecutionEvent on the current session without committing."""
    from tune.core.memory.global_memory import embed_text
    from sqlalchemy import insert
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    from tune.core.models import MemoryEpisode, MemoryFact, ProjectExecutionEvent

    embedding = await embed_text(f"{description}\n{resolution}")
    event_id = str(uuid.uuid4())
    await session.execute(
        insert(ProjectExecutionEvent).values(
            id=event_id,
            project_id=project_id,
            event_type=event_type,
            description=description,
            resolution=resolution,
            user_contributed=user_contributed,
            embedding=embedding,
        )
    )

    episode_id = str(uuid.uuid4())
    try:
        await session.execute(
            insert(MemoryEpisode).values(
                id=episode_id,
                project_id=project_id,
                source_event_id=event_id,
                thread_id=thread_id,
                job_id=job_id,
                step_id=step_id,
                event_type=event_type,
                description=description,
                resolution=resolution,
                user_contributed=user_contributed,
                metadata_json=metadata_json,
                embedding=embedding,
            )
        )
    except Exception:
        log.debug("MemoryEpisode write unavailable for project %s; keeping legacy event write only", project_id, exc_info=True)
        episode_id = ""

    derived_facts = extract_project_memory_facts(
        [
            {
                "id": event_id,
                "project_id": project_id,
                "event_type": event_type,
                "description": description,
                "resolution": resolution,
                "user_contributed": user_contributed,
            }
        ],
        project_id=project_id,
    )
    fact_id_by_key: dict[str, str] = {}
    for fact in derived_facts:
        inserted_id = str(uuid.uuid4())
        payload_json = {
            key: value
            for key, value in fact.items()
            if key
            not in {
                "id",
                "project_id",
                "fact_key",
                "fact_type",
                "title",
                "statement",
            }
        }
        try:
            result = await session.execute(
                pg_insert(MemoryFact)
                .values(
                    id=inserted_id,
                    project_id=project_id,
                    fact_key=str(fact.get("fact_key") or ""),
                    fact_type=str(fact.get("fact_type") or "memory_fact"),
                    title=str(fact.get("title") or fact.get("fact_key") or "memory_fact"),
                    statement=str(fact.get("statement") or ""),
                    payload_json=payload_json or None,
                    source_event_id=event_id,
                    source_episode_id=episode_id or None,
                    confidence=1.0 if user_contributed else 0.8,
                )
                .on_conflict_do_update(
                    constraint="uq_memory_facts_project_fact_key",
                    set_={
                        "fact_type": str(fact.get("fact_type") or "memory_fact"),
                        "title": str(fact.get("title") or fact.get("fact_key") or "memory_fact"),
                        "statement": str(fact.get("statement") or ""),
                        "payload_json": payload_json or None,
                        "source_event_id": event_id,
                        "source_episode_id": episode_id or None,
                        "confidence": 1.0 if user_contributed else 0.8,
                    },
                )
                .returning(MemoryFact.id)
            )
            fact_id_by_key[str(fact.get("fact_key") or "")] = _scalar_result_or_default(result, inserted_id)
        except Exception:
            log.debug("MemoryFact upsert unavailable for project %s; continuing with legacy event memory only", project_id, exc_info=True)

    layer_sync = await sync_project_memory_layers(
        session,
        project_id,
        source_episode_id=episode_id or None,
    )
    await sync_project_memory_links(
        session,
        project_id,
        episode_id=episode_id or None,
        event_id=event_id,
        thread_id=thread_id,
        job_id=job_id,
        step_id=step_id,
        fact_id_by_key=fact_id_by_key,
        pattern_id_by_key=(layer_sync or {}).get("pattern_id_by_key") or {},
        preference_id_by_key=(layer_sync or {}).get("preference_id_by_key") or {},
        metadata_json=metadata_json,
    )
