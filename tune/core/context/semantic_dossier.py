from __future__ import annotations

from typing import Any


def build_project_memory_summary(
    *,
    stable_facts: list[dict[str, Any]] | None = None,
    memory_patterns: list[dict[str, Any]] | None = None,
    memory_preferences: list[dict[str, Any]] | None = None,
    memory_links: list[dict[str, Any]] | None = None,
    resource_binding_fact_count: int | None = None,
) -> dict[str, Any]:
    stable_fact_rows = [item for item in (stable_facts or []) if isinstance(item, dict)]
    pattern_rows = [item for item in (memory_patterns or []) if isinstance(item, dict)]
    preference_rows = [item for item in (memory_preferences or []) if isinstance(item, dict)]
    link_rows = [item for item in (memory_links or []) if isinstance(item, dict)]
    if resource_binding_fact_count is None:
        resource_binding_fact_count = len(
            [
                item
                for item in stable_fact_rows
                if str(item.get("binding_key") or "").strip() and str(item.get("path") or "").strip()
            ]
        )
    link_breakdown = build_memory_link_breakdown(link_rows)
    return {
        "stable_fact_count": len(stable_fact_rows),
        "memory_pattern_count": len(pattern_rows),
        "memory_preference_count": len(preference_rows),
        "memory_link_count": len(link_rows),
        "resource_binding_fact_count": int(resource_binding_fact_count or 0),
        "resource_link_count": int(link_breakdown.get("resource_link_count", 0) or 0),
        "artifact_link_count": int(link_breakdown.get("artifact_link_count", 0) or 0),
        "runtime_link_count": int(link_breakdown.get("runtime_link_count", 0) or 0),
    }


def _trim_text_items(items: list[Any] | None, limit: int) -> list[str]:
    return [
        str(item).strip()
        for item in (items or [])[:limit]
        if str(item).strip()
    ]


def _trim_resource_candidates(items: list[dict[str, Any]] | None, limit: int) -> list[dict[str, Any]]:
    return [
        {
            "binding_key": item.get("binding_key"),
            "path": item.get("path"),
            "source_type": item.get("source_type"),
            "organism": item.get("organism"),
            "genome_build": item.get("genome_build"),
            "score": item.get("score"),
        }
        for item in (items or [])[:limit]
        if isinstance(item, dict)
    ]


def _trim_stable_facts(items: list[dict[str, Any]] | None, limit: int) -> list[dict[str, Any]]:
    return [
        {
            "fact_key": item.get("fact_key"),
            "fact_type": item.get("fact_type"),
            "title": item.get("title"),
            "statement": item.get("statement"),
            "binding_key": item.get("binding_key"),
            "path": item.get("path"),
        }
        for item in (items or [])[:limit]
        if isinstance(item, dict)
    ]


def build_memory_link_breakdown(
    memory_links: list[dict[str, Any]] | None,
    *,
    preview_limit: int = 3,
) -> dict[str, Any]:
    rows = [item for item in (memory_links or []) if isinstance(item, dict)]
    resource_links = [item for item in rows if str(item.get("entity_type") or "").strip() == "resource_entity"]
    artifact_links = [item for item in rows if str(item.get("entity_type") or "").strip() == "artifact_record"]
    runtime_links = [
        item
        for item in rows
        if str(item.get("entity_type") or "").strip()
        in {
            "project_execution_event",
            "thread",
            "analysis_job",
            "analysis_step_run",
            "memory_episode",
        }
    ]
    return {
        "resource_link_count": len(resource_links),
        "artifact_link_count": len(artifact_links),
        "runtime_link_count": len(runtime_links),
        "resource_links": resource_links[:preview_limit],
        "artifact_links": artifact_links[:preview_limit],
        "runtime_links": runtime_links[:preview_limit],
    }


def build_semantic_memory_dossier(
    summary: Any,
    *,
    project_id: str | None = None,
    memory_patterns: list[dict[str, Any]] | None = None,
    memory_preferences: list[dict[str, Any]] | None = None,
    memory_links: list[dict[str, Any]] | None = None,
    item_limit: int = 4,
    link_limit: int = 6,
) -> dict[str, Any] | None:
    if summary is None:
        return None

    resource_candidates = _trim_resource_candidates(getattr(summary, "resource_candidates", None), item_limit)
    stable_facts = _trim_stable_facts(getattr(summary, "stable_facts", None), item_limit)
    semantic_hints = _trim_text_items(getattr(summary, "semantic_hints", None), item_limit)
    ambiguity_hints = _trim_text_items(getattr(summary, "ambiguity_hints", None), item_limit)
    memory_hints = _trim_text_items(getattr(summary, "memory_hints", None), item_limit)

    fragments: list[str] = []
    if resource_candidates:
        fragments.append(f"resource_candidates={len(resource_candidates)}")
    if stable_facts:
        fragments.append(f"stable_facts={len(stable_facts)}")
    if semantic_hints:
        fragments.append(f"semantic_hints={len(semantic_hints)}")
    if ambiguity_hints:
        fragments.append(f"semantic_ambiguities={len(ambiguity_hints)}")
    if memory_hints:
        fragments.append(f"memory_hints={len(memory_hints)}")

    payload: dict[str, Any] = {
        "available": True,
        "project_id": project_id,
        "resource_candidate_count": len(getattr(summary, "resource_candidates", None) or []),
        "stable_fact_count": len(getattr(summary, "stable_facts", None) or []),
        "semantic_hint_count": len(getattr(summary, "semantic_hints", None) or []),
        "ambiguity_count": len(getattr(summary, "ambiguity_hints", None) or []),
        "memory_hint_count": len(getattr(summary, "memory_hints", None) or []),
        "resource_candidates": resource_candidates,
        "stable_facts": stable_facts,
        "semantic_hints": semantic_hints,
        "ambiguity_hints": ambiguity_hints,
        "memory_hints": memory_hints,
    }

    pattern_rows = [item for item in (memory_patterns or []) if isinstance(item, dict)]
    preference_rows = [item for item in (memory_preferences or []) if isinstance(item, dict)]
    link_rows = [item for item in (memory_links or []) if isinstance(item, dict)]
    project_memory_summary = build_project_memory_summary(
        stable_facts=getattr(summary, "stable_facts", None),
        memory_patterns=pattern_rows,
        memory_preferences=preference_rows,
        memory_links=link_rows,
    )
    payload.update(project_memory_summary)
    if pattern_rows:
        payload["memory_patterns"] = pattern_rows[:item_limit]
        fragments.append(f"memory_patterns={len(pattern_rows)}")
    if preference_rows:
        payload["memory_preferences"] = preference_rows[:item_limit]
        fragments.append(f"memory_preferences={len(preference_rows)}")
    if link_rows:
        payload["memory_links"] = link_rows[:link_limit]
        fragments.append(f"memory_links={len(link_rows)}")
        breakdown = build_memory_link_breakdown(link_rows)
        payload.update(breakdown)
        if breakdown["resource_link_count"]:
            fragments.append(f"memory_resource_links={breakdown['resource_link_count']}")
        if breakdown["artifact_link_count"]:
            fragments.append(f"memory_artifact_links={breakdown['artifact_link_count']}")

    payload["summary"] = " · ".join(fragments) if fragments else "No semantic-memory dossier signals."
    return payload
