"""Stable semantic document projection for fact-layer objects.

This is the low-risk foundation for later hybrid retrieval and memory
projection. The current batch only defines deterministic document identities
and text projection; it does not add storage or embedding infrastructure.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
from typing import Any


@dataclass(frozen=True)
class SemanticDocument:
    doc_id: str
    doc_type: str
    source_type: str
    source_id: str
    project_id: str | None
    title: str
    text: str
    metadata: dict[str, Any] = field(default_factory=dict)


def document_from_known_path(known_path: Any, *, project_id: str | None = None) -> SemanticDocument | None:
    key = _string_value(known_path, "key")
    path = _string_value(known_path, "path")
    resolved_project_id = project_id or _string_value(known_path, "project_id")
    if not key or not path:
        return None
    source_id = f"{resolved_project_id or 'global'}:{key}"
    title = f"Known path {key}"
    text = f"Known path {key}: {path}"
    description = _string_value(known_path, "description")
    if description:
        text += f"\nDescription: {description}"
    return SemanticDocument(
        doc_id=_semantic_doc_id("known_path", source_id),
        doc_type="known_path",
        source_type="known_path",
        source_id=source_id,
        project_id=resolved_project_id,
        title=title,
        text=text,
        metadata={
            "key": key,
            "path": path,
            "description": description,
        },
    )


def document_from_project_file(project_file: Any, *, project_id: str | None = None) -> SemanticDocument | None:
    file_id = _string_value(project_file, "id")
    path = _string_value(project_file, "path")
    filename = _string_value(project_file, "filename") or path
    if not path:
        return None
    resolved_project_id = project_id or _string_value(project_file, "project_id")
    source_id = file_id or path
    file_type = _string_value(project_file, "file_type")
    linked_sample_id = _string_value(project_file, "linked_sample_id") or _string_value(project_file, "sample_id")
    linked_experiment_id = _string_value(project_file, "linked_experiment_id") or _string_value(project_file, "experiment_id")
    read_number = _raw_value(project_file, "read_number")
    text_parts = [
        f"Project file: {filename}",
        f"Path: {path}",
    ]
    if file_type:
        text_parts.append(f"File type: {file_type}")
    if linked_sample_id:
        text_parts.append(f"Sample: {linked_sample_id}")
    if linked_experiment_id:
        text_parts.append(f"Experiment: {linked_experiment_id}")
    if read_number in {1, 2}:
        text_parts.append(f"Read number: {read_number}")
    return SemanticDocument(
        doc_id=_semantic_doc_id("project_file", source_id),
        doc_type="project_file",
        source_type="project_file",
        source_id=source_id,
        project_id=resolved_project_id,
        title=filename,
        text="\n".join(text_parts),
        metadata={
            "path": path,
            "file_type": file_type,
            "linked_sample_id": linked_sample_id,
            "linked_experiment_id": linked_experiment_id,
            "read_number": read_number,
        },
    )


def document_from_resource_entity(entity: Any, *, project_id: str | None = None) -> SemanticDocument | None:
    entity_id = _string_value(entity, "id")
    resource_role = _string_value(entity, "resource_role")
    display_name = _string_value(entity, "display_name") or entity_id or resource_role
    if not display_name:
        return None
    resolved_project_id = project_id or _string_value(entity, "project_id")
    source_id = entity_id or display_name
    organism = _string_value(entity, "organism")
    genome_build = _string_value(entity, "genome_build")
    source_type = _string_value(entity, "source_type") or "resource_entity"
    component_lines = []
    for component in list(_raw_value(entity, "components") or _raw_value(entity, "resource_files") or []):
        file_role = _string_value(component, "file_role")
        file_id = _string_value(component, "file_id")
        path = _string_value(component, "path")
        if not path:
            file_obj = _raw_value(component, "file")
            path = _string_value(file_obj, "path")
        label = path or file_id
        if not label:
            continue
        prefix = f"{file_role}: " if file_role else ""
        component_lines.append(prefix + label)

    text_parts = [f"Resource entity: {display_name}"]
    if resource_role:
        text_parts.append(f"Resource role: {resource_role}")
    if organism:
        text_parts.append(f"Organism: {organism}")
    if genome_build:
        text_parts.append(f"Genome build: {genome_build}")
    if component_lines:
        text_parts.append("Components:")
        text_parts.extend(component_lines)
    return SemanticDocument(
        doc_id=_semantic_doc_id("resource_entity", source_id),
        doc_type="resource_entity",
        source_type=source_type,
        source_id=source_id,
        project_id=resolved_project_id,
        title=display_name,
        text="\n".join(text_parts),
        metadata={
            "resource_role": resource_role,
            "organism": organism,
            "genome_build": genome_build,
            "component_count": len(component_lines),
        },
    )


def document_from_artifact_record(artifact: dict[str, Any], *, project_id: str | None = None) -> SemanticDocument | None:
    artifact_id = str(artifact.get("id") or "").strip()
    file_path = str(artifact.get("file_path") or "").strip()
    if not artifact_id and not file_path:
        return None
    source_id = artifact_id or file_path
    resolved_project_id = project_id or str(artifact.get("project_id") or "").strip() or None
    artifact_role = str(artifact.get("artifact_role") or "").strip()
    step_key = str(artifact.get("step_key") or artifact.get("dep_key") or "").strip()
    title = artifact_role or step_key or file_path or artifact_id
    text_parts = [f"Artifact record: {title}"]
    if file_path:
        text_parts.append(f"Path: {file_path}")
    if artifact_role:
        text_parts.append(f"Artifact role: {artifact_role}")
    if step_key:
        text_parts.append(f"Produced by: {step_key}")
    return SemanticDocument(
        doc_id=_semantic_doc_id("artifact_record", source_id),
        doc_type="artifact_record",
        source_type="artifact_record",
        source_id=source_id,
        project_id=resolved_project_id,
        title=title,
        text="\n".join(text_parts),
        metadata={
            "file_path": file_path,
            "artifact_role": artifact_role,
            "step_key": step_key,
        },
    )


def document_from_memory_fact(fact: dict[str, Any], *, project_id: str | None = None) -> SemanticDocument | None:
    fact_id = str(fact.get("id") or fact.get("fact_key") or "").strip()
    statement = str(fact.get("statement") or fact.get("value") or "").strip()
    if not fact_id and not statement:
        return None
    source_id = fact_id or statement
    resolved_project_id = project_id or str(fact.get("project_id") or "").strip() or None
    fact_type = str(fact.get("fact_type") or fact.get("kind") or "memory_fact").strip()
    title = str(fact.get("title") or fact_type or "memory_fact").strip()
    text = statement or title
    return SemanticDocument(
        doc_id=_semantic_doc_id("memory_fact", source_id),
        doc_type="memory_fact",
        source_type="memory_fact",
        source_id=source_id,
        project_id=resolved_project_id,
        title=title,
        text=text,
        metadata={
            "fact_type": fact_type,
            "fact_key": str(fact.get("fact_key") or "").strip() or None,
        },
    )


def documents_from_memory_facts(
    facts: list[dict[str, Any]] | None,
    *,
    project_id: str | None = None,
) -> list[SemanticDocument]:
    documents: list[SemanticDocument] = []
    for fact in facts or []:
        document = document_from_memory_fact(fact, project_id=project_id)
        if document is not None:
            documents.append(document)
    return documents


def document_from_project_event(event: Any, *, project_id: str | None = None) -> SemanticDocument | None:
    resolved_project_id = project_id or _string_value(event, "project_id")
    event_id = _string_value(event, "id")
    event_type = _string_value(event, "event_type") or "project_event"
    description = _string_value(event, "description")
    resolution = _string_value(event, "resolution")
    if not event_id and not description:
        return None
    source_id = event_id or f"{event_type}:{description}"
    title = event_type.replace("_", " ")
    text_parts = [f"Project event: {event_type}"]
    if description:
        text_parts.append(f"Description: {description}")
    if resolution:
        text_parts.append(f"Resolution: {resolution}")
    return SemanticDocument(
        doc_id=_semantic_doc_id("memory_episode", source_id),
        doc_type="memory_episode",
        source_type="project_execution_event",
        source_id=source_id,
        project_id=resolved_project_id,
        title=title,
        text="\n".join(text_parts),
        metadata={
            "event_type": event_type,
            "user_contributed": bool(_raw_value(event, "user_contributed")),
        },
    )


def documents_from_project_events(
    events: list[Any] | None,
    *,
    project_id: str | None = None,
) -> list[SemanticDocument]:
    documents: list[SemanticDocument] = []
    for event in events or []:
        document = document_from_project_event(event, project_id=project_id)
        if document is not None:
            documents.append(document)
    return documents


def documents_from_project_memory_profile(
    profile: dict[str, Any] | None,
    *,
    project_id: str | None = None,
) -> list[SemanticDocument]:
    payload = dict(profile or {})
    documents: list[SemanticDocument] = []

    preferences = dict(payload.get("preferences") or {})
    if preferences:
        preference_lines = [f"{key}: {value}" for key, value in sorted(preferences.items()) if value not in (None, "")]
        if preference_lines:
            documents.append(
                SemanticDocument(
                    doc_id=_semantic_doc_id("memory_fact", f"{project_id or 'global'}:preferences"),
                    doc_type="memory_fact",
                    source_type="project_memory_profile",
                    source_id="preferences",
                    project_id=project_id,
                    title="Project memory preferences",
                    text="Project memory preferences\n" + "\n".join(preference_lines),
                    metadata={
                        "fact_type": "project_memory_preferences",
                        "preference_count": len(preference_lines),
                    },
                )
            )

    for pattern in list(payload.get("safe_action_patterns") or []):
        safe_action = str(pattern.get("safe_action") or "").strip()
        if not safe_action:
            continue
        support_count = int(pattern.get("support_count") or 0)
        lines = [
            f"Preferred safe action pattern: {safe_action}",
            f"Support count: {support_count}",
        ]
        for key in ("incident_types", "rollback_levels", "analysis_families"):
            values = [str(v).strip() for v in (pattern.get(key) or []) if str(v).strip()]
            if values:
                lines.append(f"{key}: {', '.join(values)}")
        documents.append(
            SemanticDocument(
                doc_id=_semantic_doc_id("memory_fact", f"{project_id or 'global'}:safe_action:{safe_action}"),
                doc_type="memory_fact",
                source_type="project_memory_profile",
                source_id=f"safe_action:{safe_action}",
                project_id=project_id,
                title=f"Safe action pattern {safe_action}",
                text="\n".join(lines),
                metadata={
                    "fact_type": "project_memory_safe_action_pattern",
                    "safe_action": safe_action,
                    "support_count": support_count,
                },
            )
        )

    for pattern in list(payload.get("rollback_patterns") or []):
        rollback_level = str(pattern.get("rollback_level") or "").strip()
        if not rollback_level:
            continue
        support_count = int(pattern.get("support_count") or 0)
        lines = [
            f"Preferred rollback pattern: {rollback_level}",
            f"Support count: {support_count}",
        ]
        for key in ("safe_actions", "analysis_families", "incident_types"):
            values = [str(v).strip() for v in (pattern.get(key) or []) if str(v).strip()]
            if values:
                lines.append(f"{key}: {', '.join(values)}")
        documents.append(
            SemanticDocument(
                doc_id=_semantic_doc_id("memory_fact", f"{project_id or 'global'}:rollback:{rollback_level}"),
                doc_type="memory_fact",
                source_type="project_memory_profile",
                source_id=f"rollback:{rollback_level}",
                project_id=project_id,
                title=f"Rollback pattern {rollback_level}",
                text="\n".join(lines),
                metadata={
                    "fact_type": "project_memory_rollback_pattern",
                    "rollback_level": rollback_level,
                    "support_count": support_count,
                },
            )
        )

    return documents


def documents_from_structured_project_memory_layers(
    *,
    memory_patterns: list[dict[str, Any]] | None = None,
    memory_preferences: list[dict[str, Any]] | None = None,
    project_id: str | None = None,
) -> list[SemanticDocument]:
    documents: list[SemanticDocument] = []

    for pattern in memory_patterns or []:
        pattern_key = str(pattern.get("pattern_key") or "").strip()
        pattern_type = str(pattern.get("pattern_type") or "").strip() or "pattern"
        recommended_value = str(pattern.get("recommended_value") or "").strip()
        title = str(pattern.get("title") or pattern_key or "Project memory pattern").strip()
        if not pattern_key or not recommended_value:
            continue
        lines = [
            f"Project memory pattern: {pattern_type}",
            f"Recommended value: {recommended_value}",
            f"Support count: {int(pattern.get('support_count') or 0)}",
        ]
        for key in ("incident_types", "rollback_levels", "analysis_families", "safe_actions"):
            values = [str(v).strip() for v in (pattern.get(key) or []) if str(v).strip()]
            if values:
                lines.append(f"{key}: {', '.join(values)}")
        documents.append(
            SemanticDocument(
                doc_id=_semantic_doc_id("memory_fact", f"{project_id or 'global'}:{pattern_key}"),
                doc_type="memory_fact",
                source_type="memory_pattern",
                source_id=pattern_key,
                project_id=project_id,
                title=title,
                text="\n".join(lines),
                metadata={
                    "fact_type": f"structured_memory_pattern:{pattern_type}",
                    "pattern_type": pattern_type,
                    "recommended_value": recommended_value,
                    "support_count": int(pattern.get("support_count") or 0),
                    "confidence": str(pattern.get("confidence") or "").strip() or None,
                },
            )
        )

    for preference in memory_preferences or []:
        preference_key = str(preference.get("preference_key") or "").strip()
        preference_type = str(preference.get("preference_type") or "").strip() or "preference"
        value = str(preference.get("value") or "").strip()
        title = str(preference.get("title") or preference_key or "Project memory preference").strip()
        if not preference_key or not value:
            continue
        lines = [
            f"Project memory preference: {preference_type}",
            f"Value: {value}",
            f"Support count: {int(preference.get('support_count') or 0)}",
        ]
        basis = str(preference.get("basis") or "").strip()
        if basis:
            lines.append(f"Basis: {basis}")
        documents.append(
            SemanticDocument(
                doc_id=_semantic_doc_id("memory_fact", f"{project_id or 'global'}:{preference_key}"),
                doc_type="memory_fact",
                source_type="memory_preference",
                source_id=preference_key,
                project_id=project_id,
                title=title,
                text="\n".join(lines),
                metadata={
                    "fact_type": f"structured_memory_preference:{preference_type}",
                    "preference_type": preference_type,
                    "value": value,
                    "support_count": int(preference.get("support_count") or 0),
                    "confidence": str(preference.get("confidence") or "").strip() or None,
                },
            )
        )

    return documents


def project_semantic_documents_from_context(ctx: Any) -> list[SemanticDocument]:
    documents: list[SemanticDocument] = []
    project = _raw_value(ctx, "project")
    project_id = _string_value(project, "id")

    for known_path in list(_raw_value(project, "known_paths") or []):
        doc = document_from_known_path(known_path, project_id=project_id)
        if doc is not None:
            documents.append(doc)

    for file_info in list(_raw_value(ctx, "files") or []):
        doc = document_from_project_file(file_info, project_id=project_id)
        if doc is not None:
            documents.append(doc)

    for entity in list(_raw_value(project, "resource_entities") or []):
        doc = document_from_resource_entity(entity, project_id=project_id)
        if doc is not None:
            documents.append(doc)

    documents.sort(key=lambda item: (item.doc_type, item.title, item.doc_id))
    return documents


def _semantic_doc_id(doc_type: str, source_id: str) -> str:
    digest = hashlib.sha1(f"{doc_type}:{source_id}".encode("utf-8")).hexdigest()
    return f"semdoc:{doc_type}:{digest[:16]}"


def _raw_value(value: Any, key: str) -> Any:
    if value is None:
        return None
    if isinstance(value, dict):
        return value.get(key)
    return getattr(value, key, None)


def _string_value(value: Any, key: str) -> str | None:
    raw = _raw_value(value, key)
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None


__all__ = [
    "SemanticDocument",
    "document_from_artifact_record",
    "document_from_known_path",
    "document_from_memory_fact",
    "document_from_project_event",
    "document_from_project_file",
    "document_from_resource_entity",
    "documents_from_memory_facts",
    "documents_from_project_events",
    "documents_from_project_memory_profile",
    "documents_from_structured_project_memory_layers",
    "project_semantic_documents_from_context",
]
