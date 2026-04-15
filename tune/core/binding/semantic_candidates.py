"""Shared semantic candidate model and source adapters.

This module gives binding and later semantic-retrieval layers one stable object
shape instead of stitching slightly different dicts for each source.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class SemanticCandidate:
    file_path: str
    source_type: str
    source_ref: str | None = None
    artifact_role: str | None = None
    slot_name: str | None = None
    artifact_scope: str | None = None
    project_id: str | None = None
    sample_id: str | None = None
    experiment_id: str | None = None
    sample_name: str | None = None
    read_number: int | None = None
    entity_type: str | None = None
    entity_id: str | None = None
    step_key: str | None = None
    step_type: str | None = None
    organism: str | None = None
    genome_build: str | None = None
    confidence: float | None = None
    provenance: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def lineage(self) -> dict[str, Any]:
        return {
            "sample_id": self.sample_id,
            "experiment_id": self.experiment_id,
            "sample_name": self.sample_name,
            "read_number": self.read_number,
        }

    def to_resolver_dict(self) -> dict[str, Any]:
        return {
            "file_path": self.file_path,
            "source_type": self.source_type,
            "source_ref": self.source_ref,
            "artifact_role": self.artifact_role,
            "slot_name": self.slot_name,
            "artifact_scope": self.artifact_scope,
            "sample_id": self.sample_id,
            "experiment_id": self.experiment_id,
            "sample_name": self.sample_name,
            "read_number": self.read_number,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "step_key": self.step_key,
            "step_type": self.step_type,
            "organism": self.organism,
            "genome_build": self.genome_build,
            "confidence": self.confidence,
            "provenance": dict(self.provenance or {}),
            "metadata": dict(self.metadata or {}),
        }


def semantic_candidate_from_project_file(
    project_file: dict[str, Any],
    *,
    artifact_role: str | None = None,
    slot_name: str | None = None,
    project_id: str | None = None,
) -> SemanticCandidate | None:
    file_path = str(project_file.get("path") or "").strip()
    if not file_path:
        return None
    return SemanticCandidate(
        file_path=file_path,
        source_type="project_file",
        source_ref=_string_or_none(project_file.get("id")),
        artifact_role=artifact_role,
        slot_name=slot_name,
        artifact_scope=_string_or_none(project_file.get("artifact_scope")),
        project_id=project_id,
        sample_id=_string_or_none(project_file.get("linked_sample_id") or project_file.get("sample_id")),
        experiment_id=_string_or_none(project_file.get("linked_experiment_id") or project_file.get("experiment_id")),
        sample_name=_string_or_none(project_file.get("sample_name")),
        read_number=_int_or_none(project_file.get("read_number")),
        entity_type="project_file",
        entity_id=_string_or_none(project_file.get("id")),
        organism=_string_or_none(project_file.get("organism")),
        genome_build=_string_or_none(project_file.get("genome_build")),
        confidence=_float_or_none(project_file.get("confidence")),
        provenance={
            "filename": _string_or_none(project_file.get("filename")),
            "file_type": _string_or_none(project_file.get("file_type")),
        },
        metadata=dict(project_file.get("metadata") or {}),
    )


def semantic_candidate_from_known_path(
    *,
    known_path_key: str,
    path: str,
    artifact_role: str | None = None,
    slot_name: str | None = None,
    project_id: str | None = None,
) -> SemanticCandidate | None:
    file_path = str(path or "").strip()
    if not file_path:
        return None
    return SemanticCandidate(
        file_path=file_path,
        source_type="known_path",
        source_ref=known_path_key,
        artifact_role=artifact_role,
        slot_name=slot_name,
        project_id=project_id,
        entity_type="known_path",
        entity_id=known_path_key,
        provenance={"known_path_key": known_path_key},
    )


def semantic_candidate_from_filerun(
    filerun_candidate: dict[str, Any],
    *,
    project_id: str | None = None,
) -> SemanticCandidate | None:
    file_path = str(filerun_candidate.get("file_path") or "").strip()
    if not file_path:
        return None
    return SemanticCandidate(
        file_path=file_path,
        source_type=str(filerun_candidate.get("source_type") or "filerun"),
        source_ref=_string_or_none(filerun_candidate.get("source_ref")),
        artifact_role=_string_or_none(filerun_candidate.get("artifact_role")),
        project_id=project_id,
        sample_id=_string_or_none(filerun_candidate.get("sample_id")),
        experiment_id=_string_or_none(filerun_candidate.get("experiment_id")),
        sample_name=_string_or_none(filerun_candidate.get("sample_name")),
        read_number=_int_or_none(filerun_candidate.get("read_number")),
        entity_type="filerun",
        entity_id=_string_or_none(filerun_candidate.get("source_ref")),
        provenance={"row_type": "filerun"},
    )


def semantic_candidate_from_artifact_record(
    artifact: dict[str, Any],
    *,
    dep_key: str,
    project_id: str | None = None,
) -> SemanticCandidate | None:
    file_path = str(artifact.get("file_path") or "").strip()
    if not file_path:
        return None
    metadata = dict(artifact.get("metadata_json") or artifact.get("metadata") or {})
    return SemanticCandidate(
        file_path=file_path,
        source_type="artifact_record",
        source_ref=dep_key,
        artifact_role=_string_or_none(artifact.get("artifact_role")),
        slot_name=_string_or_none(artifact.get("slot_name")),
        artifact_scope=_string_or_none(artifact.get("artifact_scope")),
        project_id=project_id,
        sample_id=_string_or_none(artifact.get("sample_id") or metadata.get("sample_id")),
        experiment_id=_string_or_none(artifact.get("experiment_id") or metadata.get("experiment_id")),
        sample_name=_string_or_none(artifact.get("sample_name") or metadata.get("sample_name")),
        read_number=_int_or_none(artifact.get("read_number") or metadata.get("read_number")),
        entity_type="artifact_record",
        entity_id=_string_or_none(artifact.get("id")),
        step_key=dep_key,
        step_type=_string_or_none(artifact.get("step_type")),
        confidence=_float_or_none(artifact.get("confidence")),
        provenance={
            "dep_key": dep_key,
            "source_slot_name": _string_or_none(artifact.get("slot_name")),
        },
        metadata=metadata,
    )


def semantic_candidate_from_resource_entity(
    entity: Any,
    *,
    preferred_file_role: str | None = None,
) -> SemanticCandidate | None:
    file_path, resource_file = _resource_entity_primary_path(entity, preferred_file_role=preferred_file_role)
    if not file_path:
        return None
    metadata = dict(getattr(entity, "metadata_json", None) or _mapping_get(entity, "metadata_json") or {})
    return SemanticCandidate(
        file_path=file_path,
        source_type=_string_or_none(getattr(entity, "source_type", None) or _mapping_get(entity, "source_type")) or "resource_entity",
        source_ref=_string_or_none(getattr(entity, "id", None) or _mapping_get(entity, "id")),
        artifact_role=_string_or_none(
            getattr(resource_file, "file_role", None)
            or _mapping_get(resource_file, "file_role")
            or getattr(entity, "resource_role", None)
            or _mapping_get(entity, "resource_role")
        ),
        slot_name=preferred_file_role,
        project_id=_string_or_none(getattr(entity, "project_id", None) or _mapping_get(entity, "project_id")),
        entity_type="resource_entity",
        entity_id=_string_or_none(getattr(entity, "id", None) or _mapping_get(entity, "id")),
        organism=_string_or_none(getattr(entity, "organism", None) or _mapping_get(entity, "organism")),
        genome_build=_string_or_none(getattr(entity, "genome_build", None) or _mapping_get(entity, "genome_build")),
        confidence=_float_or_none(metadata.get("confidence")),
        provenance={
            "resource_role": _string_or_none(getattr(entity, "resource_role", None) or _mapping_get(entity, "resource_role")),
            "preferred_file_role": preferred_file_role,
        },
        metadata=metadata,
    )


def _resource_entity_primary_path(
    entity: Any,
    *,
    preferred_file_role: str | None = None,
) -> tuple[str | None, Any | None]:
    resource_files = list(getattr(entity, "resource_files", None) or _mapping_get(entity, "resource_files") or [])
    preferred = [
        resource_file
        for resource_file in resource_files
        if not preferred_file_role
        or _string_or_none(getattr(resource_file, "file_role", None) or _mapping_get(resource_file, "file_role")) == preferred_file_role
    ]
    ordered = sorted(
        preferred or resource_files,
        key=lambda item: (
            not bool(getattr(item, "is_primary", None) or _mapping_get(item, "is_primary")),
            _string_or_none(getattr(item, "file_role", None) or _mapping_get(item, "file_role")) or "",
        ),
    )
    for resource_file in ordered:
        file_obj = getattr(resource_file, "file", None) or _mapping_get(resource_file, "file")
        file_path = _string_or_none(getattr(file_obj, "path", None) or _mapping_get(file_obj, "path"))
        if file_path:
            return file_path, resource_file
    source_uri = _string_or_none(getattr(entity, "source_uri", None) or _mapping_get(entity, "source_uri"))
    if source_uri:
        return source_uri, None
    return None, None


def _mapping_get(value: Any, key: str) -> Any:
    if isinstance(value, dict):
        return value.get(key)
    return None


def _string_or_none(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "SemanticCandidate",
    "semantic_candidate_from_artifact_record",
    "semantic_candidate_from_filerun",
    "semantic_candidate_from_known_path",
    "semantic_candidate_from_project_file",
    "semantic_candidate_from_resource_entity",
]
