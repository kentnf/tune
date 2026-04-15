"""Shared semantic retrieval service for binding-related candidate lookup.

This layer keeps preview / preflight / execution on the same candidate assembly
path so they do not drift as the resolver evolves.
"""
from __future__ import annotations

from typing import Any, TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from tune.core.binding.semantic_candidates import semantic_candidate_from_resource_entity

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession
    from tune.core.registry.steps import SlotDefinition


async def retrieve_semantic_candidates(
    *,
    job_id: str,
    dep_keys: list[str],
    slot: "SlotDefinition",
    project_id: str | None,
    project_files: list[dict],
    kp_bindings: dict[str, str],
    db: "AsyncSession",
    preferred_lineage: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    from tune.core.binding import resolver as _resolver

    candidates: list[dict[str, Any]] = []

    candidates.extend(
        await _resolver._resolve_artifact_candidates(
            job_id,
            dep_keys,
            slot,
            db,
            preferred_lineage=preferred_lineage,
            project_id=project_id,
        )
    )

    if project_id and slot.name in {"read1", "read2", "reads"}:
        filerun_candidates = await _resolver._resolve_read_candidates_from_filerun(project_id, slot.name, db)
        for candidate in filerun_candidates:
            external_candidate = _resolver._build_external_candidate(
                slot=slot,
                source_type="filerun",
                file_path=candidate["file_path"],
                source_ref=candidate.get("source_ref"),
                artifact_role=candidate.get("artifact_role"),
                preferred_lineage=preferred_lineage,
                sample_id=candidate.get("sample_id"),
                experiment_id=candidate.get("experiment_id"),
                sample_name=candidate.get("sample_name"),
                read_number=candidate.get("read_number"),
                project_id=project_id,
            )
            if external_candidate:
                candidates.append(external_candidate)

    if project_id and slot.name in {"reference_fasta", "annotation_gtf", "annotation_bed", "index_prefix", "genome_dir"}:
        resource_candidates = await _load_resource_entity_candidates(project_id, slot, db)
        for semantic_candidate in resource_candidates:
            candidate = _resolver._build_external_candidate(
                slot=slot,
                source_type=semantic_candidate.source_type,
                file_path=semantic_candidate.file_path,
                source_ref=semantic_candidate.source_ref,
                artifact_role=semantic_candidate.artifact_role,
                preferred_lineage=preferred_lineage,
                sample_id=semantic_candidate.sample_id,
                experiment_id=semantic_candidate.experiment_id,
                sample_name=semantic_candidate.sample_name,
                read_number=semantic_candidate.read_number,
                project_id=project_id,
            )
            if candidate:
                candidates.append(candidate)

    kp_path = kp_bindings.get(slot.name)
    if kp_path:
        kp_role = (
            slot.accepted_roles[0]
            if slot.accepted_roles
            else _resolver._infer_project_file_role(slot, {"path": kp_path, "file_type": "", "read_number": None})
        )
        known_candidate = _resolver._build_external_candidate(
            slot=slot,
            source_type="known_path",
            file_path=kp_path,
            source_ref=slot.name,
            artifact_role=kp_role,
            preferred_lineage=preferred_lineage,
            project_id=project_id,
        )
        if known_candidate:
            candidates.append(known_candidate)

    for project_file in project_files:
        if not _resolver._file_matches_types(project_file.get("path", ""), slot.file_types):
            continue
        artifact_role = _resolver._infer_project_file_role(slot, project_file)
        if slot.accepted_roles and artifact_role is None:
            continue
        candidate = _resolver._build_external_candidate(
            slot=slot,
            source_type="project_file",
            file_path=project_file.get("path", ""),
            source_ref=project_file.get("id"),
            artifact_role=artifact_role,
            preferred_lineage=preferred_lineage,
            sample_id=project_file.get("linked_sample_id"),
            experiment_id=project_file.get("linked_experiment_id"),
            sample_name=project_file.get("sample_name"),
            read_number=project_file.get("read_number"),
            project_id=project_id,
        )
        if candidate:
            candidates.append(candidate)

    candidates.sort(
        key=lambda candidate: (
            candidate["score"],
            candidate["file_path"],
        ),
        reverse=True,
    )
    candidates = _resolver._dedupe_candidates(candidates)
    candidates = _resolver._collapse_multi_candidates_by_lineage(slot, candidates)

    if slot.multiple and preferred_lineage:
        lineage_matches = [
            candidate
            for candidate in candidates
            if _resolver._lineage_matches_preference(preferred_lineage, candidate.get("lineage"))
        ]
        if lineage_matches:
            return lineage_matches

    return candidates


async def _load_resource_entity_candidates(
    project_id: str,
    slot: "SlotDefinition",
    db: "AsyncSession",
) -> list[Any]:
    from tune.core.models import ResourceEntity, ResourceFile

    preferred_file_role = _preferred_resource_file_role(slot.name)
    stmt = (
        select(ResourceEntity)
        .where(ResourceEntity.project_id == project_id)
        .options(
            selectinload(ResourceEntity.resource_files).selectinload(ResourceFile.file)
        )
        .order_by(ResourceEntity.updated_at.desc(), ResourceEntity.created_at.desc())
    )
    rows = (await db.execute(stmt)).scalars().all()
    candidates = [
        semantic_candidate_from_resource_entity(entity, preferred_file_role=preferred_file_role)
        for entity in rows
    ]
    return [candidate for candidate in candidates if candidate is not None]


def _preferred_resource_file_role(slot_name: str) -> str | None:
    if slot_name in {"reference_fasta", "annotation_gtf", "annotation_bed"}:
        return slot_name
    if slot_name in {"index_prefix", "genome_dir"}:
        return slot_name
    return None


async def retrieve_best_semantic_candidate(
    *,
    job_id: str,
    dep_keys: list[str],
    slot: "SlotDefinition",
    project_id: str | None,
    project_files: list[dict],
    kp_bindings: dict[str, str],
    db: "AsyncSession",
    preferred_lineage: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    candidates = await retrieve_semantic_candidates(
        job_id=job_id,
        dep_keys=dep_keys,
        slot=slot,
        project_id=project_id,
        project_files=project_files,
        kp_bindings=kp_bindings,
        db=db,
        preferred_lineage=preferred_lineage,
    )
    return candidates[0] if candidates else None


def summarize_candidate_ambiguity(
    candidates: list[dict[str, Any]],
    *,
    max_score_gap: int = 10,
) -> dict[str, Any] | None:
    if len(candidates) < 2:
        return None

    first = candidates[0]
    second = candidates[1]
    first_path = str(first.get("file_path") or "").strip()
    second_path = str(second.get("file_path") or "").strip()
    if not first_path or not second_path or first_path == second_path:
        return None

    first_score = int(first.get("score") or 0)
    second_score = int(second.get("score") or 0)
    score_gap = first_score - second_score
    if score_gap < 0 or score_gap > max_score_gap:
        return None

    return {
        "primary_path": first_path,
        "secondary_path": second_path,
        "primary_source_type": first.get("source_type"),
        "secondary_source_type": second.get("source_type"),
        "primary_score": first_score,
        "secondary_score": second_score,
        "score_gap": score_gap,
        "candidate_count": len(candidates),
    }


__all__ = [
    "retrieve_semantic_candidates",
    "retrieve_best_semantic_candidate",
    "summarize_candidate_ambiguity",
]
