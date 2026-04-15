"""Input binding resolver — resolves input slot bindings for analysis steps.

Resolution priority for each slot:
1. Existing InputBinding record (already set by user or previous run)
2a. ArtifactRecord from upstream steps (Tier 1a — Phase 4, deterministic)
2b. Outputs from upstream steps via BFS dir scan (Tier 1b — fallback)
3. FileRun records with read_number (Tier 2 — authoritative R1/R2 assignment)
4. KnownPath records mapped via _KNOWN_PATH_SLOT_MAP (known_path)
5. Project files heuristic (tail-segment R1/R2 detection)
"""
from __future__ import annotations

import logging
import os
import uuid as _uuid_mod
from typing import TYPE_CHECKING, Any

from tune.core.binding.semantic_candidates import (
    SemanticCandidate,
    semantic_candidate_from_artifact_record,
    semantic_candidate_from_filerun,
    semantic_candidate_from_known_path,
    semantic_candidate_from_project_file,
)
from tune.core.binding.semantic_scoring import score_semantic_candidate

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession
    from tune.core.registry.steps import SlotDefinition

log = logging.getLogger(__name__)

# File extensions that match each slot file_type category
_FASTQ_EXTS = {".fastq", ".fastq.gz", ".fq", ".fq.gz"}
_BAM_EXTS = {".bam"}
_SAM_EXTS = {".sam"}
_FASTA_EXTS = {".fa", ".fasta", ".fna"}
_GTF_EXTS = {".gtf", ".gff", ".gff3"}

# Maps KnownPath.key → renderer slot name for known bioinformatics resources.
# Single source of truth — previously duplicated in tasks.py.
_KNOWN_PATH_SLOT_MAP: dict[str, str] = {
    "hisat2_index":    "index_prefix",   # align.hisat2
    "reference_fasta": "reference_fasta",  # util.hisat2_build (kept as-is; also maps below)
    "star_genome_dir": "genome_dir",     # align.star
    "annotation_gtf":  "annotation_gtf", # quant.featurecounts, align.star (optional)
    "annotation_bed":  "annotation_bed",
    "bwa_index":       "index_prefix",
    "bowtie2_index":   "index_prefix",
}

# Secondary mapping: reference_fasta → index_prefix for align.hisat2 fallback
# (when no explicit hisat2_index KnownPath exists).
# Stored separately so preflight can detect the FASTA→build injection case.
_REFERENCE_FASTA_AS_INDEX = "index_prefix"


def _file_matches_types(path: str, file_types: list[str]) -> bool:
    """Return True if path's extension matches any of the given file_types."""
    if not file_types or file_types == ["*"]:
        return True
    path_lower = path.lower()
    for ft in file_types:
        if ft == "*":
            return True
        # Handle compound extensions like "fastq.gz"
        if path_lower.endswith("." + ft.lstrip(".")):
            return True
        # Also check against logical categories
        if ft in ("fastq",) and any(path_lower.endswith(e) for e in _FASTQ_EXTS):
            return True
        if ft == "bam" and path_lower.endswith(".bam"):
            return True
        if ft == "sam" and path_lower.endswith(".sam"):
            return True
        if ft in ("fa", "fasta", "fna") and any(path_lower.endswith(e) for e in _FASTA_EXTS):
            return True
        if ft in ("gtf", "gff") and any(path_lower.endswith(e) for e in _GTF_EXTS):
            return True
    return False


async def _resolve_reads_from_filerun(
    project_id: str,
    slot_name: str,
    db: "AsyncSession",
) -> list[str]:
    """Tier 2: resolve read paths from FileRun.read_number.

    Returns list of file paths for the requested read direction:
    - slot_name='read1' → files with read_number=1
    - slot_name='read2' → files with read_number=2
    - slot_name='reads' → all FASTQ files linked via FileRun (any read_number)
    - other slots      → empty list
    """
    if slot_name not in ("read1", "read2", "reads"):
        return []

    if slot_name == "read1":
        target_read_numbers = [1]
    elif slot_name == "read2":
        target_read_numbers = [2]
    else:  # "reads" — return all
        target_read_numbers = [1, 2, None]

    try:
        from tune.core.models import FileRun, File, Experiment, Sample
        from sqlalchemy import select

        stmt = (
            select(File.path)
            .join(FileRun, FileRun.file_id == File.id)
            .join(Experiment, FileRun.experiment_id == Experiment.id)
            .join(Sample, Experiment.sample_id == Sample.id)
            .where(Sample.project_id == project_id)
            .order_by(File.path)
        )
        if slot_name != "reads":
            stmt = stmt.where(FileRun.read_number == target_read_numbers[0])
        result = await db.execute(stmt)
        return [row[0] for row in result.all() if row[0]]
    except Exception:
        log.exception(
            "_resolve_reads_from_filerun: failed for project=%s slot=%s",
            project_id, slot_name,
        )
        return []


async def load_known_path_bindings(project_id: str, db: "AsyncSession") -> dict[str, str]:
    """Return {slot_name: path} for all KnownPath records for a project.

    Uses _KNOWN_PATH_SLOT_MAP to map KnownPath.key → renderer slot name.
    The 'reference_fasta' key maps to slot 'reference_fasta' here so preflight
    can detect the FASTA-registered-but-no-index case; align.hisat2's
    'index_prefix' slot is handled separately by the pre-flight injector.
    """
    if not project_id:
        return {}
    try:
        from tune.core.models import KnownPath
        from sqlalchemy import select

        rows = (await db.execute(
            select(KnownPath).where(KnownPath.project_id == project_id)
        )).scalars().all()
        return {
            _KNOWN_PATH_SLOT_MAP.get(row.key, row.key): row.path
            for row in rows
        }
    except Exception:
        log.exception("load_known_path_bindings: failed for project=%s", project_id)
        return {}


async def load_registered_resource_bindings(
    project_id: str,
    db: "AsyncSession",
) -> dict[str, str]:
    """Return renderer slot bindings for project-level resources.

    Main resolution policy:
    - reference / annotation come from KnownPath
    - aligner indices come from DerivedResourceCache first
    - legacy index KnownPath entries remain compatibility fallback only
    """
    from tune.core.resources.cache import DerivedResourceCache, _check_index_exists

    bindings = await load_known_path_bindings(project_id, db)
    resource_bindings = {
        key: value
        for key, value in bindings.items()
        if key in {"reference_fasta", "annotation_gtf", "annotation_bed"}
    }

    cache = DerivedResourceCache()
    aligner_slot_map = {
        "hisat2": "index_prefix",
        "star": "genome_dir",
        "bwa": "index_prefix",
        "bowtie2": "index_prefix",
    }
    legacy_slot_map = {
        "hisat2": bindings.get("index_prefix"),
        "star": bindings.get("genome_dir") or bindings.get("star_genome_dir"),
        "bwa": bindings.get("index_prefix"),
        "bowtie2": bindings.get("index_prefix"),
    }

    for aligner, slot_name in aligner_slot_map.items():
        cached = await cache.get(
            project_id=project_id,
            kind="aligner_index",
            aligner=aligner,
            db=db,
        )
        if cached and cached.status == "ready" and cached.resolved_path:
            resource_bindings[slot_name] = cached.resolved_path
            continue

        legacy_path = legacy_slot_map.get(aligner)
        if legacy_path and _check_index_exists(legacy_path, aligner):
            resource_bindings[slot_name] = legacy_path

    return resource_bindings


async def _resolve_from_artifact_records(
    job_id: str,
    dep_key: str,
    slot: "SlotDefinition",
    db: "AsyncSession",
) -> str | None:
    match = await _resolve_artifact_match(job_id, [dep_key], slot, db)
    return match["file_path"] if match else None


def _candidate_lineage(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "sample_id": candidate.get("sample_id"),
        "experiment_id": candidate.get("experiment_id"),
        "sample_name": candidate.get("sample_name"),
        "read_number": candidate.get("read_number"),
    }


def _infer_project_file_role(slot: "SlotDefinition", project_file: dict[str, Any]) -> str | None:
    slot_name = slot.name
    file_type = str(project_file.get("file_type") or "").lower()
    read_number = project_file.get("read_number")
    path = str(project_file.get("path") or "")

    if slot_name == "read1":
        return "raw_reads_read1" if read_number in {None, 1} else None
    if slot_name == "read2":
        return "raw_reads_read2" if read_number in {None, 2} else None
    if slot_name == "reads":
        if read_number == 1:
            return "raw_reads_read1"
        if read_number == 2:
            return "raw_reads_read2"
        return "raw_reads"
    if file_type in {"fasta", "fa", "fna"} or any(path.lower().endswith(ext) for ext in (".fa", ".fasta", ".fna", ".fa.gz", ".fasta.gz")):
        return "reference_fasta"
    if file_type in {"gtf", "gff", "gff3"} or any(path.lower().endswith(ext) for ext in (".gtf", ".gff", ".gff3", ".gtf.gz", ".gff.gz", ".gff3.gz")):
        return "annotation_gtf"
    if slot_name == "genome_dir" and path and os.path.isfile(os.path.join(path, "SA")):
        return "star_genome_dir"
    return None


def _build_external_candidate(
    *,
    slot: "SlotDefinition",
    source_type: str,
    file_path: str,
    source_ref: str | None,
    artifact_role: str | None,
    preferred_lineage: dict[str, Any] | None,
    sample_id: str | None = None,
    experiment_id: str | None = None,
    sample_name: str | None = None,
    read_number: int | None = None,
    project_id: str | None = None,
) -> dict[str, Any] | None:
    if source_type == "known_path":
        semantic_candidate = semantic_candidate_from_known_path(
            known_path_key=str(source_ref or slot.name),
            path=file_path,
            artifact_role=artifact_role,
            slot_name=slot.name,
            project_id=project_id,
        )
    elif source_type == "project_file":
        semantic_candidate = semantic_candidate_from_project_file(
            {
                "id": source_ref,
                "path": file_path,
                "linked_sample_id": sample_id,
                "linked_experiment_id": experiment_id,
                "sample_name": sample_name,
                "read_number": read_number,
            },
            artifact_role=artifact_role,
            slot_name=slot.name,
            project_id=project_id,
        )
    elif source_type == "filerun":
        semantic_candidate = semantic_candidate_from_filerun(
            {
                "file_path": file_path,
                "source_type": source_type,
                "source_ref": source_ref,
                "artifact_role": artifact_role,
                "sample_id": sample_id,
                "experiment_id": experiment_id,
                "sample_name": sample_name,
                "read_number": read_number,
            },
            project_id=project_id,
        )
    else:
        semantic_candidate = SemanticCandidate(
            file_path=file_path,
            source_type=source_type,
            source_ref=source_ref,
            artifact_role=artifact_role,
            project_id=project_id,
            sample_id=sample_id,
            experiment_id=experiment_id,
            sample_name=sample_name,
            read_number=read_number,
        )
    if semantic_candidate is None:
        return None

    candidate = semantic_candidate.to_resolver_dict()

    score_result = score_semantic_candidate(
        slot,
        semantic_candidate,
        preferred_lineage=preferred_lineage,
        project_id=project_id,
    )
    if score_result.score <= 0:
        return None
    score = score_result.score
    reasons = list(score_result.reason_codes)

    explanation = {
        "candidate_source": source_type,
        "score": score,
        "reason_codes": reasons,
        "source_ref": source_ref,
        "artifact_role": artifact_role,
        "expected_roles": list(slot.accepted_roles or []),
        "lineage": _candidate_lineage(candidate),
    }
    return {
        "file_path": file_path,
        "dep_key": source_ref,
        "score": score,
        "source_type": source_type,
        "source_ref": source_ref,
        "lineage": _candidate_lineage(candidate),
        "explanation": explanation,
    }


async def _resolve_read_candidates_from_filerun(
    project_id: str,
    slot_name: str,
    db: "AsyncSession",
) -> list[dict[str, Any]]:
    if slot_name not in {"read1", "read2", "reads"}:
        return []
    try:
        from tune.core.models import File, FileRun, Experiment, Sample
        from sqlalchemy import select

        stmt = (
            select(
                File.path,
                FileRun.read_number,
                Experiment.id,
                Sample.id,
                Sample.sample_name,
            )
            .join(FileRun, FileRun.file_id == File.id)
            .join(Experiment, FileRun.experiment_id == Experiment.id)
            .join(Sample, Experiment.sample_id == Sample.id)
            .where(Sample.project_id == project_id)
            .order_by(File.path)
        )
        if slot_name == "read1":
            stmt = stmt.where(FileRun.read_number == 1)
        elif slot_name == "read2":
            stmt = stmt.where(FileRun.read_number == 2)

        rows = (await db.execute(stmt)).all()
        candidates: list[dict[str, Any]] = []
        for row in rows:
            read_number = row[1]
            artifact_role = (
                "raw_reads_read1"
                if read_number == 1
                else "raw_reads_read2"
                if read_number == 2
                else "raw_reads"
            )
            candidates.append(
                {
                    "file_path": row[0],
                    "source_type": "filerun",
                    "source_ref": row[2],
                    "artifact_role": artifact_role,
                    "sample_id": row[3],
                    "experiment_id": row[2],
                    "sample_name": row[4],
                    "read_number": read_number,
                }
            )
        if candidates:
            return candidates
    except Exception:
        log.exception(
            "_resolve_read_candidates_from_filerun: failed for project=%s slot=%s",
            project_id, slot_name,
        )

    # Compatibility fallback: preserve the earlier path-based resolver so
    # preflight and older tests still work when FileRun joins are unavailable.
    fallback_paths = await _resolve_reads_from_filerun(project_id, slot_name, db)
    if not fallback_paths:
        return []

    if slot_name == "read1":
        fallback_role = "raw_reads_read1"
        read_number = 1
    elif slot_name == "read2":
        fallback_role = "raw_reads_read2"
        read_number = 2
    else:
        fallback_role = "raw_reads"
        read_number = None

    return [
        {
            "file_path": path,
            "source_type": "filerun",
            "source_ref": path,
            "artifact_role": fallback_role,
            "sample_id": None,
            "experiment_id": None,
            "sample_name": "",
            "read_number": read_number,
        }
        for path in fallback_paths
        if path
    ]


def _build_artifact_match(
    dep_key: str,
    dep_index: int,
    slot: "SlotDefinition",
    artifact: dict[str, Any],
    preferred_lineage: dict[str, Any] | None = None,
    project_id: str | None = None,
) -> dict[str, Any] | None:
    semantic_candidate = semantic_candidate_from_artifact_record(
        artifact,
        dep_key=dep_key,
        project_id=project_id,
    )
    if semantic_candidate is None:
        return None
    candidate = semantic_candidate.to_resolver_dict()
    file_path = candidate.get("file_path")

    score_result = score_semantic_candidate(
        slot,
        semantic_candidate,
        preferred_lineage=preferred_lineage,
        project_id=project_id,
        dependency_rank=dep_index,
        source_type_override="artifact_record",
    )
    if score_result.score <= 0:
        return None
    score = score_result.score
    reasons = list(score_result.reason_codes)

    explanation = {
        "candidate_source": "artifact_record",
        "score": score,
        "reason_codes": reasons,
        "source_step_key": dep_key,
        "source_slot_name": candidate.get("slot_name"),
        "source_step_type": candidate.get("step_type"),
        "artifact_role": candidate.get("artifact_role"),
        "artifact_scope": candidate.get("artifact_scope"),
        "expected_roles": list(slot.accepted_roles or []),
        "lineage": _candidate_lineage(candidate),
    }
    return {
        "file_path": file_path,
        "dep_key": dep_key,
        "score": score,
        "source_type": "artifact_record",
        "source_ref": dep_key,
        "lineage": _candidate_lineage(artifact),
        "explanation": explanation,
    }


def _lineage_matches_preference(
    preferred_lineage: dict[str, Any] | None,
    candidate_lineage: dict[str, Any] | None,
) -> bool:
    if not preferred_lineage:
        return False
    if not candidate_lineage:
        return False

    sample_id = preferred_lineage.get("sample_id")
    experiment_id = preferred_lineage.get("experiment_id")
    read_number = preferred_lineage.get("read_number")

    if sample_id and candidate_lineage.get("sample_id") != sample_id:
        return False
    if experiment_id and candidate_lineage.get("experiment_id") != experiment_id:
        return False
    if read_number and candidate_lineage.get("read_number") != read_number:
        return False
    return True


def _dedupe_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen_paths: set[str] = set()
    for candidate in candidates:
        path = candidate.get("file_path")
        if not path or path in seen_paths:
            continue
        deduped.append(candidate)
        seen_paths.add(path)
    return deduped


def _collapse_multi_candidates_by_lineage(
    slot: "SlotDefinition",
    candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Keep only the best candidate per lineage for per-sample multi-input slots."""
    if not slot.multiple or getattr(slot, "artifact_scope", None) != "per_sample":
        return candidates

    collapsed: list[dict[str, Any]] = []
    seen_lineages: set[tuple[str, str, str, str | int | None]] = set()
    for candidate in candidates:
        lineage = candidate.get("lineage") or {}
        lineage_key = (
            str(lineage.get("sample_id") or ""),
            str(lineage.get("experiment_id") or ""),
            str(lineage.get("sample_name") or ""),
            lineage.get("read_number"),
        )
        if not any(lineage_key[:3]) and lineage_key[3] is None:
            collapsed.append(candidate)
            continue
        if lineage_key in seen_lineages:
            continue
        seen_lineages.add(lineage_key)
        collapsed.append(candidate)
    return collapsed


async def _resolve_artifact_candidates(
    job_id: str,
    dep_keys: list[str],
    slot: "SlotDefinition",
    db: "AsyncSession",
    preferred_lineage: dict[str, Any] | None = None,
    project_id: str | None = None,
) -> list[dict[str, Any]]:
    """Tier 1a: query ArtifactRecord for upstream outputs matching a slot."""
    try:
        from tune.core.binding.artifacts import load_artifacts_for_step

        candidates: list[dict[str, Any]] = []
        for dep_index, dep_key in enumerate(dep_keys):
            artifacts = await load_artifacts_for_step(job_id, dep_key, db)
            for artifact in artifacts:
                candidate = _build_artifact_match(
                    dep_key,
                    dep_index,
                    slot,
                    artifact,
                    preferred_lineage=preferred_lineage,
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
        return _dedupe_candidates(candidates)
    except Exception:
        log.exception(
            "_resolve_artifact_candidates: failed for job=%s deps=%s slot=%s",
            job_id, dep_keys, slot.name,
        )
        return []


async def _resolve_artifact_match(
    job_id: str,
    dep_keys: list[str],
    slot: "SlotDefinition",
    db: "AsyncSession",
    preferred_lineage: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    candidates = await _resolve_artifact_candidates(
        job_id,
        dep_keys,
        slot,
        db,
        preferred_lineage=preferred_lineage,
    )
    return candidates[0] if candidates else None


async def _select_semantic_candidates(
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
    from tune.core.binding.semantic_retrieval import retrieve_semantic_candidates

    return await retrieve_semantic_candidates(
        job_id=job_id,
        dep_keys=dep_keys,
        slot=slot,
        project_id=project_id,
        project_files=project_files,
        kp_bindings=kp_bindings,
        db=db,
        preferred_lineage=preferred_lineage,
    )


async def _select_semantic_candidate(
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
    from tune.core.binding.semantic_retrieval import retrieve_best_semantic_candidate

    return await retrieve_best_semantic_candidate(
        job_id=job_id,
        dep_keys=dep_keys,
        slot=slot,
        project_id=project_id,
        project_files=project_files,
        kp_bindings=kp_bindings,
        db=db,
        preferred_lineage=preferred_lineage,
    )


def _result_rows(result) -> list[Any]:
    try:
        rows = result.scalars().all()
        if isinstance(rows, (list, tuple)):
            return list(rows)
    except Exception:
        pass

    try:
        row = result.scalar_one_or_none()
    except Exception:
        return []
    return [row] if row is not None else []


def _binding_payload_from_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "resolved_path": candidate.get("file_path"),
        "source_type": candidate.get("source_type"),
        "source_ref": candidate.get("source_ref"),
        "match_metadata": candidate.get("explanation"),
    }


def _binding_payload_from_bfs(dep_key: str, file_path: str) -> dict[str, Any]:
    return {
        "resolved_path": file_path,
        "source_type": "step_output",
        "source_ref": dep_key,
        "match_metadata": None,
    }


def _binding_payload_from_upstream_dir(dep_key: str, output_dir: str) -> dict[str, Any]:
    return {
        "resolved_path": output_dir,
        "source_type": "step_output",
        "source_ref": dep_key,
        "match_metadata": {
            "source_step_key": dep_key,
            "source_slot_name": "output_dir",
            "reason_codes": ["dependency_proximity"],
        },
    }


def _apply_binding_payload(binding, payload: dict[str, Any] | None, status: str) -> None:
    payload = payload or {}
    binding.source_type = payload.get("source_type")
    binding.source_ref = payload.get("source_ref")
    binding.resolved_path = payload.get("resolved_path")
    binding.match_metadata_json = payload.get("match_metadata")
    binding.status = status


def _collect_transitive_dep_keys(
    step: dict[str, Any],
    steps_by_key: dict[str, dict[str, Any]],
) -> list[str]:
    """Return direct and indirect upstream step keys in BFS order."""
    queue = [str(dep).strip() for dep in (step.get("depends_on") or []) if str(dep).strip()]
    ordered: list[str] = []
    seen: set[str] = set()

    while queue:
        dep_key = queue.pop(0)
        dep_key_norm = dep_key.lower()
        if dep_key_norm in seen:
            continue
        seen.add(dep_key_norm)
        ordered.append(dep_key)
        dep_step = steps_by_key.get(dep_key_norm)
        if dep_step:
            for upstream_key in dep_step.get("depends_on") or []:
                upstream_key = str(upstream_key).strip()
                if upstream_key:
                    queue.append(upstream_key)

    return ordered


async def resolve_bindings(
    job_id: str,
    steps: list[dict],  # list of step dicts with step_key, step_type, step_run_id
    project_files: list[dict],  # [{id, path, filename, file_type}, ...]
    db: "AsyncSession",
    target_step_keys: set[str] | None = None,
) -> list[str]:
    """Resolve input bindings for selected steps of a job."""
    import os
    from tune.core.models import InputBinding, AnalysisJob
    from tune.core.registry import get_step_type
    from sqlalchemy import select

    unresolved: list[str] = []
    target_step_keys_norm = {key.lower() for key in (target_step_keys or set()) if key}
    steps_by_key = {
        (step.get("step_key") or step.get("name") or "").strip().lower(): step
        for step in steps
        if (step.get("step_key") or step.get("name") or "").strip()
    }

    step_run_map: dict[str, dict] = {}
    for step in steps:
        step_key = (step.get("step_key") or step.get("name") or "").strip()
        if not step_key:
            continue
        step_run_map[step_key.lower()] = {
            "run_id": step.get("run_id") or step.get("_run_id"),
            "output_dir": step.get("output_dir"),
        }

    project_id: str | None = None
    try:
        job_res = await db.execute(select(AnalysisJob).where(AnalysisJob.id == job_id))
        job = job_res.scalar_one_or_none()
        if job:
            project_id = job.project_id
    except Exception:
        log.exception("resolve_bindings: failed to load job project_id")

    kp_bindings: dict[str, str] = {}
    if project_id:
        kp_bindings = await load_registered_resource_bindings(project_id, db)

    for step in steps:
        step_key = (step.get("step_key") or step.get("name") or "").strip()
        if not step_key:
            continue
        if target_step_keys_norm and step_key.lower() not in target_step_keys_norm:
            continue

        step_type = step.get("step_type") or ""
        run_id = step.get("run_id") or step.get("_run_id")
        defn = get_step_type(step_type)
        if defn is None:
            log.warning("resolve_bindings: unknown step_type %s for step %s", step_type, step_key)
            continue

        preferred_lineage: dict[str, Any] | None = dict(step.get("_preferred_lineage") or {}) or None
        transitive_dep_keys = _collect_transitive_dep_keys(step, steps_by_key)

        for slot in defn.input_slots:
            existing_result = await db.execute(
                select(InputBinding).where(
                    InputBinding.job_id == job_id,
                    InputBinding.step_id == run_id,
                    InputBinding.slot_name == slot.name,
                )
            )
            existing_bindings = [row for row in _result_rows(existing_result) if row is not None]
            resolved_existing = [
                binding
                for binding in existing_bindings
                if getattr(binding, "status", None) == "resolved"
                and getattr(binding, "resolved_path", None)
            ]
            if resolved_existing:
                existing_meta = getattr(resolved_existing[0], "match_metadata_json", None) or {}
                if existing_meta.get("lineage"):
                    preferred_lineage = existing_meta["lineage"]
                continue

            selected_payloads: list[dict[str, Any]] = []
            semantic_candidates = await _select_semantic_candidates(
                job_id=job_id,
                dep_keys=transitive_dep_keys,
                slot=slot,
                project_id=project_id,
                project_files=project_files,
                kp_bindings=kp_bindings,
                db=db,
                preferred_lineage=preferred_lineage,
            )
            if semantic_candidates:
                if slot.multiple:
                    selected_payloads = [
                        _binding_payload_from_candidate(candidate)
                        for candidate in semantic_candidates
                    ]
                else:
                    selected_payloads = [
                        _binding_payload_from_candidate(semantic_candidates[0])
                    ]

            if not selected_payloads:
                if getattr(slot, "from_upstream_dir", False):
                    for dep_key in step.get("depends_on") or []:
                        dep = step_run_map.get(dep_key.lower())
                        dep_output_dir = dep.get("output_dir") if dep else None
                        if dep_output_dir:
                            selected_payloads = [
                                _binding_payload_from_upstream_dir(dep_key, dep_output_dir)
                            ]
                            break

            if not selected_payloads:
                bfs_payloads: list[dict[str, Any]] = []
                for dep_key in step.get("depends_on") or []:
                    dep = step_run_map.get(dep_key.lower())
                    if not dep or not dep.get("output_dir"):
                        continue
                    for dirpath, _, filenames in os.walk(dep["output_dir"]):
                        for fname in sorted(filenames):
                            fpath = os.path.join(dirpath, fname)
                            if not _file_matches_types(fpath, slot.file_types):
                                continue
                            payload = _binding_payload_from_bfs(dep_key, fpath)
                            if slot.multiple:
                                bfs_payloads.append(payload)
                            else:
                                selected_payloads = [payload]
                                break
                        if (slot.multiple and bfs_payloads) or selected_payloads:
                            if not slot.multiple:
                                break
                    if selected_payloads and not slot.multiple:
                        break
                if slot.multiple and bfs_payloads:
                    deduped_payloads: list[dict[str, Any]] = []
                    seen_paths: set[str] = set()
                    for payload in bfs_payloads:
                        path_value = payload.get("resolved_path")
                        if path_value and path_value not in seen_paths:
                            deduped_payloads.append(payload)
                            seen_paths.add(path_value)
                    selected_payloads = deduped_payloads

            status = "resolved" if selected_payloads else "missing"
            if status == "missing" and slot.required:
                unresolved.append(f"{step_key}.{slot.name}")

            if selected_payloads:
                first_meta = selected_payloads[0].get("match_metadata") or {}
                if first_meta.get("lineage"):
                    preferred_lineage = first_meta["lineage"]

            if slot.multiple:
                bindings_to_update = list(existing_bindings)
                if not selected_payloads and not bindings_to_update:
                    binding = InputBinding(
                        id=str(_uuid_mod.uuid4()),
                        job_id=job_id,
                        step_id=run_id or "",
                        slot_name=slot.name,
                        source_type=None,
                        source_ref=None,
                        resolved_path=None,
                        match_metadata_json=None,
                        status="missing",
                    )
                    db.add(binding)
                    continue

                for idx, payload in enumerate(selected_payloads):
                    if idx < len(bindings_to_update):
                        binding = bindings_to_update[idx]
                    else:
                        binding = InputBinding(
                            id=str(_uuid_mod.uuid4()),
                            job_id=job_id,
                            step_id=run_id or "",
                            slot_name=slot.name,
                            source_type=None,
                            source_ref=None,
                            resolved_path=None,
                            match_metadata_json=None,
                            status="missing",
                        )
                        db.add(binding)
                        bindings_to_update.append(binding)
                    _apply_binding_payload(binding, payload, "resolved")

                for binding in bindings_to_update[len(selected_payloads):]:
                    _apply_binding_payload(binding, None, "missing")
                continue

            binding = existing_bindings[0] if existing_bindings else None
            if binding is None:
                binding = InputBinding(
                    id=str(_uuid_mod.uuid4()),
                    job_id=job_id,
                    step_id=run_id or "",
                    slot_name=slot.name,
                    source_type=None,
                    source_ref=None,
                    resolved_path=None,
                    match_metadata_json=None,
                    status="missing",
                )
                db.add(binding)

            _apply_binding_payload(binding, selected_payloads[0] if selected_payloads else None, status)

            for extra_binding in existing_bindings[1:]:
                _apply_binding_payload(extra_binding, None, "missing")

    return unresolved
