"""Shared scoring helpers for semantic candidates."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from tune.core.binding.semantic_candidates import SemanticCandidate

_FASTQ_EXTS = (".fastq", ".fastq.gz", ".fq", ".fq.gz")
_BAM_EXTS = (".bam",)
_SAM_EXTS = (".sam",)
_FASTA_EXTS = (".fa", ".fasta", ".fna", ".fa.gz", ".fasta.gz")
_GTF_EXTS = (".gtf", ".gff", ".gff3", ".gtf.gz", ".gff.gz", ".gff3.gz")


@dataclass(frozen=True)
class SemanticCandidateScore:
    score: int
    reason_codes: list[str] = field(default_factory=list)


def score_semantic_candidate(
    slot: Any,
    candidate: SemanticCandidate,
    *,
    preferred_lineage: dict[str, Any] | None = None,
    project_id: str | None = None,
    dependency_rank: int | None = None,
    source_type_override: str | None = None,
) -> SemanticCandidateScore:
    resolver_candidate = candidate.to_resolver_dict()
    total = 0
    reasons: list[str] = []

    role_score, role_reasons = _artifact_role_score(slot, resolver_candidate)
    total += role_score
    reasons.extend(role_reasons)
    if total <= 0:
        return SemanticCandidateScore(score=total, reason_codes=reasons)

    lineage_score, lineage_reasons = _lineage_score(preferred_lineage, candidate)
    total += lineage_score
    reasons.extend(lineage_reasons)

    source_score, source_reasons = _source_preference_score(source_type_override or candidate.source_type)
    total += source_score
    reasons.extend(source_reasons)

    scope_score, scope_reasons = _project_scope_score(project_id, candidate)
    total += scope_score
    reasons.extend(scope_reasons)

    override_score, override_reasons = _user_override_score(candidate)
    total += override_score
    reasons.extend(override_reasons)

    recency_score, recency_reasons = _recency_score(candidate)
    total += recency_score
    reasons.extend(recency_reasons)

    if dependency_rank is not None:
        dependency_bonus = max(5 - dependency_rank, 0)
        if dependency_bonus:
            total += dependency_bonus
            reasons.append("dependency_proximity")

    return SemanticCandidateScore(score=total, reason_codes=reasons)


def _artifact_role_score(slot: Any, candidate: dict[str, Any]) -> tuple[int, list[str]]:
    expected_roles = list(getattr(slot, "accepted_roles", None) or [])
    artifact_role = candidate.get("artifact_role")
    score = 0
    reasons: list[str] = []

    if artifact_role and expected_roles:
        if artifact_role in expected_roles:
            role_index = expected_roles.index(artifact_role)
            score += max(80 - role_index * 10, 50)
            reasons.append("role_exact" if role_index == 0 else "role_compatible")
        else:
            score -= 40
            reasons.append("role_mismatch")

    if candidate.get("slot_name") == getattr(slot, "name", None):
        score += 35
        reasons.append("slot_name_exact")

    if _file_matches_types(candidate.get("file_path", ""), list(getattr(slot, "file_types", []) or [])):
        score += 15
        reasons.append("file_type_match")

    return score, reasons


def _lineage_score(
    preferred_lineage: dict[str, Any] | None,
    candidate: SemanticCandidate,
) -> tuple[int, list[str]]:
    if not preferred_lineage:
        return 0, []

    score = 0
    reasons: list[str] = []

    if preferred_lineage.get("sample_id") and candidate.sample_id:
        if preferred_lineage["sample_id"] == candidate.sample_id:
            score += 25
            reasons.append("sample_match")
        else:
            score -= 20
            reasons.append("sample_mismatch")

    if preferred_lineage.get("experiment_id") and candidate.experiment_id:
        if preferred_lineage["experiment_id"] == candidate.experiment_id:
            score += 18
            reasons.append("experiment_match")
        else:
            score -= 12
            reasons.append("experiment_mismatch")

    if preferred_lineage.get("read_number") and candidate.read_number:
        if preferred_lineage["read_number"] == candidate.read_number:
            score += 12
            reasons.append("read_number_match")
        else:
            score -= 16
            reasons.append("read_number_mismatch")

    return score, reasons


def _source_preference_score(source_type: str) -> tuple[int, list[str]]:
    score_map = {
        "artifact_record": 30,
        "filerun": 35,
        "known_path": 24,
        "project_file": 12,
        "resource_entity": 22,
        "project_file_scan": 12,
        "user_confirmed": 24,
    }
    score = score_map.get(source_type, 0)
    return score, ([f"source_{source_type}"] if score else [])


def _project_scope_score(project_id: str | None, candidate: SemanticCandidate) -> tuple[int, list[str]]:
    if not project_id or not candidate.project_id:
        return 0, []
    if project_id == candidate.project_id:
        return 8, ["project_scope_match"]
    return -10, ["project_scope_mismatch"]


def _user_override_score(candidate: SemanticCandidate) -> tuple[int, list[str]]:
    if candidate.source_type == "known_path":
        return 6, ["explicit_user_override"]
    if str(candidate.source_type).lower() == "user_confirmed":
        return 6, ["explicit_user_override"]
    return 0, []


def _recency_score(candidate: SemanticCandidate) -> tuple[int, list[str]]:
    metadata = candidate.metadata or {}
    raw_value = metadata.get("created_at") or metadata.get("mtime") or metadata.get("updated_at")
    if not raw_value:
        return 0, []
    parsed = _parse_timestamp(raw_value)
    if parsed is None:
        return 0, []
    age_days = (datetime.now(timezone.utc) - parsed).total_seconds() / 86400.0
    if age_days <= 7:
        return 4, ["recent_candidate"]
    if age_days <= 30:
        return 2, ["moderately_recent_candidate"]
    return 0, []


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _file_matches_types(path: str, file_types: list[str]) -> bool:
    if not file_types or file_types == ["*"]:
        return True
    path_lower = str(path or "").lower()
    for file_type in file_types:
        if file_type == "*":
            return True
        normalized = "." + str(file_type).lstrip(".")
        if path_lower.endswith(normalized):
            return True
        if file_type == "fastq" and path_lower.endswith(_FASTQ_EXTS):
            return True
        if file_type == "bam" and path_lower.endswith(_BAM_EXTS):
            return True
        if file_type == "sam" and path_lower.endswith(_SAM_EXTS):
            return True
        if file_type in {"fa", "fasta", "fna"} and path_lower.endswith(_FASTA_EXTS):
            return True
        if file_type in {"gtf", "gff", "gff3"} and path_lower.endswith(_GTF_EXTS):
            return True
    return False


__all__ = ["SemanticCandidateScore", "score_semantic_candidate"]
