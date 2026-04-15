"""Data models for structured biological context assembly."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Literal, Optional

if TYPE_CHECKING:
    from tune.core.resources.models import ResourceSummary

# ---------------------------------------------------------------------------
# EnhancedMetadata keys that describe the FILE ITSELF (content-intrinsic).
# Only these keys are read from the EnhancedMetadata table when building
# FilePlannerInfo.intrinsic.  Relational attributes (organism, sample_id,
# experiment_type, paired_end) must NOT be stored here — they live in the
# Project / Sample / Experiment / FileRun relational model.
# ---------------------------------------------------------------------------
INTRINSIC_META_KEYS: frozenset[str] = frozenset(
    {
        "reference_genome",
        "genome_build",
        "schema",
        "queryable",
        "read_length",
        "quality_encoding",
        "notes",
    }
)


# ---------------------------------------------------------------------------
# Input scope — controls what the builder queries
# ---------------------------------------------------------------------------


@dataclass
class ContextScope:
    """Determines which data to include in the assembled PlannerContext."""

    project_id: Optional[str] = None
    file_ids: Optional[list[str]] = None
    mode: Literal["project", "file_set", "global"] = "project"


# ---------------------------------------------------------------------------
# Per-entity info dataclasses
# ---------------------------------------------------------------------------


@dataclass
class FilePlannerInfo:
    id: str
    path: str
    filename: str
    file_type: str
    read_number: Optional[int]              # 1, 2, or None (single-end / unlinked)
    linked_sample_id: Optional[str]         # None if not linked to any Sample
    linked_experiment_id: Optional[str]     # None if not linked to any Experiment
    intrinsic: dict[str, str]               # From EnhancedMetadata (INTRINSIC_META_KEYS only)


@dataclass
class SamplePlannerInfo:
    id: str
    sample_name: str
    organism: Optional[str]
    attrs: dict                             # tissue, treatment, replicate, sex, genotype, …


@dataclass
class ExperimentPlannerInfo:
    id: str
    sample_id: str
    library_strategy: Optional[str]        # RNA-Seq | WGS | ChIP-Seq | ATAC-Seq …
    library_layout: Optional[str]          # PAIRED | SINGLE
    platform: Optional[str]                # ILLUMINA | PACBIO_SMRT …
    instrument_model: Optional[str]
    file_ids: list[str]                     # File IDs linked via FileRun


@dataclass
class ProjectPlannerInfo:
    id: str
    name: str
    project_dir: str
    project_goal: Optional[str]
    project_info: dict                      # PI, institution, organism, project_type, date
    known_paths: list[dict]                 # [{key, path, description}]
    resource_entities: list[dict] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Derived summary — used directly in the planner prompt
# ---------------------------------------------------------------------------


@dataclass
class AnalysisSummary:
    total_files: int
    files_by_type: dict[str, int]           # {"fastq": 12, "gtf": 1, …}
    sample_count: int
    experiment_count: int
    library_strategies: list[str]           # deduplicated, sorted
    organisms: list[str]                    # deduplicated, sorted
    is_paired_end: Optional[bool]           # True | False | None (mixed/unknown)
    has_reference_genome: bool
    files_without_samples: int              # files with no FileRun linkage
    metadata_completeness: Literal["complete", "partial", "missing"]
    suggested_analysis_type: Optional[str]  # "rna_seq" | "chip_seq" | …
    analysis_family: Optional[str]          # "transcriptomics" | "epigenomics" | …
    required_resource_roles: list[str]      # planner-facing required resource categories
    planning_hints: list[str]               # domain-specific hints for LLM planning
    potential_issues: list[str]             # human-readable notices for the planner
    resource_candidates: list[dict] = field(default_factory=list)  # lightweight retrieval hints for planner
    memory_hints: list[str] = field(default_factory=list)  # project-memory-derived hints for planner
    stable_facts: list[dict] = field(default_factory=list)  # deterministic user-confirmed facts
    semantic_hints: list[str] = field(default_factory=list)  # semantic-corpus retrieval summaries for planner
    ambiguity_hints: list[str] = field(default_factory=list)  # close-score candidate conflicts for planner


# ---------------------------------------------------------------------------
# Top-level context object passed to PlannerPromptAdapter and generate_coarse_plan
# ---------------------------------------------------------------------------


def _project_info_from_project_state(project_state: dict) -> ProjectPlannerInfo | None:
    project = dict(project_state.get("project") or {})
    project_id = str(project.get("id") or "").strip()
    if not project_id:
        return None
    return ProjectPlannerInfo(
        id=project_id,
        name=str(project.get("name") or ""),
        project_dir=str(project.get("project_dir") or ""),
        project_goal=str(project_state.get("project_brief_md") or "").strip() or None,
        project_info=dict(project.get("project_info") or {}),
        known_paths=[],
        resource_entities=[],
    )


def _samples_from_project_state(project_state: dict) -> list[SamplePlannerInfo]:
    lineage = dict(project_state.get("lineage") or {})
    linked_groups = list(lineage.get("linked_groups") or [])
    if linked_groups:
        samples: list[SamplePlannerInfo] = []
        seen: set[str] = set()
        for group in linked_groups:
            if not isinstance(group, dict):
                continue
            item = dict(group.get("sample") or {})
            sample_id = str(item.get("id") or "").strip()
            if not sample_id or sample_id in seen:
                continue
            seen.add(sample_id)
            attrs = dict(item.get("biological_context") or {})
            if item.get("description") and "description" not in attrs:
                attrs["description"] = item.get("description")
            if item.get("sample_title") and "sample_title" not in attrs:
                attrs["sample_title"] = item.get("sample_title")
            samples.append(
                SamplePlannerInfo(
                    id=sample_id,
                    sample_name=str(item.get("sample_name") or ""),
                    organism=item.get("organism"),
                    attrs=attrs,
                )
            )
        return samples
    samples: list[SamplePlannerInfo] = []
    for item in list(project_state.get("samples") or []):
        if not isinstance(item, dict):
            continue
        samples.append(
            SamplePlannerInfo(
                id=str(item.get("id") or ""),
                sample_name=str(item.get("sample_name") or ""),
                organism=item.get("organism"),
                attrs=dict(item.get("attrs") or {}),
            )
        )
    return samples


def _experiments_from_project_state(project_state: dict) -> list[ExperimentPlannerInfo]:
    lineage = dict(project_state.get("lineage") or {})
    linked_groups = list(lineage.get("linked_groups") or [])
    if linked_groups:
        experiments: list[ExperimentPlannerInfo] = []
        seen: set[str] = set()
        for group in linked_groups:
            if not isinstance(group, dict):
                continue
            item = dict(group.get("experiment") or {})
            sample = dict(group.get("sample") or {})
            experiment_id = str(item.get("id") or "").strip()
            if not experiment_id or experiment_id in seen:
                continue
            seen.add(experiment_id)
            file_ids = [
                str(file_info.get("file_id") or "")
                for file_info in list(group.get("files") or [])
                if isinstance(file_info, dict) and str(file_info.get("file_id") or "").strip()
            ]
            experiments.append(
                ExperimentPlannerInfo(
                    id=experiment_id,
                    sample_id=str(sample.get("id") or ""),
                    library_strategy=item.get("library_strategy"),
                    library_layout=item.get("library_layout"),
                    platform=item.get("platform"),
                    instrument_model=item.get("instrument_model"),
                    file_ids=file_ids,
                )
            )
        return experiments
    experiments: list[ExperimentPlannerInfo] = []
    for item in list(project_state.get("experiments") or []):
        if not isinstance(item, dict):
            continue
        file_ids = [
            str(file_id)
            for file_id in list(item.get("file_ids") or [])
            if str(file_id or "").strip()
        ]
        experiments.append(
            ExperimentPlannerInfo(
                id=str(item.get("id") or ""),
                sample_id=str(item.get("sample_id") or ""),
                library_strategy=item.get("library_strategy"),
                library_layout=item.get("library_layout"),
                platform=item.get("platform"),
                instrument_model=item.get("instrument_model"),
                file_ids=file_ids,
            )
        )
    return experiments


def _files_from_project_state(project_state: dict) -> list[FilePlannerInfo]:
    lineage = dict(project_state.get("lineage") or {})
    linked_groups = list(lineage.get("linked_groups") or [])
    if linked_groups:
        files: list[FilePlannerInfo] = []
        seen: set[str] = set()
        for group in linked_groups:
            if not isinstance(group, dict):
                continue
            sample = dict(group.get("sample") or {})
            experiment = dict(group.get("experiment") or {})
            for item in list(group.get("files") or []):
                if not isinstance(item, dict):
                    continue
                file_id = str(item.get("file_id") or "").strip()
                if not file_id or file_id in seen:
                    continue
                seen.add(file_id)
                files.append(
                    FilePlannerInfo(
                        id=file_id,
                        path=str(item.get("path") or ""),
                        filename=str(item.get("filename") or ""),
                        file_type=str(item.get("file_type") or ""),
                        read_number=item.get("read_number"),
                        linked_sample_id=str(sample.get("id") or "") or None,
                        linked_experiment_id=str(experiment.get("id") or "") or None,
                        intrinsic={},
                    )
                )
        return files
    files: list[FilePlannerInfo] = []
    for item in list(project_state.get("files") or []):
        if not isinstance(item, dict):
            continue
        files.append(
            FilePlannerInfo(
                id=str(item.get("id") or ""),
                path=str(item.get("path") or ""),
                filename=str(item.get("filename") or ""),
                file_type=str(item.get("file_type") or ""),
                read_number=item.get("read_number"),
                linked_sample_id=item.get("linked_sample_id"),
                linked_experiment_id=item.get("linked_experiment_id"),
                intrinsic=dict(item.get("intrinsic") or {}),
            )
        )
    return files


def _summary_from_project_state(project_state: dict) -> AnalysisSummary:
    payload = dict(project_state.get("summary") or {})
    return AnalysisSummary(
        total_files=int(payload.get("total_files") or 0),
        files_by_type=dict(payload.get("files_by_type") or {}),
        sample_count=int(payload.get("sample_count") or 0),
        experiment_count=int(payload.get("experiment_count") or 0),
        library_strategies=list(payload.get("library_strategies") or []),
        organisms=list(payload.get("organisms") or []),
        is_paired_end=payload.get("is_paired_end"),
        has_reference_genome=payload.get("has_reference_genome"),
        files_without_samples=payload.get("files_without_samples") or 0,
        metadata_completeness=payload.get("metadata_completeness"),
        suggested_analysis_type=payload.get("suggested_analysis_type"),
        analysis_family=payload.get("analysis_family"),
        required_resource_roles=list(payload.get("required_resource_roles") or []),
        planning_hints=list(payload.get("planning_hints") or []),
        potential_issues=list(payload.get("potential_issues") or []),
        resource_candidates=[],
        memory_hints=[],
        stable_facts=[],
        semantic_hints=[],
        ambiguity_hints=[],
    )


@dataclass
class PlannerContext:
    context_mode: Literal["project", "file_set", "global"]
    project: Optional[ProjectPlannerInfo] = None
    samples: list[SamplePlannerInfo] = field(default_factory=list)
    experiments: list[ExperimentPlannerInfo] = field(default_factory=list)
    files: list[FilePlannerInfo] = field(default_factory=list)
    file_to_sample: dict[str, str] = field(default_factory=dict)          # file_id → sample_id
    file_to_experiment: dict[str, str] = field(default_factory=dict)      # file_id → experiment_id
    summary: Optional[AnalysisSummary] = None
    project_state: dict = field(default_factory=dict)
    semantic_memory_dossier: dict = field(default_factory=dict)
    generated_at: datetime = field(default_factory=datetime.utcnow)
    # Resource readiness summary — None when ResourceGraph was not built
    resource_summary: Optional["ResourceSummary"] = None
    # Full ResourceGraph — stored for downstream use by ReadinessChecker
    resource_graph: Optional[object] = None  # ResourceGraph (avoids circular import)
    _derived_cache: dict[str, object] = field(default_factory=dict, init=False, repr=False)

    def __getattribute__(self, name: str):
        if name in {
            "project",
            "samples",
            "experiments",
            "files",
            "file_to_sample",
            "file_to_experiment",
            "summary",
        }:
            value = object.__getattribute__(self, name)
            project_state = object.__getattribute__(self, "project_state")
            if project_state and (
                value is None
                or value == []
                or value == {}
            ):
                return object.__getattribute__(self, "_derive_from_project_state")(name)
            return value
        return object.__getattribute__(self, name)

    def _derive_from_project_state(self, name: str):
        cache = self._derived_cache
        if name in cache:
            return cache[name]
        project_state = dict(self.project_state or {})
        if name == "project":
            cache[name] = _project_info_from_project_state(project_state)
        elif name == "samples":
            cache[name] = _samples_from_project_state(project_state)
        elif name == "experiments":
            cache[name] = _experiments_from_project_state(project_state)
        elif name == "files":
            cache[name] = _files_from_project_state(project_state)
        elif name == "file_to_sample":
            cache[name] = {
                file_info.id: str(file_info.linked_sample_id or "")
                for file_info in self.files
                if str(file_info.linked_sample_id or "").strip()
            }
        elif name == "file_to_experiment":
            cache[name] = {
                file_info.id: str(file_info.linked_experiment_id or "")
                for file_info in self.files
                if str(file_info.linked_experiment_id or "").strip()
            }
        elif name == "summary":
            cache[name] = _summary_from_project_state(project_state)
        else:
            cache[name] = None
        return cache[name]
