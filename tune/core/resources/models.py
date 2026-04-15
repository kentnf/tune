"""Data models for resource readiness: ResourceGraph, ResourceNode, ReadinessIssue."""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, Optional

# ---------------------------------------------------------------------------
# Enums / Literal types
# ---------------------------------------------------------------------------

ResourceKind = Literal[
    "reads",
    "reference_fasta",
    "annotation_gtf",
    "aligner_index",
    "artifact",
    "derived_index",
    "other",
]

ResourceStatus = Literal[
    "ready",
    "derivable",
    "missing",
    "stale",
    "ambiguous",
]

ResourceSourceType = Literal[
    "filerun_db",
    "resource_entity",
    "known_path",
    "artifact_record",
    "enhanced_metadata",
    "project_file_scan",
    "user_confirmed",
    "auto_derived",
]

EdgeRelation = Literal["derived_from", "requires", "produces"]


# ---------------------------------------------------------------------------
# Section 1: ResourceGraph data models
# ---------------------------------------------------------------------------


@dataclass
class ResourceCandidate:
    """A ranked candidate path for an ambiguous resource."""

    path: str
    resource_entity_id: Optional[str] = None
    file_id: Optional[str] = None
    linked_file_ids: list[str] = field(default_factory=list)
    organism: Optional[str] = None
    genome_build: Optional[str] = None
    source_type: Optional[ResourceSourceType] = None
    confidence: float = 0.5


@dataclass
class ResourceNode:
    """A single bioinformatics resource with semantic status."""

    id: str
    kind: ResourceKind
    status: ResourceStatus
    label: str
    resource_entity_id: Optional[str] = None
    resolved_path: Optional[str] = None
    candidates: list[ResourceCandidate] = field(default_factory=list)
    organism: Optional[str] = None
    genome_build: Optional[str] = None
    linked_file_ids: list[str] = field(default_factory=list)
    source_type: Optional[ResourceSourceType] = None
    derived_from_ids: list[str] = field(default_factory=list)
    derive_command: Optional[str] = None
    size_bytes: Optional[int] = None
    created_at: Optional[datetime] = None


@dataclass
class ReadGroup:
    """Links a sample/experiment to its paired read resources."""

    sample_id: str
    sample_name: str
    experiment_id: str
    library_strategy: Optional[str]
    library_layout: Optional[str]
    read1_resource_id: Optional[str] = None
    read2_resource_id: Optional[str] = None


@dataclass
class ResourceEdge:
    """A directed dependency edge between two resource nodes."""

    from_id: str
    to_id: str
    relation: EdgeRelation


@dataclass
class ResourceGraph:
    """Complete resource dependency graph for a project analysis."""

    nodes: dict[str, ResourceNode] = field(default_factory=dict)
    edges: list[ResourceEdge] = field(default_factory=list)
    by_kind: dict[str, list[str]] = field(default_factory=dict)  # kind → [node_id, …]
    read_groups: list[ReadGroup] = field(default_factory=list)


@dataclass
class ResourceSummary:
    """Compressed resource status for inclusion in PlannerContext."""

    reads_ready: bool
    reference_status: ResourceStatus
    annotation_status: ResourceStatus
    index_status: ResourceStatus
    prepare_steps_needed: list[str] = field(default_factory=list)  # e.g. ["hisat2_build"]


# ---------------------------------------------------------------------------
# Section 2: ReadinessIssue + ReadinessReport
# ---------------------------------------------------------------------------

IssueKind = Literal[
    "missing_reference",
    "missing_annotation",
    "missing_index",
    "missing_reads",
    "unbound_reads",
    "ambiguous_reference",
    "ambiguous_annotation",
    "ambiguous_index",
    "stale_index",
    "organism_mismatch",
    "incomplete_metadata",
    "missing_experiment_link",
    "missing_input_slot",
    "render_error",
    "missing_concrete_path",
    "unknown_step_type",
]

IssueSeverity = Literal["blocking", "warning"]

ResolutionType = Literal[
    "select_candidate",
    "provide_path",
    "confirm_auto_build",
    "link_experiment",
    "provide_metadata",
]


@dataclass
class ReadinessIssue:
    """A semantic resource readiness issue suitable for user dialogue."""

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    kind: IssueKind = "missing_reference"
    severity: IssueSeverity = "blocking"
    title: str = ""
    description: str = ""
    suggestion: str = ""
    affected_resource_ids: list[str] = field(default_factory=list)
    affected_step_keys: list[str] = field(default_factory=list)
    resolution_type: Optional[ResolutionType] = None
    candidates: list[ResourceCandidate] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class ReadinessReport:
    """Result of ReadinessChecker.check() — ok=True means job can proceed."""

    ok: bool
    issues: list[ReadinessIssue] = field(default_factory=list)    # blocking
    warnings: list[ReadinessIssue] = field(default_factory=list)  # non-blocking
    resource_graph: Optional[ResourceGraph] = None
