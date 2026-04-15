"""SQLAlchemy ORM models for all Tune tables."""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Optional

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from tune.core.database import Base


def _uuid() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Files & metadata
# ---------------------------------------------------------------------------


class File(Base):
    __tablename__ = "files"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    path: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    filename: Mapped[str] = mapped_column(String(512), nullable=False)
    file_type: Mapped[str] = mapped_column(String(64), nullable=False)  # e.g. "fastq"
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    md5: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    mtime: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    preview: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # first N lines
    duplicate_of: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("files.id"), nullable=True
    )
    discovered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    project_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("projects.id"), nullable=True
    )
    # pgvector embedding of concatenated metadata for semantic search
    embedding: Mapped[Optional[list[float]]] = mapped_column(Vector(1536), nullable=True)

    enhanced_metadata: Mapped[list["EnhancedMetadata"]] = relationship(
        back_populates="file", cascade="all, delete-orphan"
    )


class EnhancedMetadata(Base):
    __tablename__ = "enhanced_metadata"
    __table_args__ = (UniqueConstraint("file_id", "field_key"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    file_id: Mapped[str] = mapped_column(String(36), ForeignKey("files.id"), nullable=False)
    field_key: Mapped[str] = mapped_column(String(128), nullable=False)
    field_value: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source: Mapped[str] = mapped_column(String(32), default="inferred")  # inferred | user
    confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    file: Mapped["File"] = relationship(back_populates="enhanced_metadata")


class Project(Base):
    __tablename__ = "projects"
    __table_args__ = (UniqueConstraint("project_dir", name="uq_projects_project_dir"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(256), unique=True, nullable=False)
    project_dir: Mapped[str] = mapped_column(String(256), unique=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    dir_path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    narrative: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    inferred: Mapped[bool] = mapped_column(Boolean, default=True)
    schema_extensions: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True, default=dict)
    project_info: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True, default=dict)
    project_goal: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    samples: Mapped[list["Sample"]] = relationship(
        back_populates="project", cascade="all, delete-orphan"
    )


# ---------------------------------------------------------------------------
# SRA three-tier metadata: Sample → Experiment → FileRun
# ---------------------------------------------------------------------------


class Sample(Base):
    """BioSample — one biological specimen within a project."""
    __tablename__ = "samples"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    sample_name: Mapped[str] = mapped_column(String(256), nullable=False)
    organism: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    # All other BioSample fields stored in JSONB (tissue, treatment, sex, age, etc.)
    attrs: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    project: Mapped["Project"] = relationship(back_populates="samples")
    experiments: Mapped[list["Experiment"]] = relationship(
        back_populates="sample", cascade="all, delete-orphan"
    )


class Experiment(Base):
    """SRA Experiment — one library prep + sequencing run linked to a sample."""
    __tablename__ = "experiments"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    sample_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("samples.id", ondelete="CASCADE"), nullable=False
    )
    # SRA Experiment required fields as dedicated columns
    library_strategy: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)   # RNA-Seq, WGS, ChIP-Seq …
    library_source: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)     # TRANSCRIPTOMIC, GENOMIC …
    library_selection: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)  # cDNA, ChIP, RANDOM …
    library_layout: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)     # PAIRED | SINGLE
    platform: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)           # ILLUMINA, PACBIO_SMRT …
    instrument_model: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)  # HiSeq 6000 …
    # Remaining SRA fields + custom fields
    attrs: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    sample: Mapped["Sample"] = relationship(back_populates="experiments")
    file_runs: Mapped[list["FileRun"]] = relationship(
        back_populates="experiment", cascade="all, delete-orphan"
    )


class FileRun(Base):
    """SRA Run — links an Experiment to one or two FASTQ files (R1/R2)."""
    __tablename__ = "file_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    experiment_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("experiments.id", ondelete="CASCADE"), nullable=False
    )
    file_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("files.id", ondelete="CASCADE"), nullable=False
    )
    read_number: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # 1, 2, or null (single-end)
    filename: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)  # denormalised for fast display
    attrs: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True, default=dict)  # run_accession, run_name, spots, bases

    experiment: Mapped["Experiment"] = relationship(back_populates="file_runs")


class UserProfile(Base):
    """Single global row (id=1) tracking inferred researcher characteristics."""

    __tablename__ = "user_profiles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    research_domain: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    experience_level: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)  # novice|intermediate|expert
    language_preference: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)  # en|zh
    communication_style: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)  # brief|detailed
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


# ---------------------------------------------------------------------------
# Conversation threads
# ---------------------------------------------------------------------------


class Thread(Base):
    __tablename__ = "threads"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("projects.id", ondelete="SET NULL"), nullable=True
    )
    title: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    messages: Mapped[list["ThreadMessage"]] = relationship(
        back_populates="thread", cascade="all, delete-orphan", order_by="ThreadMessage.created_at"
    )
    analysis_jobs: Mapped[list["AnalysisJob"]] = relationship(back_populates="thread")


class ThreadMessage(Base):
    __tablename__ = "thread_messages"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    thread_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("threads.id", ondelete="CASCADE"), nullable=False
    )
    role: Mapped[str] = mapped_column(String(16), nullable=False)  # user | assistant
    content: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    thread: Mapped["Thread"] = relationship(back_populates="messages")


# ---------------------------------------------------------------------------
# Session continuity & authoritative analysis revisions
# ---------------------------------------------------------------------------


class SessionState(Base):
    __tablename__ = "session_states"
    __table_args__ = (UniqueConstraint("thread_id", name="uq_session_states_thread_id"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    thread_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("threads.id", ondelete="CASCADE"), nullable=False, index=True
    )
    project_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("projects.id", ondelete="SET NULL"), nullable=True, index=True
    )
    current_focus: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    current_stage: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    last_action: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    progress_state_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    latest_readiness_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    latest_context_acquisition_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    analysis_intent_trace_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    pending_decision_packet_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    pending_clarification_request_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    pending_analysis_plan_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    active_intent_revision_id: Mapped[Optional[str]] = mapped_column(
        String(36),
        ForeignKey("analysis_intent_revisions.id", ondelete="SET NULL"),
        nullable=True,
    )
    active_capability_plan_revision_id: Mapped[Optional[str]] = mapped_column(
        String(36),
        ForeignKey("capability_plan_revisions.id", ondelete="SET NULL"),
        nullable=True,
    )
    event_trail_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True, default=list)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class AnalysisIntentRevision(Base):
    __tablename__ = "analysis_intent_revisions"
    __table_args__ = (
        UniqueConstraint("thread_id", "revision_index", name="uq_intent_revision_thread_index"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    thread_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("threads.id", ondelete="CASCADE"), nullable=False, index=True
    )
    project_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("projects.id", ondelete="SET NULL"), nullable=True, index=True
    )
    revision_index: Mapped[int] = mapped_column(Integer, nullable=False)
    user_goal: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stage: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    intent_json: Mapped[Any] = mapped_column(JSONB, nullable=False)
    readiness_assessment_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    context_acquisition_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    trace_snapshot_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class CapabilityPlanRevision(Base):
    __tablename__ = "capability_plan_revisions"
    __table_args__ = (
        UniqueConstraint(
            "thread_id",
            "revision_index",
            name="uq_capability_plan_revision_thread_index",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    thread_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("threads.id", ondelete="CASCADE"), nullable=False, index=True
    )
    project_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("projects.id", ondelete="SET NULL"), nullable=True, index=True
    )
    intent_revision_id: Mapped[Optional[str]] = mapped_column(
        String(36),
        ForeignKey("analysis_intent_revisions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    revision_index: Mapped[int] = mapped_column(Integer, nullable=False)
    user_goal: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    capability_plan_json: Mapped[Any] = mapped_column(JSONB, nullable=False)
    implementation_decisions_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    decision_packet_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


# ---------------------------------------------------------------------------
# Analysis jobs
# ---------------------------------------------------------------------------


class AnalysisJob(Base):
    __tablename__ = "analysis_jobs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    thread_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("threads.id", ondelete="SET NULL"), nullable=True, index=True
    )
    project_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("projects.id"), nullable=True
    )
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    status: Mapped[str] = mapped_column(
        String(32), default="queued"
    )  # queued|running|completed|failed|cancelled|interrupted
    goal: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    plan: Mapped[Optional[Any]] = mapped_column(JSON, nullable=True)  # coarse pipeline
    output_dir: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    procrastinate_job_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_progress_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    peak_cpu_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    peak_mem_mb: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    language: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)
    # Pipeline-v2 additions
    current_step_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    plan_draft_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    resolved_plan_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    execution_ir_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    expanded_dag_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    binding_status: Mapped[Optional[str]] = mapped_column(String(32), nullable=True, default="not_started")
    env_status: Mapped[Optional[str]] = mapped_column(String(32), nullable=True, default="pending")
    env_spec_hash: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    session_state_id: Mapped[Optional[str]] = mapped_column(
        String(36),
        ForeignKey("session_states.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    intent_revision_id: Mapped[Optional[str]] = mapped_column(
        String(36),
        ForeignKey("analysis_intent_revisions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    capability_plan_revision_id: Mapped[Optional[str]] = mapped_column(
        String(36),
        ForeignKey("capability_plan_revisions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    # Phase 1 persistent state machine: track pending auth/repair request for DB-poll resume
    pending_auth_request_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    pending_repair_request_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    pending_step_key: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    pending_interaction_type: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    pending_interaction_payload_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    # Resource graph JSON snapshot (serialized ResourceGraph for this job)
    resource_graph_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    thread: Mapped[Optional["Thread"]] = relationship(back_populates="analysis_jobs")


class JobLog(Base):
    __tablename__ = "job_logs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    job_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("analysis_jobs.id"), nullable=False
    )
    stream: Mapped[str] = mapped_column(String(8), default="stdout")  # stdout | stderr
    line: Mapped[str] = mapped_column(Text, nullable=False)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# ---------------------------------------------------------------------------
# Skills
# ---------------------------------------------------------------------------


class Skill(Base):
    __tablename__ = "skills"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(256), unique=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    skill_type: Mapped[str] = mapped_column(String(32), default="analysis")
    current_version: Mapped[str] = mapped_column(String(16), default="1.0")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    source_job_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("analysis_jobs.id"), nullable=True
    )

    versions: Mapped[list["SkillVersion"]] = relationship(
        back_populates="skill", cascade="all, delete-orphan", order_by="SkillVersion.version"
    )


class SkillVersion(Base):
    __tablename__ = "skill_versions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    skill_id: Mapped[str] = mapped_column(String(36), ForeignKey("skills.id"), nullable=False)
    version: Mapped[str] = mapped_column(String(16), nullable=False)
    input_params: Mapped[Any] = mapped_column(JSON, nullable=False, default=list)
    steps: Mapped[Any] = mapped_column(JSON, nullable=False, default=list)
    pixi_toml: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    pixi_lock: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    tags: Mapped[Any] = mapped_column(JSON, nullable=False, default=list)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    skill: Mapped["Skill"] = relationship(back_populates="versions")


# ---------------------------------------------------------------------------
# Conversations
# ---------------------------------------------------------------------------


class Conversation(Base):
    __tablename__ = "conversations"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("projects.id"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    messages: Mapped[list["Message"]] = relationship(
        back_populates="conversation", cascade="all, delete-orphan", order_by="Message.created_at"
    )


class Message(Base):
    __tablename__ = "messages"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    conversation_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("conversations.id"), nullable=False
    )
    role: Mapped[str] = mapped_column(String(16), nullable=False)  # user | assistant | system
    content: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    conversation: Mapped["Conversation"] = relationship(back_populates="messages")


# ---------------------------------------------------------------------------
# Analysis environment memory
# ---------------------------------------------------------------------------


class InstalledPackage(Base):
    __tablename__ = "installed_packages"
    __table_args__ = (UniqueConstraint("project_id", "package_name"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    package_name: Mapped[str] = mapped_column(String(256), nullable=False)
    installed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class KnownPath(Base):
    __tablename__ = "known_paths"
    __table_args__ = (UniqueConstraint("project_id", "key"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    key: Mapped[str] = mapped_column(String(128), nullable=False)
    path: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


# ---------------------------------------------------------------------------
# Resource entities — canonical logical bioinformatics resources
# ---------------------------------------------------------------------------


class ResourceEntity(Base):
    __tablename__ = "resource_entities"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False, index=True
    )
    resource_role: Mapped[str] = mapped_column(String(64), nullable=False)
    display_name: Mapped[str] = mapped_column(String(256), nullable=False)
    organism: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    genome_build: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    version_label: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    source_type: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    source_uri: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    metadata_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    resource_files: Mapped[list["ResourceFile"]] = relationship(
        back_populates="resource_entity", cascade="all, delete-orphan"
    )


class ResourceFile(Base):
    __tablename__ = "resource_files"
    __table_args__ = (UniqueConstraint("resource_entity_id", "file_id", "file_role"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    resource_entity_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("resource_entities.id", ondelete="CASCADE"), nullable=False, index=True
    )
    file_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("files.id", ondelete="CASCADE"), nullable=False, index=True
    )
    file_role: Mapped[str] = mapped_column(String(64), nullable=False)
    is_primary: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    metadata_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True, default=dict)

    resource_entity: Mapped["ResourceEntity"] = relationship(back_populates="resource_files")
    file: Mapped["File"] = relationship()


class ResourceDerivation(Base):
    __tablename__ = "resource_derivations"
    __table_args__ = (
        UniqueConstraint("parent_resource_id", "child_resource_id", "derivation_type"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    parent_resource_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("resource_entities.id", ondelete="CASCADE"), nullable=False, index=True
    )
    child_resource_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("resource_entities.id", ondelete="CASCADE"), nullable=False, index=True
    )
    derivation_type: Mapped[str] = mapped_column(String(128), nullable=False)
    tool_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    tool_version: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    params_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True, default=dict)
    created_by_job_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("analysis_jobs.id", ondelete="SET NULL"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


# ---------------------------------------------------------------------------
# Derived resource cache (aligner indices, STAR genomes, etc.)
# ---------------------------------------------------------------------------


class DerivedResource(Base):
    """Persistent cache of derived bioinformatics resources (indices, genomes).

    Replaces KnownPath entries for hisat2_index, star_genome_dir, bwa_index,
    bowtie2_index.  Tracks provenance and mtime for staleness detection.
    """

    __tablename__ = "derived_resources"
    __table_args__ = (
        UniqueConstraint("project_id", "kind", "aligner", name="uq_derived_resources"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    kind: Mapped[str] = mapped_column(String(64), nullable=False)         # aligner_index | derived_index
    aligner: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)  # hisat2 | star | bwa | bowtie2
    organism: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    genome_build: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    path: Mapped[str] = mapped_column(Text, nullable=False)               # index prefix / genome dir
    derived_from_path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    derived_from_mtime: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


# ---------------------------------------------------------------------------
# Scan state (checkpoint)
# ---------------------------------------------------------------------------


class ScanState(Base):
    __tablename__ = "scan_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    total_discovered: Mapped[int] = mapped_column(Integer, default=0)
    total_processed: Mapped[int] = mapped_column(Integer, default=0)
    last_scanned_path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="idle")  # idle|running|syncing_resources|complete
    resource_sync_status: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    resource_sync_summary_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True, default=dict)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


# ---------------------------------------------------------------------------
# LLM request logs
# ---------------------------------------------------------------------------


class LLMLog(Base):
    __tablename__ = "llm_logs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    model: Mapped[str] = mapped_column(String(128), nullable=False)
    input_tokens: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    output_tokens: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    latency_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    success: Mapped[bool] = mapped_column(Boolean, default=True)
    error_type: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# ---------------------------------------------------------------------------
# Error recovery memory
# ---------------------------------------------------------------------------


class GlobalMemory(Base):
    """Reusable error recovery knowledge — system-seeded and user-taught."""

    __tablename__ = "global_memories"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    trigger_condition: Mapped[str] = mapped_column(Text, nullable=False)
    approach: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(String(8), nullable=False, default="system")  # system | user
    embedding: Mapped[Optional[list[float]]] = mapped_column(Vector(1536), nullable=True)
    success_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class MetadataProposal(Base):
    """Staged metadata change proposal — LLM output before any DB write."""

    __tablename__ = "metadata_proposals"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    task_type: Mapped[str] = mapped_column(String(64), nullable=False)
    # running | pending | applied | discarded | failed
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="running")
    instruction: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    payload: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    applied_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


# ---------------------------------------------------------------------------
# Analysis pipeline v2 — step runs, authorization, repair, decisions
# ---------------------------------------------------------------------------


class AnalysisStepRun(Base):
    """One executed step within an AnalysisJob."""

    __tablename__ = "analysis_step_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    job_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("analysis_jobs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    step_key: Mapped[str] = mapped_column(String(128), nullable=False)
    step_type: Mapped[str] = mapped_column(String(128), nullable=False)
    display_name: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    # pending|ready|binding_missing|awaiting_authorization|running|
    # repairable_failed|waiting_for_human_repair|succeeded|failed|skipped
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    depends_on: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    params_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    bindings_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    outputs_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    renderer_version: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


class CommandAuthorizationRequest(Base):
    """DB-backed command authorization — replaces asyncio.Event."""

    __tablename__ = "command_authorization_requests"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    job_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("analysis_jobs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    step_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("analysis_step_runs.id", ondelete="SET NULL"), nullable=True
    )
    command_text: Mapped[str] = mapped_column(Text, nullable=False)
    current_command_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    command_fingerprint: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    command_template_type: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    revision_history_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    # pending|approved|rejected|bypassed
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    requested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    @property
    def effective_command(self) -> str:
        return self.current_command_text or self.command_text


class RepairRequest(Base):
    """DB-backed error recovery request — replaces asyncio.Event human recovery."""

    __tablename__ = "repair_requests"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    job_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("analysis_jobs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    step_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("analysis_step_runs.id", ondelete="SET NULL"), nullable=True
    )
    failed_command: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stderr_excerpt: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    repair_level: Mapped[int] = mapped_column(Integer, nullable=False)  # 1, 2, or 3
    # pending|resolved|cancelled
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    suggestion_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    # Phase 1: user's resolution payload (command, should_continue) written by ws handler
    human_resolution_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)


class UserDecision(Base):
    """Audit log of every user-driven state change for a job."""

    __tablename__ = "user_decisions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    job_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("analysis_jobs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    step_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("analysis_step_runs.id", ondelete="SET NULL"), nullable=True
    )
    # plan_confirmed|plan_modified|authorization_approved|authorization_rejected|
    # repair_choice|job_cancelled
    decision_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class InputBinding(Base):
    """Explicit slot-to-file binding for a step in a job."""

    __tablename__ = "input_bindings"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    job_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("analysis_jobs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    step_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("analysis_step_runs.id", ondelete="CASCADE"), nullable=False
    )
    slot_name: Mapped[str] = mapped_column(String(128), nullable=False)
    # project_file | step_output | known_path | user_provided
    source_type: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    source_ref: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    resolved_path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    match_metadata_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    # resolved | missing | invalid
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="missing")


class ProjectExecutionEvent(Base):
    """Per-project record of resolved step errors and how they were fixed."""

    __tablename__ = "project_execution_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)  # error_resolved | step_completed
    description: Mapped[str] = mapped_column(Text, nullable=False)
    resolution: Mapped[str] = mapped_column(Text, nullable=False)
    user_contributed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    embedding: Mapped[Optional[list[float]]] = mapped_column(Vector(1536), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class MemoryFact(Base):
    """Stable project-scoped fact memory persisted separately from raw events."""

    __tablename__ = "memory_facts"
    __table_args__ = (UniqueConstraint("project_id", "fact_key", name="uq_memory_facts_project_fact_key"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    fact_key: Mapped[str] = mapped_column(String(255), nullable=False)
    fact_type: Mapped[str] = mapped_column(String(64), nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    statement: Mapped[str] = mapped_column(Text, nullable=False)
    payload_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    source_event_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("project_execution_events.id", ondelete="SET NULL"), nullable=True
    )
    source_episode_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("memory_episodes.id", ondelete="SET NULL"), nullable=True
    )
    confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class MemoryEpisode(Base):
    """Project-scoped reusable execution episode derived from runtime events."""

    __tablename__ = "memory_episodes"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    source_event_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("project_execution_events.id", ondelete="SET NULL"), nullable=True
    )
    thread_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("threads.id", ondelete="SET NULL"), nullable=True
    )
    job_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("analysis_jobs.id", ondelete="SET NULL"), nullable=True
    )
    step_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("analysis_step_runs.id", ondelete="SET NULL"), nullable=True
    )
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    resolution: Mapped[str] = mapped_column(Text, nullable=False)
    user_contributed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    metadata_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    embedding: Mapped[Optional[list[float]]] = mapped_column(Vector(1536), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class MemoryPattern(Base):
    """Persisted structured project memory patterns derived from recent episodes."""

    __tablename__ = "memory_patterns"
    __table_args__ = (UniqueConstraint("project_id", "pattern_key", name="uq_memory_patterns_project_pattern_key"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    pattern_key: Mapped[str] = mapped_column(String(255), nullable=False)
    pattern_type: Mapped[str] = mapped_column(String(64), nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    recommended_value: Mapped[str] = mapped_column(Text, nullable=False)
    support_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    user_validated_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    confidence: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    payload_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    source_episode_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("memory_episodes.id", ondelete="SET NULL"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class MemoryPreference(Base):
    """Persisted structured project memory preferences derived from recent episodes."""

    __tablename__ = "memory_preferences"
    __table_args__ = (UniqueConstraint("project_id", "preference_key", name="uq_memory_preferences_project_preference_key"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    preference_key: Mapped[str] = mapped_column(String(255), nullable=False)
    preference_type: Mapped[str] = mapped_column(String(64), nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    basis: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    support_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    confidence: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    payload_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    source_episode_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("memory_episodes.id", ondelete="SET NULL"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class MemoryLink(Base):
    """Typed relationship edge between memory records and project/runtime entities."""

    __tablename__ = "memory_links"
    __table_args__ = (
        UniqueConstraint(
            "project_id",
            "memory_type",
            "memory_id",
            "entity_type",
            "entity_id",
            "link_role",
            name="uq_memory_links_edge",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    project_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    memory_type: Mapped[str] = mapped_column(String(64), nullable=False)
    memory_id: Mapped[str] = mapped_column(String(36), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    entity_id: Mapped[str] = mapped_column(String(255), nullable=False)
    link_role: Mapped[str] = mapped_column(String(64), nullable=False)
    strength: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    last_confirmed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


# ---------------------------------------------------------------------------
# Phase 4: ArtifactRecord — tracks step outputs for deterministic downstream binding
# ---------------------------------------------------------------------------


class ArtifactRecord(Base):
    """Output file produced by a completed analysis step.

    Written by tasks.py after each step succeeds.  The binding resolver
    queries this table (Tier 1a) before falling back to BFS directory scan
    (Tier 1b), giving deterministic, typed binding for downstream steps.
    """

    __tablename__ = "artifact_records"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    job_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("analysis_jobs.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    step_key: Mapped[str] = mapped_column(String(128), nullable=False)
    step_type: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    step_run_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("analysis_step_runs.id", ondelete="SET NULL"), nullable=True
    )
    # Output slot name from StepTypeDefinition (e.g. "sam", "sorted_bam", "counts_matrix")
    slot_name: Mapped[str] = mapped_column(String(128), nullable=False)
    # File extension / logical type (e.g. "sam", "bam", "txt", "html")
    artifact_type: Mapped[str] = mapped_column(String(64), nullable=False)
    artifact_role: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    artifact_scope: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    file_path: Mapped[str] = mapped_column(Text, nullable=False)
    sample_name: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    size_bytes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    # Extra context: renderer_version, command_fingerprint, etc.
    metadata_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


# ---------------------------------------------------------------------------
# Phase 6: RepairMemory — long-term human repair knowledge
# ---------------------------------------------------------------------------


class RepairMemory(Base):
    """Stores successful human repair patterns for future automatic reuse.

    Written by tasks.py after a human-provided repair command succeeds.
    Queried by the repair engine (Tier 0) before Level-1 rules.

    Matching: error_signature is a deterministic hash of (step_type, stderr
    keywords) that identifies the same error class regardless of exact paths
    or numbers. When a Tier-0 hit is found, the stored fix is applied to the
    current command and validated by _is_safe_repair before executing.
    """

    __tablename__ = "repair_memories"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    step_type: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    tool_name: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    error_signature: Mapped[str] = mapped_column(String(16), nullable=False)
    context_fingerprint: Mapped[str] = mapped_column(String(16), nullable=False, default="")
    # {repair_command, original_command, action}
    human_solution_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    # reduce_threads | reduce_memory | fix_path | add_flag | custom
    normalized_strategy: Mapped[str] = mapped_column(String(32), nullable=False, default="custom")
    # "global" = applies everywhere; "project" = project-specific
    scope_type: Mapped[str] = mapped_column(String(16), nullable=False, default="global")
    project_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("projects.id", ondelete="SET NULL"), nullable=True
    )
    success_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failure_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


# ---------------------------------------------------------------------------
# Pipeline-v2: Skill template extraction
# ---------------------------------------------------------------------------


class SkillTemplate(Base):
    """Reusable parameterized pipeline template extracted from a completed job."""

    __tablename__ = "skill_templates"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    # List of step_type strings (e.g. ["qc.fastqc", "align.hisat2"])
    step_types: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=False, default=list)
    # Abstract plan with slot references ({{slot_name}}) instead of absolute paths
    plan_schema: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    # EnvSpec snapshot: {packages: [...], hash: "..."}
    env_spec: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    source_job_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("analysis_jobs.id", ondelete="SET NULL"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class SkillVersionSnapshot(Base):
    """Snapshot of the exact execution environment and plan for a SkillTemplate (pipeline-v2)."""

    __tablename__ = "skill_version_snapshots"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    template_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("skill_templates.id", ondelete="CASCADE"),
        nullable=False, index=True
    )
    version_number: Mapped[int] = mapped_column(nullable=False, default=1)
    plan_json: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    pixi_toml: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    pixi_lock: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    # {step_key: renderer_version} from AnalysisStepRun records
    renderer_versions: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    source_job_id: Mapped[Optional[str]] = mapped_column(
        String(36), ForeignKey("analysis_jobs.id", ondelete="SET NULL"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
