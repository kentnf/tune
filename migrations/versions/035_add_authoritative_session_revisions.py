"""Add session state and authoritative analysis revision tables.

Revision ID: 035
Revises: 034
Create Date: 2026-03-31 21:30:00
"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "035"
down_revision: Union[str, None] = "034"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "analysis_intent_revisions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "thread_id",
            sa.String(36),
            sa.ForeignKey("threads.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "project_id",
            sa.String(36),
            sa.ForeignKey("projects.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("revision_index", sa.Integer(), nullable=False),
        sa.Column("user_goal", sa.Text(), nullable=True),
        sa.Column("stage", sa.String(32), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("intent_json", JSONB, nullable=False),
        sa.Column("readiness_assessment_json", JSONB, nullable=True),
        sa.Column("context_acquisition_json", JSONB, nullable=True),
        sa.Column("trace_snapshot_json", JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint(
            "thread_id",
            "revision_index",
            name="uq_intent_revision_thread_index",
        ),
    )
    op.create_index(
        "ix_analysis_intent_revisions_thread_id",
        "analysis_intent_revisions",
        ["thread_id"],
    )
    op.create_index(
        "ix_analysis_intent_revisions_project_id",
        "analysis_intent_revisions",
        ["project_id"],
    )

    op.create_table(
        "capability_plan_revisions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "thread_id",
            sa.String(36),
            sa.ForeignKey("threads.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "project_id",
            sa.String(36),
            sa.ForeignKey("projects.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "intent_revision_id",
            sa.String(36),
            sa.ForeignKey("analysis_intent_revisions.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("revision_index", sa.Integer(), nullable=False),
        sa.Column("user_goal", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("capability_plan_json", JSONB, nullable=False),
        sa.Column("implementation_decisions_json", JSONB, nullable=True),
        sa.Column("decision_packet_json", JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint(
            "thread_id",
            "revision_index",
            name="uq_capability_plan_revision_thread_index",
        ),
    )
    op.create_index(
        "ix_capability_plan_revisions_thread_id",
        "capability_plan_revisions",
        ["thread_id"],
    )
    op.create_index(
        "ix_capability_plan_revisions_project_id",
        "capability_plan_revisions",
        ["project_id"],
    )
    op.create_index(
        "ix_capability_plan_revisions_intent_revision_id",
        "capability_plan_revisions",
        ["intent_revision_id"],
    )

    op.create_table(
        "session_states",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "thread_id",
            sa.String(36),
            sa.ForeignKey("threads.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "project_id",
            sa.String(36),
            sa.ForeignKey("projects.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("current_focus", sa.Text(), nullable=True),
        sa.Column("current_stage", sa.String(64), nullable=True),
        sa.Column("last_action", sa.Text(), nullable=True),
        sa.Column("latest_readiness_json", JSONB, nullable=True),
        sa.Column("latest_context_acquisition_json", JSONB, nullable=True),
        sa.Column("analysis_intent_trace_json", JSONB, nullable=True),
        sa.Column("pending_decision_packet_json", JSONB, nullable=True),
        sa.Column("pending_analysis_plan_json", JSONB, nullable=True),
        sa.Column(
            "active_intent_revision_id",
            sa.String(36),
            sa.ForeignKey("analysis_intent_revisions.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "active_capability_plan_revision_id",
            sa.String(36),
            sa.ForeignKey("capability_plan_revisions.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("event_trail_json", JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("thread_id", name="uq_session_states_thread_id"),
    )
    op.create_index("ix_session_states_thread_id", "session_states", ["thread_id"])
    op.create_index("ix_session_states_project_id", "session_states", ["project_id"])

    op.add_column(
        "analysis_jobs",
        sa.Column(
            "session_state_id",
            sa.String(36),
            sa.ForeignKey("session_states.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.add_column(
        "analysis_jobs",
        sa.Column(
            "intent_revision_id",
            sa.String(36),
            sa.ForeignKey("analysis_intent_revisions.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.add_column(
        "analysis_jobs",
        sa.Column(
            "capability_plan_revision_id",
            sa.String(36),
            sa.ForeignKey("capability_plan_revisions.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.create_index("ix_analysis_jobs_session_state_id", "analysis_jobs", ["session_state_id"])
    op.create_index("ix_analysis_jobs_intent_revision_id", "analysis_jobs", ["intent_revision_id"])
    op.create_index(
        "ix_analysis_jobs_capability_plan_revision_id",
        "analysis_jobs",
        ["capability_plan_revision_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_analysis_jobs_capability_plan_revision_id", table_name="analysis_jobs")
    op.drop_index("ix_analysis_jobs_intent_revision_id", table_name="analysis_jobs")
    op.drop_index("ix_analysis_jobs_session_state_id", table_name="analysis_jobs")
    op.drop_column("analysis_jobs", "capability_plan_revision_id")
    op.drop_column("analysis_jobs", "intent_revision_id")
    op.drop_column("analysis_jobs", "session_state_id")
    op.drop_table("session_states")
    op.drop_table("capability_plan_revisions")
    op.drop_table("analysis_intent_revisions")
