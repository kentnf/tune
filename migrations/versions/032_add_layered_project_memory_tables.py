"""Add memory_facts and memory_episodes tables.

Revision ID: 032
Revises: 031
Create Date: 2026-03-29 21:00:00
"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from pgvector.sqlalchemy import Vector

revision: str = "032"
down_revision: Union[str, None] = "031"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "memory_episodes",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "project_id",
            sa.String(36),
            sa.ForeignKey("projects.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "source_event_id",
            sa.String(36),
            sa.ForeignKey("project_execution_events.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "thread_id",
            sa.String(36),
            sa.ForeignKey("threads.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "job_id",
            sa.String(36),
            sa.ForeignKey("analysis_jobs.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "step_id",
            sa.String(36),
            sa.ForeignKey("analysis_step_runs.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("description", sa.Text, nullable=False),
        sa.Column("resolution", sa.Text, nullable=False),
        sa.Column("user_contributed", sa.Boolean, nullable=False, server_default=sa.text("false")),
        sa.Column("metadata_json", sa.dialects.postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("embedding", Vector(1536), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_memory_episodes_project_id", "memory_episodes", ["project_id"])
    op.create_index(
        "ix_memory_episodes_embedding",
        "memory_episodes",
        ["embedding"],
        postgresql_using="ivfflat",
        postgresql_ops={"embedding": "vector_cosine_ops"},
    )

    op.create_table(
        "memory_facts",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "project_id",
            sa.String(36),
            sa.ForeignKey("projects.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("fact_key", sa.String(255), nullable=False),
        sa.Column("fact_type", sa.String(64), nullable=False),
        sa.Column("title", sa.Text, nullable=False),
        sa.Column("statement", sa.Text, nullable=False),
        sa.Column("payload_json", sa.dialects.postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "source_event_id",
            sa.String(36),
            sa.ForeignKey("project_execution_events.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "source_episode_id",
            sa.String(36),
            sa.ForeignKey("memory_episodes.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("project_id", "fact_key", name="uq_memory_facts_project_fact_key"),
    )
    op.create_index("ix_memory_facts_project_id", "memory_facts", ["project_id"])
    op.create_index("ix_memory_facts_fact_type", "memory_facts", ["fact_type"])


def downgrade() -> None:
    op.drop_table("memory_facts")
    op.drop_table("memory_episodes")
