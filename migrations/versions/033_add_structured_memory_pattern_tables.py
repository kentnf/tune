"""Add memory_patterns and memory_preferences tables.

Revision ID: 033
Revises: 032
Create Date: 2026-03-29 23:20:00
"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "033"
down_revision: Union[str, None] = "032"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "memory_patterns",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "project_id",
            sa.String(36),
            sa.ForeignKey("projects.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("pattern_key", sa.String(255), nullable=False),
        sa.Column("pattern_type", sa.String(64), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("recommended_value", sa.Text(), nullable=False),
        sa.Column("support_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("user_validated_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("confidence", sa.String(16), nullable=True),
        sa.Column("payload_json", sa.dialects.postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "source_episode_id",
            sa.String(36),
            sa.ForeignKey("memory_episodes.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("project_id", "pattern_key", name="uq_memory_patterns_project_pattern_key"),
    )
    op.create_index("ix_memory_patterns_project_id", "memory_patterns", ["project_id"])
    op.create_index("ix_memory_patterns_pattern_type", "memory_patterns", ["pattern_type"])

    op.create_table(
        "memory_preferences",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "project_id",
            sa.String(36),
            sa.ForeignKey("projects.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("preference_key", sa.String(255), nullable=False),
        sa.Column("preference_type", sa.String(64), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column("basis", sa.String(64), nullable=True),
        sa.Column("support_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("confidence", sa.String(16), nullable=True),
        sa.Column("payload_json", sa.dialects.postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "source_episode_id",
            sa.String(36),
            sa.ForeignKey("memory_episodes.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("project_id", "preference_key", name="uq_memory_preferences_project_preference_key"),
    )
    op.create_index("ix_memory_preferences_project_id", "memory_preferences", ["project_id"])
    op.create_index("ix_memory_preferences_preference_type", "memory_preferences", ["preference_type"])


def downgrade() -> None:
    op.drop_table("memory_preferences")
    op.drop_table("memory_patterns")
