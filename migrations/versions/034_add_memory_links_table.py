"""Add memory_links table.

Revision ID: 034
Revises: 033
Create Date: 2026-03-29 23:55:00
"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "034"
down_revision: Union[str, None] = "033"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "memory_links",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "project_id",
            sa.String(36),
            sa.ForeignKey("projects.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("memory_type", sa.String(64), nullable=False),
        sa.Column("memory_id", sa.String(36), nullable=False),
        sa.Column("entity_type", sa.String(64), nullable=False),
        sa.Column("entity_id", sa.String(255), nullable=False),
        sa.Column("link_role", sa.String(64), nullable=False),
        sa.Column("strength", sa.Float(), nullable=True),
        sa.Column("last_confirmed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint(
            "project_id",
            "memory_type",
            "memory_id",
            "entity_type",
            "entity_id",
            "link_role",
            name="uq_memory_links_edge",
        ),
    )
    op.create_index("ix_memory_links_project_id", "memory_links", ["project_id"])
    op.create_index("ix_memory_links_memory", "memory_links", ["memory_type", "memory_id"])
    op.create_index("ix_memory_links_entity", "memory_links", ["entity_type", "entity_id"])


def downgrade() -> None:
    op.drop_table("memory_links")
