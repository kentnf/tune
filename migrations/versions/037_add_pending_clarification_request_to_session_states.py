"""Add pending_clarification_request_json to session_states.

Revision ID: 037
Revises: 036
Create Date: 2026-04-01 00:00:00
"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "037"
down_revision: Union[str, None] = "036"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "session_states",
        sa.Column("pending_clarification_request_json", JSONB, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("session_states", "pending_clarification_request_json")
