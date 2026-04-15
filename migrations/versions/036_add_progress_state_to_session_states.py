"""Add progress_state_json to session_states.

Revision ID: 036
Revises: 035
Create Date: 2026-03-31 23:10:00
"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "036"
down_revision: Union[str, None] = "035"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "session_states",
        sa.Column("progress_state_json", JSONB, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("session_states", "progress_state_json")
