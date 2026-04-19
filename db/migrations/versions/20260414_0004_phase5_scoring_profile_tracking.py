"""Phase 5 scoring profile tracking.

Revision ID: 20260414_0004
Revises: 20260413_0003
Create Date: 2026-04-14 00:45:00
"""

import sqlalchemy as sa
from alembic import op

revision = "20260414_0004"
down_revision = "20260413_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("score_run") as batch_op:
        batch_op.add_column(sa.Column("profile_name", sa.String(length=255), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("score_run") as batch_op:
        batch_op.drop_column("profile_name")
