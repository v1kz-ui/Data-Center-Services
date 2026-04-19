"""Phase 7 operator action event audit log.

Revision ID: 20260415_0005
Revises: 20260414_0004
Create Date: 2026-04-15 10:15:00
"""

import sqlalchemy as sa
from alembic import op

revision = "20260415_0005"
down_revision = "20260414_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "operator_action_event",
        sa.Column("action_event_id", sa.Uuid(), nullable=False),
        sa.Column("action_type", sa.String(length=64), nullable=False),
        sa.Column("target_type", sa.String(length=32), nullable=False),
        sa.Column("target_id", sa.String(length=64), nullable=False),
        sa.Column("batch_id", sa.Uuid(), nullable=True),
        sa.Column("run_id", sa.Uuid(), nullable=True),
        sa.Column("actor_name", sa.String(length=255), nullable=False),
        sa.Column("action_reason", sa.Text(), nullable=True),
        sa.Column("action_payload", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["batch_id"], ["score_batch.batch_id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["run_id"], ["score_run.run_id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("action_event_id"),
    )
    op.create_index(
        "ix_operator_action_event_batch_id",
        "operator_action_event",
        ["batch_id"],
        unique=False,
    )
    op.create_index(
        "ix_operator_action_event_run_id",
        "operator_action_event",
        ["run_id"],
        unique=False,
    )
    op.create_index(
        "ix_operator_action_event_action_type",
        "operator_action_event",
        ["action_type"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_operator_action_event_action_type", table_name="operator_action_event")
    op.drop_index("ix_operator_action_event_run_id", table_name="operator_action_event")
    op.drop_index("ix_operator_action_event_batch_id", table_name="operator_action_event")
    op.drop_table("operator_action_event")
