"""Phase 7 UAT sign-off event history.

Revision ID: 20260415_0007
Revises: 20260415_0006
Create Date: 2026-04-15 15:25:00
"""

import sqlalchemy as sa
from alembic import op

revision = "20260415_0007"
down_revision = "20260415_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "uat_cycle_event",
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("cycle_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("actor_name", sa.String(length=255), nullable=False),
        sa.Column("scenario_id", sa.String(length=64), nullable=True),
        sa.Column("defect_id", sa.Uuid(), nullable=True),
        sa.Column("event_notes", sa.Text(), nullable=True),
        sa.Column("event_payload", sa.Text(), nullable=True),
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
        sa.ForeignKeyConstraint(
            ["cycle_id"],
            ["uat_cycle.cycle_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("event_id"),
    )
    op.create_index(
        "ix_uat_cycle_event_cycle_id",
        "uat_cycle_event",
        ["cycle_id"],
        unique=False,
    )
    op.create_index(
        "ix_uat_cycle_event_event_type",
        "uat_cycle_event",
        ["event_type"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_uat_cycle_event_event_type", table_name="uat_cycle_event")
    op.drop_index("ix_uat_cycle_event_cycle_id", table_name="uat_cycle_event")
    op.drop_table("uat_cycle_event")
