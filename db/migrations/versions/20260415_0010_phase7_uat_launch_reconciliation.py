"""Phase 7 UAT launch reconciliation decision records.

Revision ID: 20260415_0010
Revises: 20260415_0009
Create Date: 2026-04-15 18:15:00
"""

import sqlalchemy as sa
from alembic import op

revision = "20260415_0010"
down_revision = "20260415_0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "uat_launch_decision_record",
        sa.Column("decision_id", sa.Uuid(), nullable=False),
        sa.Column("snapshot_id", sa.Uuid(), nullable=False),
        sa.Column("decision", sa.String(length=32), nullable=False),
        sa.Column("reviewer_name", sa.String(length=255), nullable=False),
        sa.Column("reviewer_role", sa.String(length=255), nullable=True),
        sa.Column("reviewer_organization", sa.String(length=255), nullable=True),
        sa.Column("decision_notes", sa.Text(), nullable=True),
        sa.Column("recorded_by", sa.String(length=255), nullable=False),
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
            ["snapshot_id"],
            ["uat_handoff_snapshot.snapshot_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("decision_id"),
    )
    op.create_index(
        "ix_uat_launch_decision_record_snapshot_id",
        "uat_launch_decision_record",
        ["snapshot_id"],
        unique=False,
    )
    op.create_index(
        "ix_uat_launch_decision_record_decision",
        "uat_launch_decision_record",
        ["decision"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_uat_launch_decision_record_decision",
        table_name="uat_launch_decision_record",
    )
    op.drop_index(
        "ix_uat_launch_decision_record_snapshot_id",
        table_name="uat_launch_decision_record",
    )
    op.drop_table("uat_launch_decision_record")
