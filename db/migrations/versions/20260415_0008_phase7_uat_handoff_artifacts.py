"""Phase 7 UAT handoff snapshots and acceptance artifacts.

Revision ID: 20260415_0008
Revises: 20260415_0007
Create Date: 2026-04-15 16:40:00
"""

import sqlalchemy as sa
from alembic import op

revision = "20260415_0008"
down_revision = "20260415_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "uat_handoff_snapshot",
        sa.Column("snapshot_id", sa.Uuid(), nullable=False),
        sa.Column("cycle_id", sa.Uuid(), nullable=False),
        sa.Column("snapshot_name", sa.String(length=255), nullable=False),
        sa.Column("report_version", sa.String(length=64), nullable=False),
        sa.Column("export_scope", sa.String(length=32), nullable=False),
        sa.Column("cycle_status", sa.String(length=32), nullable=False),
        sa.Column("approval_ready", sa.Boolean(), nullable=False),
        sa.Column("blocking_issue_count", sa.Integer(), nullable=False),
        sa.Column("open_defect_count", sa.Integer(), nullable=False),
        sa.Column("open_high_severity_defect_count", sa.Integer(), nullable=False),
        sa.Column("distribution_summary", sa.Text(), nullable=False),
        sa.Column("report_payload", sa.Text(), nullable=False),
        sa.Column("created_by", sa.String(length=255), nullable=False),
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
        sa.PrimaryKeyConstraint("snapshot_id"),
        sa.UniqueConstraint(
            "cycle_id",
            "snapshot_name",
            name="uq_uat_handoff_snapshot_cycle_snapshot_name",
        ),
    )
    op.create_index(
        "ix_uat_handoff_snapshot_cycle_id",
        "uat_handoff_snapshot",
        ["cycle_id"],
        unique=False,
    )
    op.create_index(
        "ix_uat_handoff_snapshot_approval_ready",
        "uat_handoff_snapshot",
        ["approval_ready"],
        unique=False,
    )

    op.create_table(
        "uat_acceptance_artifact",
        sa.Column("artifact_id", sa.Uuid(), nullable=False),
        sa.Column("snapshot_id", sa.Uuid(), nullable=False),
        sa.Column("decision", sa.String(length=32), nullable=False),
        sa.Column("stakeholder_name", sa.String(length=255), nullable=False),
        sa.Column("stakeholder_role", sa.String(length=255), nullable=True),
        sa.Column("stakeholder_organization", sa.String(length=255), nullable=True),
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
        sa.PrimaryKeyConstraint("artifact_id"),
    )
    op.create_index(
        "ix_uat_acceptance_artifact_snapshot_id",
        "uat_acceptance_artifact",
        ["snapshot_id"],
        unique=False,
    )
    op.create_index(
        "ix_uat_acceptance_artifact_decision",
        "uat_acceptance_artifact",
        ["decision"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_uat_acceptance_artifact_decision",
        table_name="uat_acceptance_artifact",
    )
    op.drop_index(
        "ix_uat_acceptance_artifact_snapshot_id",
        table_name="uat_acceptance_artifact",
    )
    op.drop_table("uat_acceptance_artifact")

    op.drop_index(
        "ix_uat_handoff_snapshot_approval_ready",
        table_name="uat_handoff_snapshot",
    )
    op.drop_index(
        "ix_uat_handoff_snapshot_cycle_id",
        table_name="uat_handoff_snapshot",
    )
    op.drop_table("uat_handoff_snapshot")
