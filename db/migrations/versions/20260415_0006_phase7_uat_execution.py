"""Phase 7 UAT cycle execution and defect tracking.

Revision ID: 20260415_0006
Revises: 20260415_0005
Create Date: 2026-04-15 13:20:00
"""

import sqlalchemy as sa
from alembic import op

revision = "20260415_0006"
down_revision = "20260415_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "uat_cycle",
        sa.Column("cycle_id", sa.Uuid(), nullable=False),
        sa.Column("cycle_name", sa.String(length=255), nullable=False),
        sa.Column("environment_name", sa.String(length=64), nullable=False),
        sa.Column("scenario_pack_path", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_by", sa.String(length=255), nullable=False),
        sa.Column("summary_notes", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
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
        sa.PrimaryKeyConstraint("cycle_id"),
        sa.UniqueConstraint("cycle_name", name="uq_uat_cycle_cycle_name"),
    )
    op.create_index("ix_uat_cycle_status", "uat_cycle", ["status"], unique=False)

    op.create_table(
        "uat_scenario_execution",
        sa.Column("execution_id", sa.Uuid(), nullable=False),
        sa.Column("cycle_id", sa.Uuid(), nullable=False),
        sa.Column("scenario_id", sa.String(length=64), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("actor_role", sa.String(length=32), nullable=False),
        sa.Column("workflow", sa.String(length=255), nullable=False),
        sa.Column("entrypoint", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("execution_notes", sa.Text(), nullable=True),
        sa.Column("evidence_reference", sa.Text(), nullable=True),
        sa.Column("executed_by", sa.String(length=255), nullable=True),
        sa.Column("executed_at", sa.DateTime(timezone=True), nullable=True),
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
        sa.PrimaryKeyConstraint("execution_id"),
        sa.UniqueConstraint(
            "cycle_id",
            "scenario_id",
            name="uq_uat_scenario_execution_cycle_scenario",
        ),
    )
    op.create_index(
        "ix_uat_scenario_execution_cycle_id",
        "uat_scenario_execution",
        ["cycle_id"],
        unique=False,
    )
    op.create_index(
        "ix_uat_scenario_execution_status",
        "uat_scenario_execution",
        ["status"],
        unique=False,
    )

    op.create_table(
        "uat_defect",
        sa.Column("defect_id", sa.Uuid(), nullable=False),
        sa.Column("cycle_id", sa.Uuid(), nullable=False),
        sa.Column("scenario_id", sa.String(length=64), nullable=True),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("reported_by", sa.String(length=255), nullable=False),
        sa.Column("owner_name", sa.String(length=255), nullable=True),
        sa.Column("external_reference", sa.String(length=255), nullable=True),
        sa.Column("resolution_notes", sa.Text(), nullable=True),
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
        sa.PrimaryKeyConstraint("defect_id"),
    )
    op.create_index("ix_uat_defect_cycle_id", "uat_defect", ["cycle_id"], unique=False)
    op.create_index("ix_uat_defect_severity", "uat_defect", ["severity"], unique=False)
    op.create_index("ix_uat_defect_status", "uat_defect", ["status"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_uat_defect_status", table_name="uat_defect")
    op.drop_index("ix_uat_defect_severity", table_name="uat_defect")
    op.drop_index("ix_uat_defect_cycle_id", table_name="uat_defect")
    op.drop_table("uat_defect")

    op.drop_index(
        "ix_uat_scenario_execution_status",
        table_name="uat_scenario_execution",
    )
    op.drop_index(
        "ix_uat_scenario_execution_cycle_id",
        table_name="uat_scenario_execution",
    )
    op.drop_table("uat_scenario_execution")

    op.drop_index("ix_uat_cycle_status", table_name="uat_cycle")
    op.drop_table("uat_cycle")
