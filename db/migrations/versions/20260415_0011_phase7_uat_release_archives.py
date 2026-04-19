"""Phase 7 UAT release archives and support handoff bundles.

Revision ID: 20260415_0011
Revises: 20260415_0010
Create Date: 2026-04-15 19:05:00
"""

import sqlalchemy as sa
from alembic import op

revision = "20260415_0011"
down_revision = "20260415_0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "uat_release_archive",
        sa.Column("archive_id", sa.Uuid(), nullable=False),
        sa.Column("snapshot_id", sa.Uuid(), nullable=False),
        sa.Column("archive_name", sa.String(length=255), nullable=False),
        sa.Column("recommended_outcome", sa.String(length=32), nullable=False),
        sa.Column("blocking_exception_count", sa.Integer(), nullable=False),
        sa.Column("attention_exception_count", sa.Integer(), nullable=False),
        sa.Column("support_handoff_owner", sa.String(length=255), nullable=True),
        sa.Column("support_handoff_summary", sa.Text(), nullable=False),
        sa.Column("operations_runbook_reference", sa.String(length=255), nullable=True),
        sa.Column("archive_summary", sa.Text(), nullable=False),
        sa.Column("manifest_payload", sa.Text(), nullable=False),
        sa.Column("archive_checksum", sa.String(length=64), nullable=False),
        sa.Column("created_by", sa.String(length=255), nullable=False),
        sa.Column("sealed_at", sa.DateTime(timezone=True), nullable=False),
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
        sa.PrimaryKeyConstraint("archive_id"),
        sa.UniqueConstraint(
            "snapshot_id",
            "archive_name",
            name="uq_uat_release_archive_snapshot_archive_name",
        ),
    )
    op.create_index(
        "ix_uat_release_archive_snapshot_id",
        "uat_release_archive",
        ["snapshot_id"],
        unique=False,
    )
    op.create_index(
        "ix_uat_release_archive_recommended_outcome",
        "uat_release_archive",
        ["recommended_outcome"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_uat_release_archive_recommended_outcome",
        table_name="uat_release_archive",
    )
    op.drop_index(
        "ix_uat_release_archive_snapshot_id",
        table_name="uat_release_archive",
    )
    op.drop_table("uat_release_archive")
