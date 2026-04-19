"""Phase 7 UAT archive retention operations and export workflows.

Revision ID: 20260415_0013
Revises: 20260415_0012
Create Date: 2026-04-15 21:15:00
"""

import sqlalchemy as sa
from alembic import op

revision = "20260415_0013"
down_revision = "20260415_0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "uat_release_archive_export",
        sa.Column("export_id", sa.Uuid(), nullable=False),
        sa.Column("archive_id", sa.Uuid(), nullable=False),
        sa.Column("export_name", sa.String(length=255), nullable=False),
        sa.Column("export_scope", sa.String(length=64), nullable=False),
        sa.Column("destination_system", sa.String(length=128), nullable=False),
        sa.Column("destination_reference", sa.String(length=255), nullable=True),
        sa.Column("handoff_status", sa.String(length=32), nullable=False),
        sa.Column("trigger_reason", sa.Text(), nullable=True),
        sa.Column("handoff_notes", sa.Text(), nullable=True),
        sa.Column("export_payload", sa.Text(), nullable=False),
        sa.Column("export_checksum", sa.String(length=64), nullable=False),
        sa.Column("exported_by", sa.String(length=255), nullable=False),
        sa.Column("exported_at", sa.DateTime(timezone=True), nullable=False),
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
            ["archive_id"],
            ["uat_release_archive.archive_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("export_id"),
        sa.UniqueConstraint(
            "archive_id",
            "export_name",
            name="uq_uat_release_archive_export_archive_export_name",
        ),
    )
    op.create_index(
        "ix_uat_release_archive_export_archive_id",
        "uat_release_archive_export",
        ["archive_id"],
        unique=False,
    )
    op.create_index(
        "ix_uat_release_archive_export_export_scope",
        "uat_release_archive_export",
        ["export_scope"],
        unique=False,
    )
    op.create_index(
        "ix_uat_release_archive_export_destination_system",
        "uat_release_archive_export",
        ["destination_system"],
        unique=False,
    )
    op.create_index(
        "ix_uat_release_archive_export_handoff_status",
        "uat_release_archive_export",
        ["handoff_status"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_uat_release_archive_export_handoff_status",
        table_name="uat_release_archive_export",
    )
    op.drop_index(
        "ix_uat_release_archive_export_destination_system",
        table_name="uat_release_archive_export",
    )
    op.drop_index(
        "ix_uat_release_archive_export_export_scope",
        table_name="uat_release_archive_export",
    )
    op.drop_index(
        "ix_uat_release_archive_export_archive_id",
        table_name="uat_release_archive_export",
    )
    op.drop_table("uat_release_archive_export")
