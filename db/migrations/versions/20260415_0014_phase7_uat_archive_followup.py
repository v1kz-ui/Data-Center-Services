"""Phase 7 UAT archive export follow-up and retention remediation.

Revision ID: 20260415_0014
Revises: 20260415_0013
Create Date: 2026-04-15 22:15:00
"""

import sqlalchemy as sa
from alembic import op

revision = "20260415_0014"
down_revision = "20260415_0013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "uat_release_archive_export",
        sa.Column("delivery_confirmed_by", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "uat_release_archive_export",
        sa.Column("delivery_confirmed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "uat_release_archive_export",
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "uat_release_archive_export",
        sa.Column(
            "retry_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "uat_release_archive_export",
        sa.Column("last_status_updated_by", sa.String(length=255), nullable=True),
    )

    op.create_table(
        "uat_release_archive_retention_action",
        sa.Column("action_id", sa.Uuid(), nullable=False),
        sa.Column("archive_id", sa.Uuid(), nullable=False),
        sa.Column("related_export_id", sa.Uuid(), nullable=True),
        sa.Column("action_type", sa.String(length=64), nullable=False),
        sa.Column(
            "previous_retention_review_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column("next_retention_review_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("scheduled_retry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("action_notes", sa.Text(), nullable=True),
        sa.Column("recorded_by", sa.String(length=255), nullable=False),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
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
        sa.ForeignKeyConstraint(
            ["related_export_id"],
            ["uat_release_archive_export.export_id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("action_id"),
    )
    op.create_index(
        "ix_uat_release_archive_retention_action_archive_id",
        "uat_release_archive_retention_action",
        ["archive_id"],
        unique=False,
    )
    op.create_index(
        "ix_uat_release_archive_retention_action_action_type",
        "uat_release_archive_retention_action",
        ["action_type"],
        unique=False,
    )
    op.create_index(
        "ix_uat_release_archive_retention_action_related_export_id",
        "uat_release_archive_retention_action",
        ["related_export_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_uat_release_archive_retention_action_related_export_id",
        table_name="uat_release_archive_retention_action",
    )
    op.drop_index(
        "ix_uat_release_archive_retention_action_action_type",
        table_name="uat_release_archive_retention_action",
    )
    op.drop_index(
        "ix_uat_release_archive_retention_action_archive_id",
        table_name="uat_release_archive_retention_action",
    )
    op.drop_table("uat_release_archive_retention_action")

    op.drop_column("uat_release_archive_export", "last_status_updated_by")
    op.drop_column("uat_release_archive_export", "retry_count")
    op.drop_column("uat_release_archive_export", "next_retry_at")
    op.drop_column("uat_release_archive_export", "delivery_confirmed_at")
    op.drop_column("uat_release_archive_export", "delivery_confirmed_by")
