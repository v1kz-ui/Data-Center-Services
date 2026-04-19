"""Phase 7 UAT archive delivery journaling and notification tracking.

Revision ID: 20260418_0015
Revises: 20260415_0014
Create Date: 2026-04-18 15:30:00
"""

import sqlalchemy as sa
from alembic import op

revision = "20260418_0015"
down_revision = "20260415_0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "uat_release_archive_export_delivery_event",
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("archive_id", sa.Uuid(), nullable=False),
        sa.Column("export_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("target_name", sa.String(length=255), nullable=False),
        sa.Column("delivery_channel", sa.String(length=64), nullable=True),
        sa.Column("external_reference", sa.String(length=255), nullable=True),
        sa.Column("event_notes", sa.Text(), nullable=True),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
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
            ["archive_id"],
            ["uat_release_archive.archive_id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["export_id"],
            ["uat_release_archive_export.export_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("event_id"),
    )
    op.create_index(
        "ix_uat_release_archive_export_delivery_event_archive_id",
        "uat_release_archive_export_delivery_event",
        ["archive_id"],
        unique=False,
    )
    op.create_index(
        "ix_uat_release_archive_export_delivery_event_export_id",
        "uat_release_archive_export_delivery_event",
        ["export_id"],
        unique=False,
    )
    op.create_index(
        "ix_uat_release_archive_export_delivery_event_event_type",
        "uat_release_archive_export_delivery_event",
        ["event_type"],
        unique=False,
    )
    op.create_index(
        "ix_uat_release_archive_export_delivery_event_occurred_at",
        "uat_release_archive_export_delivery_event",
        ["occurred_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_uat_release_archive_export_delivery_event_occurred_at",
        table_name="uat_release_archive_export_delivery_event",
    )
    op.drop_index(
        "ix_uat_release_archive_export_delivery_event_event_type",
        table_name="uat_release_archive_export_delivery_event",
    )
    op.drop_index(
        "ix_uat_release_archive_export_delivery_event_export_id",
        table_name="uat_release_archive_export_delivery_event",
    )
    op.drop_index(
        "ix_uat_release_archive_export_delivery_event_archive_id",
        table_name="uat_release_archive_export_delivery_event",
    )
    op.drop_table("uat_release_archive_export_delivery_event")
