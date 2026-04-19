"""Phase 7 UAT archive retrieval and evidence retention workflows.

Revision ID: 20260415_0012
Revises: 20260415_0011
Create Date: 2026-04-15 20:15:00
"""

import sqlalchemy as sa
from alembic import op

revision = "20260415_0012"
down_revision = "20260415_0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("uat_release_archive") as batch_op:
        batch_op.add_column(
            sa.Column("retention_review_at", sa.DateTime(timezone=True), nullable=True)
        )
        batch_op.add_column(sa.Column("superseded_by_archive_id", sa.Uuid(), nullable=True))
        batch_op.add_column(sa.Column("supersession_reason", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("superseded_at", sa.DateTime(timezone=True), nullable=True))

    op.execute(
        """
        UPDATE uat_release_archive
        SET retention_review_at = COALESCE(retention_review_at, sealed_at)
        """
    )

    with op.batch_alter_table("uat_release_archive") as batch_op:
        batch_op.alter_column("retention_review_at", nullable=False)
        batch_op.create_foreign_key(
            "fk_uat_release_archive_superseded_by_archive_id",
            "uat_release_archive",
            ["superseded_by_archive_id"],
            ["archive_id"],
            ondelete="SET NULL",
        )
        batch_op.create_index(
            "ix_uat_release_archive_retention_review_at",
            ["retention_review_at"],
            unique=False,
        )
        batch_op.create_index(
            "ix_uat_release_archive_superseded_by_archive_id",
            ["superseded_by_archive_id"],
            unique=False,
        )

    op.create_table(
        "uat_release_archive_evidence_item",
        sa.Column("evidence_item_id", sa.Uuid(), nullable=False),
        sa.Column("archive_id", sa.Uuid(), nullable=False),
        sa.Column("evidence_type", sa.String(length=64), nullable=False),
        sa.Column("evidence_status", sa.String(length=32), nullable=False),
        sa.Column("reference_id", sa.String(length=255), nullable=False),
        sa.Column("reference_name", sa.String(length=255), nullable=False),
        sa.Column("retention_label", sa.String(length=64), nullable=False),
        sa.Column("evidence_summary", sa.Text(), nullable=False),
        sa.Column("source_recorded_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_location", sa.String(length=255), nullable=True),
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
        sa.PrimaryKeyConstraint("evidence_item_id"),
    )
    op.create_index(
        "ix_uat_release_archive_evidence_item_archive_id",
        "uat_release_archive_evidence_item",
        ["archive_id"],
        unique=False,
    )
    op.create_index(
        "ix_uat_release_archive_evidence_item_evidence_type",
        "uat_release_archive_evidence_item",
        ["evidence_type"],
        unique=False,
    )
    op.create_index(
        "ix_uat_release_archive_evidence_item_retention_label",
        "uat_release_archive_evidence_item",
        ["retention_label"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_uat_release_archive_evidence_item_retention_label",
        table_name="uat_release_archive_evidence_item",
    )
    op.drop_index(
        "ix_uat_release_archive_evidence_item_evidence_type",
        table_name="uat_release_archive_evidence_item",
    )
    op.drop_index(
        "ix_uat_release_archive_evidence_item_archive_id",
        table_name="uat_release_archive_evidence_item",
    )
    op.drop_table("uat_release_archive_evidence_item")

    with op.batch_alter_table("uat_release_archive") as batch_op:
        batch_op.drop_index("ix_uat_release_archive_superseded_by_archive_id")
        batch_op.drop_index("ix_uat_release_archive_retention_review_at")
        batch_op.drop_constraint(
            "fk_uat_release_archive_superseded_by_archive_id",
            type_="foreignkey",
        )
        batch_op.drop_column("superseded_at")
        batch_op.drop_column("supersession_reason")
        batch_op.drop_column("superseded_by_archive_id")
        batch_op.drop_column("retention_review_at")
