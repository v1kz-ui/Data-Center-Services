"""Phase 8 connector refresh foundation.

Revision ID: 20260418_0016
Revises: 20260418_0015
Create Date: 2026-04-18 17:20:00
"""

import sqlalchemy as sa
from alembic import op

revision = "20260418_0016"
down_revision = "20260418_0015"
branch_labels = None
depends_on = None


def audit_columns() -> tuple[sa.Column, sa.Column]:
    return (
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )


def upgrade() -> None:
    op.create_table(
        "source_refresh_job",
        sa.Column("job_id", sa.Uuid(), primary_key=True),
        sa.Column("source_id", sa.String(length=64), nullable=False),
        sa.Column("metro_id", sa.String(length=16), nullable=False),
        sa.Column("connector_key", sa.String(length=128), nullable=False),
        sa.Column("trigger_mode", sa.String(length=32), nullable=False, server_default="manual"),
        sa.Column("actor_name", sa.String(length=255), nullable=False, server_default="system"),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="running"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("source_version", sa.String(length=128), nullable=True),
        sa.Column("snapshot_id", sa.Uuid(), nullable=True),
        sa.Column("row_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("accepted_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("rejected_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("checkpoint_in_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("checkpoint_out_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("checkpoint_cursor_in", sa.Text(), nullable=True),
        sa.Column("checkpoint_cursor_out", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        *audit_columns(),
        sa.ForeignKeyConstraint(["source_id"], ["source_catalog.source_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["metro_id"], ["metro_catalog.metro_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["snapshot_id"], ["source_snapshot.snapshot_id"], ondelete="SET NULL"),
    )
    op.create_index(
        "ix_source_refresh_job_source_metro_started",
        "source_refresh_job",
        ["source_id", "metro_id", "started_at"],
    )
    op.create_index("ix_source_refresh_job_status", "source_refresh_job", ["status"])

    op.create_table(
        "source_refresh_checkpoint",
        sa.Column("checkpoint_id", sa.Uuid(), primary_key=True),
        sa.Column("source_id", sa.String(length=64), nullable=False),
        sa.Column("metro_id", sa.String(length=16), nullable=False),
        sa.Column("connector_key", sa.String(length=128), nullable=False),
        sa.Column("source_version", sa.String(length=128), nullable=True),
        sa.Column("snapshot_id", sa.Uuid(), nullable=True),
        sa.Column("checkpoint_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("checkpoint_cursor", sa.Text(), nullable=True),
        sa.Column("last_job_id", sa.Uuid(), nullable=True),
        sa.Column("last_status", sa.String(length=32), nullable=True),
        sa.Column("last_error_message", sa.Text(), nullable=True),
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=True),
        *audit_columns(),
        sa.ForeignKeyConstraint(["source_id"], ["source_catalog.source_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["metro_id"], ["metro_catalog.metro_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["snapshot_id"], ["source_snapshot.snapshot_id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["last_job_id"], ["source_refresh_job.job_id"], ondelete="SET NULL"),
        sa.UniqueConstraint("source_id", "metro_id", name="uq_source_refresh_checkpoint_source_metro"),
    )


def downgrade() -> None:
    op.drop_table("source_refresh_checkpoint")
    op.drop_index("ix_source_refresh_job_status", table_name="source_refresh_job")
    op.drop_index("ix_source_refresh_job_source_metro_started", table_name="source_refresh_job")
    op.drop_table("source_refresh_job")
