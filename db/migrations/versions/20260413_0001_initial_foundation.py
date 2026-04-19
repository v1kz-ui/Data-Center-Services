"""Initial foundation schema.

Revision ID: 20260413_0001
Revises: None
Create Date: 2026-04-13 22:45:00
"""

import sqlalchemy as sa
from alembic import op

revision = "20260413_0001"
down_revision = None
branch_labels = None
depends_on = None


score_batch_status = sa.Enum("building", "failed", "completed", "active", name="score_batch_status")
score_run_status = sa.Enum("running", "failed", "completed", name="score_run_status")
scoring_profile_status = sa.Enum("draft", "active", "retired", name="scoring_profile_status")


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
    bind = op.get_bind()
    score_batch_status.create(bind, checkfirst=True)
    score_run_status.create(bind, checkfirst=True)
    scoring_profile_status.create(bind, checkfirst=True)

    op.create_table(
        "source_catalog",
        sa.Column("source_id", sa.String(length=64), primary_key=True),
        sa.Column("display_name", sa.String(length=255), nullable=False),
        sa.Column("owner_name", sa.String(length=255), nullable=False),
        sa.Column("refresh_cadence", sa.String(length=64), nullable=False),
        sa.Column("block_refresh", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("metro_coverage", sa.Text(), nullable=True),
        sa.Column("target_table_name", sa.String(length=255), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        *audit_columns(),
    )

    op.create_table(
        "source_interface",
        sa.Column("interface_id", sa.Uuid(), primary_key=True),
        sa.Column("source_id", sa.String(length=64), nullable=False),
        sa.Column("interface_name", sa.String(length=128), nullable=False),
        sa.Column("schema_version", sa.String(length=64), nullable=False),
        sa.Column("load_mode", sa.String(length=32), nullable=False, server_default="full"),
        sa.Column("validation_notes", sa.Text(), nullable=True),
        *audit_columns(),
        sa.ForeignKeyConstraint(["source_id"], ["source_catalog.source_id"], ondelete="CASCADE"),
        sa.UniqueConstraint("source_id", "interface_name", name="uq_source_interface_name"),
    )

    op.create_table(
        "factor_catalog",
        sa.Column("factor_id", sa.String(length=16), primary_key=True),
        sa.Column("display_name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        *audit_columns(),
    )

    op.create_table(
        "bonus_catalog",
        sa.Column("bonus_id", sa.String(length=16), primary_key=True),
        sa.Column("display_name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("max_points", sa.Integer(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        *audit_columns(),
    )

    op.create_table(
        "scoring_profile",
        sa.Column("profile_id", sa.Uuid(), primary_key=True),
        sa.Column("profile_name", sa.String(length=255), nullable=False, unique=True),
        sa.Column("version_label", sa.String(length=64), nullable=False),
        sa.Column("status", scoring_profile_status, nullable=False, server_default="draft"),
        sa.Column("effective_from", sa.DateTime(timezone=True), nullable=True),
        sa.Column("effective_to", sa.DateTime(timezone=True), nullable=True),
        *audit_columns(),
    )

    op.create_table(
        "scoring_profile_factor",
        sa.Column("profile_factor_id", sa.Uuid(), primary_key=True),
        sa.Column("profile_id", sa.Uuid(), nullable=False),
        sa.Column("factor_id", sa.String(length=16), nullable=False),
        sa.Column("max_points", sa.Integer(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        *audit_columns(),
        sa.ForeignKeyConstraint(["factor_id"], ["factor_catalog.factor_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["profile_id"], ["scoring_profile.profile_id"], ondelete="CASCADE"),
        sa.UniqueConstraint("profile_id", "factor_id", name="uq_profile_factor"),
        sa.UniqueConstraint("profile_id", "ordinal", name="uq_profile_factor_ordinal"),
    )

    op.create_table(
        "score_batch",
        sa.Column("batch_id", sa.Uuid(), primary_key=True),
        sa.Column("status", score_batch_status, nullable=False, server_default="building"),
        sa.Column("expected_metros", sa.Integer(), nullable=False),
        sa.Column("completed_metros", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("activated_at", sa.DateTime(timezone=True), nullable=True),
        *audit_columns(),
        sa.CheckConstraint(
            "expected_metros >= 0",
            name="ck_score_batch_expected_metros_nonnegative",
        ),
        sa.CheckConstraint(
            "completed_metros >= 0",
            name="ck_score_batch_completed_metros_nonnegative",
        ),
    )

    op.create_table(
        "score_run",
        sa.Column("run_id", sa.Uuid(), primary_key=True),
        sa.Column("batch_id", sa.Uuid(), nullable=False),
        sa.Column("metro_id", sa.String(length=64), nullable=False),
        sa.Column("status", score_run_status, nullable=False, server_default="running"),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        *audit_columns(),
        sa.ForeignKeyConstraint(["batch_id"], ["score_batch.batch_id"], ondelete="CASCADE"),
        sa.UniqueConstraint("batch_id", "metro_id", name="uq_score_run_batch_metro"),
    )


def downgrade() -> None:
    op.drop_table("score_run")
    op.drop_table("score_batch")
    op.drop_table("scoring_profile_factor")
    op.drop_table("scoring_profile")
    op.drop_table("bonus_catalog")
    op.drop_table("factor_catalog")
    op.drop_table("source_interface")
    op.drop_table("source_catalog")

    bind = op.get_bind()
    scoring_profile_status.drop(bind, checkfirst=True)
    score_run_status.drop(bind, checkfirst=True)
    score_batch_status.drop(bind, checkfirst=True)
