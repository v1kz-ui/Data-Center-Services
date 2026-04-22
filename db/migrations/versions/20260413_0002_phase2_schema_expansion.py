"""Phase 2 canonical schema expansion.

Revision ID: 20260413_0002
Revises: 20260413_0001
Create Date: 2026-04-13 23:45:00
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260413_0002"
down_revision = "20260413_0001"
branch_labels = None
depends_on = None


source_snapshot_status = postgresql.ENUM(
    "success",
    "failed",
    "quarantined",
    name="source_snapshot_status",
    create_type=False,
)
parcel_evaluation_status = postgresql.ENUM(
    "prefiltered_band",
    "prefiltered_size",
    "pending_exclusion_check",
    "pending_scoring",
    "scored",
    "excluded",
    name="parcel_evaluation_status",
    create_type=False,
)


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
    source_snapshot_status.create(bind, checkfirst=True)
    parcel_evaluation_status.create(bind, checkfirst=True)

    op.create_table(
        "metro_catalog",
        sa.Column("metro_id", sa.String(length=16), primary_key=True),
        sa.Column("display_name", sa.String(length=255), nullable=False, unique=True),
        sa.Column("state_code", sa.String(length=2), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        *audit_columns(),
    )

    op.create_table(
        "county_catalog",
        sa.Column("county_fips", sa.String(length=5), primary_key=True),
        sa.Column("metro_id", sa.String(length=16), nullable=False),
        sa.Column("display_name", sa.String(length=255), nullable=False),
        sa.Column("state_code", sa.String(length=2), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        *audit_columns(),
        sa.ForeignKeyConstraint(["metro_id"], ["metro_catalog.metro_id"], ondelete="RESTRICT"),
    )

    op.create_table(
        "source_snapshot",
        sa.Column("snapshot_id", sa.Uuid(), primary_key=True),
        sa.Column("source_id", sa.String(length=64), nullable=False),
        sa.Column("metro_id", sa.String(length=16), nullable=False),
        sa.Column("snapshot_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_version", sa.String(length=128), nullable=False),
        sa.Column("row_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("checksum", sa.String(length=128), nullable=True),
        sa.Column("status", source_snapshot_status, nullable=False, server_default="success"),
        sa.Column("error_message", sa.Text(), nullable=True),
        *audit_columns(),
        sa.CheckConstraint("row_count >= 0", name="ck_source_snapshot_row_count_nonnegative"),
        sa.ForeignKeyConstraint(["source_id"], ["source_catalog.source_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["metro_id"], ["metro_catalog.metro_id"], ondelete="RESTRICT"),
    )
    op.create_index(
        "ix_source_snapshot_source_metro_ts",
        "source_snapshot",
        ["source_id", "metro_id", "snapshot_ts"],
    )

    op.create_table(
        "raw_parcels",
        sa.Column("parcel_id", sa.String(length=64), primary_key=True),
        sa.Column("county_fips", sa.String(length=5), nullable=False),
        sa.Column("metro_id", sa.String(length=16), nullable=False),
        sa.Column("apn", sa.String(length=128), nullable=True),
        sa.Column("acreage", sa.Numeric(12, 2), nullable=False),
        sa.Column("geometry_wkt", sa.Text(), nullable=False),
        sa.Column("source_snapshot_id", sa.Uuid(), nullable=True),
        sa.Column("lineage_key", sa.String(length=255), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        *audit_columns(),
        sa.CheckConstraint("acreage >= 0", name="ck_raw_parcels_acreage_nonnegative"),
        sa.ForeignKeyConstraint(
            ["county_fips"],
            ["county_catalog.county_fips"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(["metro_id"], ["metro_catalog.metro_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["source_snapshot_id"],
            ["source_snapshot.snapshot_id"],
            ondelete="SET NULL",
        ),
    )
    op.create_index(
        "ix_raw_parcels_county_fips_parcel_id",
        "raw_parcels",
        ["county_fips", "parcel_id"],
    )
    op.create_index("ix_raw_parcels_metro_id", "raw_parcels", ["metro_id"])

    op.create_table(
        "parcel_rep_point",
        sa.Column("parcel_id", sa.String(length=64), primary_key=True),
        sa.Column("rep_point_wkt", sa.Text(), nullable=False),
        sa.Column(
            "geometry_method",
            sa.String(length=64),
            nullable=False,
            server_default="representative_point",
        ),
        sa.Column("source_snapshot_id", sa.Uuid(), nullable=True),
        *audit_columns(),
        sa.ForeignKeyConstraint(["parcel_id"], ["raw_parcels.parcel_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["source_snapshot_id"],
            ["source_snapshot.snapshot_id"],
            ondelete="SET NULL",
        ),
    )
    op.create_index(
        "ix_parcel_rep_point_source_snapshot_id",
        "parcel_rep_point",
        ["source_snapshot_id"],
    )

    op.create_table(
        "parcel_evaluations",
        sa.Column("evaluation_id", sa.Uuid(), primary_key=True),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("parcel_id", sa.String(length=64), nullable=False),
        sa.Column(
            "status",
            parcel_evaluation_status,
            nullable=False,
            server_default="pending_exclusion_check",
        ),
        sa.Column("status_reason", sa.Text(), nullable=True),
        sa.Column("viability_score", sa.Numeric(5, 2), nullable=True),
        sa.Column("confidence_score", sa.Numeric(5, 2), nullable=True),
        *audit_columns(),
        sa.CheckConstraint(
            "(status != 'scored') OR (confidence_score IS NOT NULL)",
            name="ck_parcel_evaluations_confidence_required_when_scored",
        ),
        sa.ForeignKeyConstraint(["run_id"], ["score_run.run_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["parcel_id"], ["raw_parcels.parcel_id"], ondelete="CASCADE"),
        sa.UniqueConstraint("run_id", "parcel_id", name="uq_parcel_evaluations_run_parcel"),
    )
    op.create_index(
        "ix_parcel_evaluations_run_status",
        "parcel_evaluations",
        ["run_id", "status"],
    )

    op.create_table(
        "parcel_exclusion_events",
        sa.Column("exclusion_event_id", sa.Uuid(), primary_key=True),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("parcel_id", sa.String(length=64), nullable=False),
        sa.Column("evaluation_id", sa.Uuid(), nullable=True),
        sa.Column("exclusion_code", sa.String(length=64), nullable=False),
        sa.Column("exclusion_reason", sa.Text(), nullable=False),
        sa.Column("rule_version", sa.String(length=64), nullable=True),
        *audit_columns(),
        sa.ForeignKeyConstraint(["run_id"], ["score_run.run_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["parcel_id"], ["raw_parcels.parcel_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["evaluation_id"],
            ["parcel_evaluations.evaluation_id"],
            ondelete="SET NULL",
        ),
    )
    op.create_index(
        "ix_parcel_exclusion_events_run_parcel",
        "parcel_exclusion_events",
        ["run_id", "parcel_id"],
    )

    op.create_table(
        "score_factor_detail",
        sa.Column("factor_detail_id", sa.Uuid(), primary_key=True),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("parcel_id", sa.String(length=64), nullable=False),
        sa.Column("factor_id", sa.String(length=16), nullable=False),
        sa.Column("points_awarded", sa.Numeric(5, 2), nullable=False, server_default="0"),
        sa.Column("rationale", sa.Text(), nullable=True),
        *audit_columns(),
        sa.CheckConstraint("points_awarded >= 0", name="ck_score_factor_detail_points_nonnegative"),
        sa.ForeignKeyConstraint(["run_id"], ["score_run.run_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["parcel_id"], ["raw_parcels.parcel_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["factor_id"], ["factor_catalog.factor_id"], ondelete="RESTRICT"),
        sa.UniqueConstraint(
            "run_id",
            "parcel_id",
            "factor_id",
            name="uq_score_factor_detail_run_parcel_factor",
        ),
    )
    op.create_index(
        "ix_score_factor_detail_run_parcel",
        "score_factor_detail",
        ["run_id", "parcel_id"],
    )

    op.create_table(
        "score_factor_input",
        sa.Column("factor_input_id", sa.Uuid(), primary_key=True),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("parcel_id", sa.String(length=64), nullable=False),
        sa.Column("factor_id", sa.String(length=16), nullable=False),
        sa.Column("input_name", sa.String(length=128), nullable=False),
        sa.Column("input_value", sa.Text(), nullable=False),
        sa.Column("evidence_quality", sa.String(length=32), nullable=False),
        *audit_columns(),
        sa.ForeignKeyConstraint(["run_id"], ["score_run.run_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["parcel_id"], ["raw_parcels.parcel_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["factor_id"], ["factor_catalog.factor_id"], ondelete="RESTRICT"),
        sa.UniqueConstraint(
            "run_id",
            "parcel_id",
            "factor_id",
            "input_name",
            name="uq_score_factor_input_run_parcel_factor_input",
        ),
    )
    op.create_index(
        "ix_score_factor_input_run_parcel_factor",
        "score_factor_input",
        ["run_id", "parcel_id", "factor_id"],
    )

    op.create_table(
        "score_bonus_detail",
        sa.Column("bonus_detail_id", sa.Uuid(), primary_key=True),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("parcel_id", sa.String(length=64), nullable=False),
        sa.Column("bonus_id", sa.String(length=16), nullable=False),
        sa.Column("applied", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("points_awarded", sa.Numeric(5, 2), nullable=False, server_default="0"),
        sa.Column("rationale", sa.Text(), nullable=True),
        *audit_columns(),
        sa.CheckConstraint("points_awarded >= 0", name="ck_score_bonus_detail_points_nonnegative"),
        sa.ForeignKeyConstraint(["run_id"], ["score_run.run_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["parcel_id"], ["raw_parcels.parcel_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["bonus_id"], ["bonus_catalog.bonus_id"], ondelete="RESTRICT"),
        sa.UniqueConstraint(
            "run_id",
            "parcel_id",
            "bonus_id",
            name="uq_score_bonus_detail_run_parcel_bonus",
        ),
    )
    op.create_index(
        "ix_score_bonus_detail_run_parcel",
        "score_bonus_detail",
        ["run_id", "parcel_id"],
    )

    with op.batch_alter_table("score_batch") as batch_op:
        batch_op.create_check_constraint(
            "ck_score_batch_completed_metros_le_expected_metros",
            "completed_metros <= expected_metros",
        )

    op.create_index(
        "ix_score_run_batch_metro_status",
        "score_run",
        ["batch_id", "metro_id", "status"],
    )


def downgrade() -> None:
    op.drop_index("ix_score_run_batch_metro_status", table_name="score_run")

    with op.batch_alter_table("score_batch") as batch_op:
        batch_op.drop_constraint(
            "ck_score_batch_completed_metros_le_expected_metros",
            type_="check",
        )

    op.drop_index("ix_score_bonus_detail_run_parcel", table_name="score_bonus_detail")
    op.drop_table("score_bonus_detail")

    op.drop_index("ix_score_factor_input_run_parcel_factor", table_name="score_factor_input")
    op.drop_table("score_factor_input")

    op.drop_index("ix_score_factor_detail_run_parcel", table_name="score_factor_detail")
    op.drop_table("score_factor_detail")

    op.drop_index("ix_parcel_exclusion_events_run_parcel", table_name="parcel_exclusion_events")
    op.drop_table("parcel_exclusion_events")

    op.drop_index("ix_parcel_evaluations_run_status", table_name="parcel_evaluations")
    op.drop_table("parcel_evaluations")

    op.drop_index("ix_parcel_rep_point_source_snapshot_id", table_name="parcel_rep_point")
    op.drop_table("parcel_rep_point")

    op.drop_index("ix_raw_parcels_metro_id", table_name="raw_parcels")
    op.drop_index("ix_raw_parcels_county_fips_parcel_id", table_name="raw_parcels")
    op.drop_table("raw_parcels")

    op.drop_index("ix_source_snapshot_source_metro_ts", table_name="source_snapshot")
    op.drop_table("source_snapshot")

    op.drop_table("county_catalog")
    op.drop_table("metro_catalog")

    bind = op.get_bind()
    parcel_evaluation_status.drop(bind, checkfirst=True)
    source_snapshot_status.drop(bind, checkfirst=True)
