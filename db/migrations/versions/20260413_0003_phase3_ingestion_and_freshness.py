"""Phase 3 ingestion and freshness controls.

Revision ID: 20260413_0003
Revises: 20260413_0002
Create Date: 2026-04-13 23:58:00
"""

import sqlalchemy as sa
from alembic import op

revision = "20260413_0003"
down_revision = "20260413_0002"
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
        "source_record_rejection",
        sa.Column("rejection_id", sa.Uuid(), primary_key=True),
        sa.Column("snapshot_id", sa.Uuid(), nullable=False),
        sa.Column("row_number", sa.Integer(), nullable=False),
        sa.Column("external_key", sa.String(length=255), nullable=True),
        sa.Column("rejection_code", sa.String(length=64), nullable=False),
        sa.Column("rejection_message", sa.Text(), nullable=False),
        sa.Column("raw_payload", sa.Text(), nullable=True),
        *audit_columns(),
        sa.ForeignKeyConstraint(
            ["snapshot_id"],
            ["source_snapshot.snapshot_id"],
            ondelete="CASCADE",
        ),
    )
    op.create_index(
        "ix_source_record_rejection_snapshot_row",
        "source_record_rejection",
        ["snapshot_id", "row_number"],
    )

    op.create_table(
        "raw_zoning",
        sa.Column("zoning_record_id", sa.Uuid(), primary_key=True),
        sa.Column("parcel_id", sa.String(length=64), nullable=False),
        sa.Column("county_fips", sa.String(length=5), nullable=False),
        sa.Column("metro_id", sa.String(length=16), nullable=False),
        sa.Column("zoning_code", sa.String(length=128), nullable=False),
        sa.Column("land_use_code", sa.String(length=128), nullable=True),
        sa.Column("source_snapshot_id", sa.Uuid(), nullable=True),
        sa.Column("lineage_key", sa.String(length=255), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        *audit_columns(),
        sa.ForeignKeyConstraint(["parcel_id"], ["raw_parcels.parcel_id"], ondelete="CASCADE"),
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
        sa.UniqueConstraint(
            "parcel_id",
            "source_snapshot_id",
            name="uq_raw_zoning_parcel_snapshot",
        ),
    )
    op.create_index("ix_raw_zoning_metro_parcel", "raw_zoning", ["metro_id", "parcel_id"])

    op.create_table(
        "source_evidence",
        sa.Column("evidence_id", sa.Uuid(), primary_key=True),
        sa.Column("source_id", sa.String(length=64), nullable=False),
        sa.Column("metro_id", sa.String(length=16), nullable=False),
        sa.Column("county_fips", sa.String(length=5), nullable=True),
        sa.Column("parcel_id", sa.String(length=64), nullable=True),
        sa.Column("source_snapshot_id", sa.Uuid(), nullable=True),
        sa.Column("record_key", sa.String(length=255), nullable=False),
        sa.Column("attribute_name", sa.String(length=128), nullable=False),
        sa.Column("attribute_value", sa.Text(), nullable=False),
        sa.Column("lineage_key", sa.String(length=255), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        *audit_columns(),
        sa.ForeignKeyConstraint(["source_id"], ["source_catalog.source_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["metro_id"], ["metro_catalog.metro_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["county_fips"],
            ["county_catalog.county_fips"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(["parcel_id"], ["raw_parcels.parcel_id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(
            ["source_snapshot_id"],
            ["source_snapshot.snapshot_id"],
            ondelete="SET NULL",
        ),
        sa.UniqueConstraint(
            "source_snapshot_id",
            "record_key",
            "attribute_name",
            name="uq_source_evidence_snapshot_record_attribute",
        ),
    )
    op.create_index("ix_source_evidence_source_metro", "source_evidence", ["source_id", "metro_id"])
    op.create_index("ix_source_evidence_parcel_id", "source_evidence", ["parcel_id"])


def downgrade() -> None:
    op.drop_index("ix_source_evidence_parcel_id", table_name="source_evidence")
    op.drop_index("ix_source_evidence_source_metro", table_name="source_evidence")
    op.drop_table("source_evidence")

    op.drop_index("ix_raw_zoning_metro_parcel", table_name="raw_zoning")
    op.drop_table("raw_zoning")

    op.drop_index("ix_source_record_rejection_snapshot_row", table_name="source_record_rejection")
    op.drop_table("source_record_rejection")
