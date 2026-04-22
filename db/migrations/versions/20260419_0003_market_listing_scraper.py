"""Market listing scraper framework.

Revision ID: 20260419_0003
Revises: 20260413_0002
Create Date: 2026-04-19 08:10:00
"""

import sqlalchemy as sa
from alembic import op

revision = "20260419_0003"
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
        "listing_source_catalog",
        sa.Column("listing_source_id", sa.String(length=64), primary_key=True),
        sa.Column("display_name", sa.String(length=255), nullable=False),
        sa.Column("acquisition_method", sa.String(length=64), nullable=False),
        sa.Column("base_url", sa.Text(), nullable=True),
        sa.Column("terms_url", sa.Text(), nullable=True),
        sa.Column("allows_scraping", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("compliance_notes", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        *audit_columns(),
    )

    op.create_table(
        "market_listing",
        sa.Column("market_listing_id", sa.Uuid(), primary_key=True),
        sa.Column("source_id", sa.String(length=64), nullable=False),
        sa.Column("listing_source_id", sa.String(length=64), nullable=False),
        sa.Column("metro_id", sa.String(length=16), nullable=False),
        sa.Column("county_fips", sa.String(length=5), nullable=True),
        sa.Column("parcel_id", sa.String(length=64), nullable=True),
        sa.Column("source_snapshot_id", sa.Uuid(), nullable=True),
        sa.Column("source_listing_key", sa.String(length=255), nullable=False),
        sa.Column("listing_title", sa.String(length=255), nullable=False),
        sa.Column("asset_type", sa.String(length=64), nullable=True),
        sa.Column("listing_status", sa.String(length=64), nullable=True),
        sa.Column("asking_price", sa.Numeric(14, 2), nullable=True),
        sa.Column("acreage", sa.Numeric(14, 4), nullable=True),
        sa.Column("building_sqft", sa.Numeric(14, 2), nullable=True),
        sa.Column("address_line1", sa.String(length=255), nullable=True),
        sa.Column("city", sa.String(length=128), nullable=True),
        sa.Column("state_code", sa.String(length=2), nullable=True),
        sa.Column("postal_code", sa.String(length=16), nullable=True),
        sa.Column("latitude", sa.Numeric(10, 7), nullable=True),
        sa.Column("longitude", sa.Numeric(10, 7), nullable=True),
        sa.Column("broker_name", sa.String(length=255), nullable=True),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("last_verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lineage_key", sa.String(length=255), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        *audit_columns(),
        sa.ForeignKeyConstraint(["source_id"], ["source_catalog.source_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["listing_source_id"],
            ["listing_source_catalog.listing_source_id"],
            ondelete="RESTRICT",
        ),
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
            "listing_source_id",
            "source_listing_key",
            name="uq_market_listing_snapshot_source_key",
        ),
    )
    op.create_index(
        "ix_market_listing_source_metro_status",
        "market_listing",
        ["listing_source_id", "metro_id", "is_active"],
    )
    op.create_index("ix_market_listing_parcel_id", "market_listing", ["parcel_id"])
    op.create_index("ix_market_listing_source_id", "market_listing", ["source_id"])


def downgrade() -> None:
    op.drop_index("ix_market_listing_source_id", table_name="market_listing")
    op.drop_index("ix_market_listing_parcel_id", table_name="market_listing")
    op.drop_index("ix_market_listing_source_metro_status", table_name="market_listing")
    op.drop_table("market_listing")
    op.drop_table("listing_source_catalog")
