from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.models.mixins import TimestampMixin


class ListingSourceCatalog(TimestampMixin, Base):
    __tablename__ = "listing_source_catalog"

    listing_source_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    acquisition_method: Mapped[str] = mapped_column(String(64), nullable=False)
    base_url: Mapped[str | None] = mapped_column(Text)
    terms_url: Mapped[str | None] = mapped_column(Text)
    allows_scraping: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    compliance_notes: Mapped[str | None] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class MarketListing(TimestampMixin, Base):
    __tablename__ = "market_listing"
    __table_args__ = (
        UniqueConstraint(
            "source_snapshot_id",
            "listing_source_id",
            "source_listing_key",
            name="uq_market_listing_snapshot_source_key",
        ),
        Index("ix_market_listing_source_metro_status", "listing_source_id", "metro_id", "is_active"),
        Index("ix_market_listing_parcel_id", "parcel_id"),
        Index("ix_market_listing_source_id", "source_id"),
    )

    market_listing_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    source_id: Mapped[str] = mapped_column(
        ForeignKey("source_catalog.source_id", ondelete="RESTRICT"),
        nullable=False,
    )
    listing_source_id: Mapped[str] = mapped_column(
        ForeignKey("listing_source_catalog.listing_source_id", ondelete="RESTRICT"),
        nullable=False,
    )
    metro_id: Mapped[str] = mapped_column(
        ForeignKey("metro_catalog.metro_id", ondelete="RESTRICT"),
        nullable=False,
    )
    county_fips: Mapped[str | None] = mapped_column(
        ForeignKey("county_catalog.county_fips", ondelete="RESTRICT"),
    )
    parcel_id: Mapped[str | None] = mapped_column(
        ForeignKey("raw_parcels.parcel_id", ondelete="SET NULL"),
    )
    source_snapshot_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("source_snapshot.snapshot_id", ondelete="SET NULL"),
    )
    source_listing_key: Mapped[str] = mapped_column(String(255), nullable=False)
    listing_title: Mapped[str] = mapped_column(String(255), nullable=False)
    asset_type: Mapped[str | None] = mapped_column(String(64))
    listing_status: Mapped[str | None] = mapped_column(String(64))
    asking_price: Mapped[float | None] = mapped_column(Numeric(14, 2))
    acreage: Mapped[float | None] = mapped_column(Numeric(14, 4))
    building_sqft: Mapped[float | None] = mapped_column(Numeric(14, 2))
    address_line1: Mapped[str | None] = mapped_column(String(255))
    city: Mapped[str | None] = mapped_column(String(128))
    state_code: Mapped[str | None] = mapped_column(String(2))
    postal_code: Mapped[str | None] = mapped_column(String(16))
    latitude: Mapped[float | None] = mapped_column(Numeric(10, 7))
    longitude: Mapped[float | None] = mapped_column(Numeric(10, 7))
    broker_name: Mapped[str | None] = mapped_column(String(255))
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    last_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lineage_key: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
