from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, CheckConstraint, ForeignKey, Index, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
from app.db.models.mixins import TimestampMixin

if TYPE_CHECKING:
    from app.db.models.evaluation import ParcelEvaluation
    from app.db.models.ingestion import SourceSnapshot


class MetroCatalog(TimestampMixin, Base):
    __tablename__ = "metro_catalog"

    metro_id: Mapped[str] = mapped_column(String(16), primary_key=True)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    state_code: Mapped[str] = mapped_column(String(2), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    counties: Mapped[list[CountyCatalog]] = relationship(
        back_populates="metro",
        cascade="all, delete-orphan",
    )
    parcels: Mapped[list[RawParcel]] = relationship(back_populates="metro")
    source_snapshots: Mapped[list[SourceSnapshot]] = relationship(back_populates="metro")


class CountyCatalog(TimestampMixin, Base):
    __tablename__ = "county_catalog"

    county_fips: Mapped[str] = mapped_column(String(5), primary_key=True)
    metro_id: Mapped[str] = mapped_column(
        ForeignKey("metro_catalog.metro_id", ondelete="RESTRICT"),
        nullable=False,
    )
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    state_code: Mapped[str] = mapped_column(String(2), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    metro: Mapped[MetroCatalog] = relationship(back_populates="counties")
    parcels: Mapped[list[RawParcel]] = relationship(back_populates="county")


class RawParcel(TimestampMixin, Base):
    __tablename__ = "raw_parcels"
    __table_args__ = (
        CheckConstraint("acreage >= 0", name="acreage_nonnegative"),
        Index("ix_raw_parcels_county_fips_parcel_id", "county_fips", "parcel_id"),
        Index("ix_raw_parcels_metro_id", "metro_id"),
    )

    parcel_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    county_fips: Mapped[str] = mapped_column(
        ForeignKey("county_catalog.county_fips", ondelete="RESTRICT"),
        nullable=False,
    )
    metro_id: Mapped[str] = mapped_column(
        ForeignKey("metro_catalog.metro_id", ondelete="RESTRICT"),
        nullable=False,
    )
    apn: Mapped[str | None] = mapped_column(String(128))
    acreage: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    geometry_wkt: Mapped[str] = mapped_column(Text, nullable=False)
    source_snapshot_id: Mapped[str | None] = mapped_column(
        ForeignKey("source_snapshot.snapshot_id", ondelete="SET NULL"),
    )
    lineage_key: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    metro: Mapped[MetroCatalog] = relationship(back_populates="parcels")
    county: Mapped[CountyCatalog] = relationship(back_populates="parcels")
    rep_point: Mapped[ParcelRepPoint | None] = relationship(
        back_populates="parcel",
        uselist=False,
        cascade="all, delete-orphan",
    )
    evaluations: Mapped[list[ParcelEvaluation]] = relationship(back_populates="parcel")


class ParcelRepPoint(TimestampMixin, Base):
    __tablename__ = "parcel_rep_point"
    __table_args__ = (
        Index("ix_parcel_rep_point_source_snapshot_id", "source_snapshot_id"),
    )

    parcel_id: Mapped[str] = mapped_column(
        ForeignKey("raw_parcels.parcel_id", ondelete="CASCADE"),
        primary_key=True,
    )
    rep_point_wkt: Mapped[str] = mapped_column(Text, nullable=False)
    geometry_method: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        default="representative_point",
    )
    source_snapshot_id: Mapped[str | None] = mapped_column(
        ForeignKey("source_snapshot.snapshot_id", ondelete="SET NULL"),
    )

    parcel: Mapped[RawParcel] = relationship(back_populates="rep_point")
