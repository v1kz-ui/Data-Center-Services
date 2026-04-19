from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
from app.db.models.mixins import TimestampMixin

if TYPE_CHECKING:
    from app.db.models.ingestion import SourceSnapshot


class SourceRecordRejection(TimestampMixin, Base):
    __tablename__ = "source_record_rejection"
    __table_args__ = (
        Index("ix_source_record_rejection_snapshot_row", "snapshot_id", "row_number"),
    )

    rejection_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    snapshot_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("source_snapshot.snapshot_id", ondelete="CASCADE"),
        nullable=False,
    )
    row_number: Mapped[int] = mapped_column(Integer, nullable=False)
    external_key: Mapped[str | None] = mapped_column(String(255))
    rejection_code: Mapped[str] = mapped_column(String(64), nullable=False)
    rejection_message: Mapped[str] = mapped_column(Text, nullable=False)
    raw_payload: Mapped[str | None] = mapped_column(Text)

    snapshot: Mapped[SourceSnapshot] = relationship(back_populates="rejections")


class RawZoning(TimestampMixin, Base):
    __tablename__ = "raw_zoning"
    __table_args__ = (
        UniqueConstraint("parcel_id", "source_snapshot_id", name="uq_raw_zoning_parcel_snapshot"),
        Index("ix_raw_zoning_metro_parcel", "metro_id", "parcel_id"),
    )

    zoning_record_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    parcel_id: Mapped[str] = mapped_column(
        ForeignKey("raw_parcels.parcel_id", ondelete="CASCADE"),
        nullable=False,
    )
    county_fips: Mapped[str] = mapped_column(
        ForeignKey("county_catalog.county_fips", ondelete="RESTRICT"),
        nullable=False,
    )
    metro_id: Mapped[str] = mapped_column(
        ForeignKey("metro_catalog.metro_id", ondelete="RESTRICT"),
        nullable=False,
    )
    zoning_code: Mapped[str] = mapped_column(String(128), nullable=False)
    land_use_code: Mapped[str | None] = mapped_column(String(128))
    source_snapshot_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("source_snapshot.snapshot_id", ondelete="SET NULL"),
    )
    lineage_key: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class SourceEvidence(TimestampMixin, Base):
    __tablename__ = "source_evidence"
    __table_args__ = (
        UniqueConstraint(
            "source_snapshot_id",
            "record_key",
            "attribute_name",
            name="uq_source_evidence_snapshot_record_attribute",
        ),
        Index("ix_source_evidence_source_metro", "source_id", "metro_id"),
        Index("ix_source_evidence_parcel_id", "parcel_id"),
    )

    evidence_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    source_id: Mapped[str] = mapped_column(
        ForeignKey("source_catalog.source_id", ondelete="RESTRICT"),
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
    record_key: Mapped[str] = mapped_column(String(255), nullable=False)
    attribute_name: Mapped[str] = mapped_column(String(128), nullable=False)
    attribute_value: Mapped[str] = mapped_column(Text, nullable=False)
    lineage_key: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
