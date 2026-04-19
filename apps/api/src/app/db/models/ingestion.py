from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import CheckConstraint, DateTime, Enum, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
from app.db.models.enums import SourceSnapshotStatus
from app.db.models.mixins import TimestampMixin

if TYPE_CHECKING:
    from app.db.models.catalogs import SourceCatalog
    from app.db.models.source_data import SourceRecordRejection
    from app.db.models.territory import MetroCatalog


class SourceSnapshot(TimestampMixin, Base):
    __tablename__ = "source_snapshot"
    __table_args__ = (
        CheckConstraint("row_count >= 0", name="row_count_nonnegative"),
        Index("ix_source_snapshot_source_metro_ts", "source_id", "metro_id", "snapshot_ts"),
        Index(
            "ix_source_snapshot_connector_scope_ts",
            "connector_key",
            "snapshot_ts",
        ),
    )

    snapshot_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    source_id: Mapped[str] = mapped_column(
        ForeignKey("source_catalog.source_id", ondelete="RESTRICT"),
        nullable=False,
    )
    metro_id: Mapped[str] = mapped_column(
        ForeignKey("metro_catalog.metro_id", ondelete="RESTRICT"),
        nullable=False,
    )
    connector_key: Mapped[str | None] = mapped_column(String(128))
    snapshot_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source_version: Mapped[str] = mapped_column(String(128), nullable=False)
    row_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    checksum: Mapped[str | None] = mapped_column(String(128))
    status: Mapped[SourceSnapshotStatus] = mapped_column(
        Enum(SourceSnapshotStatus, name="source_snapshot_status"),
        nullable=False,
        default=SourceSnapshotStatus.SUCCESS,
    )
    error_message: Mapped[str | None] = mapped_column(Text)

    source: Mapped[SourceCatalog] = relationship()
    metro: Mapped[MetroCatalog] = relationship(back_populates="source_snapshots")
    rejections: Mapped[list[SourceRecordRejection]] = relationship(
        back_populates="snapshot",
        cascade="all, delete-orphan",
    )
