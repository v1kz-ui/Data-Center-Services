from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.models.mixins import TimestampMixin


class SourceRefreshJob(TimestampMixin, Base):
    __tablename__ = "source_refresh_job"
    __table_args__ = (
        Index("ix_source_refresh_job_source_metro_started", "source_id", "metro_id", "started_at"),
        Index("ix_source_refresh_job_status", "status"),
    )

    job_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    source_id: Mapped[str] = mapped_column(
        ForeignKey("source_catalog.source_id", ondelete="RESTRICT"),
        nullable=False,
    )
    metro_id: Mapped[str] = mapped_column(
        ForeignKey("metro_catalog.metro_id", ondelete="RESTRICT"),
        nullable=False,
    )
    connector_key: Mapped[str] = mapped_column(String(128), nullable=False)
    trigger_mode: Mapped[str] = mapped_column(String(32), nullable=False, default="manual")
    actor_name: Mapped[str] = mapped_column(String(255), nullable=False, default="system")
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="running")
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    source_version: Mapped[str | None] = mapped_column(String(128))
    snapshot_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("source_snapshot.snapshot_id", ondelete="SET NULL"),
    )
    row_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    accepted_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rejected_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    checkpoint_in_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    checkpoint_out_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    checkpoint_cursor_in: Mapped[str | None] = mapped_column(Text)
    checkpoint_cursor_out: Mapped[str | None] = mapped_column(Text)
    error_message: Mapped[str | None] = mapped_column(Text)


class SourceRefreshCheckpoint(TimestampMixin, Base):
    __tablename__ = "source_refresh_checkpoint"
    __table_args__ = (
        UniqueConstraint("connector_key", name="uq_source_refresh_checkpoint_connector_key"),
    )

    checkpoint_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    source_id: Mapped[str] = mapped_column(
        ForeignKey("source_catalog.source_id", ondelete="RESTRICT"),
        nullable=False,
    )
    metro_id: Mapped[str] = mapped_column(
        ForeignKey("metro_catalog.metro_id", ondelete="RESTRICT"),
        nullable=False,
    )
    connector_key: Mapped[str] = mapped_column(String(128), nullable=False)
    source_version: Mapped[str | None] = mapped_column(String(128))
    snapshot_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("source_snapshot.snapshot_id", ondelete="SET NULL"),
    )
    checkpoint_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    checkpoint_cursor: Mapped[str | None] = mapped_column(Text)
    last_job_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("source_refresh_job.job_id", ondelete="SET NULL"),
    )
    last_status: Mapped[str | None] = mapped_column(String(32))
    last_error_message: Mapped[str | None] = mapped_column(Text)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
