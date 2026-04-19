import uuid
from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
from app.db.models.enums import ScoreBatchStatus, ScoreRunStatus
from app.db.models.mixins import TimestampMixin


class ScoreBatch(TimestampMixin, Base):
    __tablename__ = "score_batch"
    __table_args__ = (
        CheckConstraint(
            "expected_metros >= 0",
            name="expected_metros_nonnegative",
        ),
        CheckConstraint(
            "completed_metros >= 0",
            name="completed_metros_nonnegative",
        ),
        CheckConstraint(
            "completed_metros <= expected_metros",
            name="completed_metros_le_expected_metros",
        ),
    )

    batch_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    status: Mapped[ScoreBatchStatus] = mapped_column(
        Enum(ScoreBatchStatus, name="score_batch_status"),
        nullable=False,
        default=ScoreBatchStatus.BUILDING,
    )
    expected_metros: Mapped[int] = mapped_column(Integer, nullable=False)
    completed_metros: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    activated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    runs: Mapped[list["ScoreRun"]] = relationship(
        back_populates="batch",
        cascade="all, delete-orphan",
    )


class ScoreRun(TimestampMixin, Base):
    __tablename__ = "score_run"
    __table_args__ = (
        UniqueConstraint("batch_id", "metro_id", name="uq_score_run_batch_metro"),
        Index("ix_score_run_batch_metro_status", "batch_id", "metro_id", "status"),
    )

    run_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    batch_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("score_batch.batch_id", ondelete="CASCADE"),
        nullable=False,
    )
    metro_id: Mapped[str] = mapped_column(String(64), nullable=False)
    profile_name: Mapped[str | None] = mapped_column(String(255))
    status: Mapped[ScoreRunStatus] = mapped_column(
        Enum(ScoreRunStatus, name="score_run_status"),
        nullable=False,
        default=ScoreRunStatus.RUNNING,
    )
    failure_reason: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    batch: Mapped[ScoreBatch] = relationship(back_populates="runs")
