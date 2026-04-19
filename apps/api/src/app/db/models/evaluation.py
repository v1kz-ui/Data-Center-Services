from __future__ import annotations

import uuid
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import (
    CheckConstraint,
    Enum,
    ForeignKey,
    Index,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
from app.db.models.enums import ParcelEvaluationStatus
from app.db.models.mixins import TimestampMixin

if TYPE_CHECKING:
    from app.db.models.territory import RawParcel


class ParcelEvaluation(TimestampMixin, Base):
    __tablename__ = "parcel_evaluations"
    __table_args__ = (
        UniqueConstraint("run_id", "parcel_id", name="uq_parcel_evaluations_run_parcel"),
        CheckConstraint(
            "(status != 'scored') OR (confidence_score IS NOT NULL)",
            name="confidence_required_when_scored",
        ),
        Index("ix_parcel_evaluations_run_status", "run_id", "status"),
    )

    evaluation_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("score_run.run_id", ondelete="CASCADE"),
        nullable=False,
    )
    parcel_id: Mapped[str] = mapped_column(
        ForeignKey("raw_parcels.parcel_id", ondelete="CASCADE"),
        nullable=False,
    )
    status: Mapped[ParcelEvaluationStatus] = mapped_column(
        Enum(ParcelEvaluationStatus, name="parcel_evaluation_status"),
        nullable=False,
        default=ParcelEvaluationStatus.PENDING_EXCLUSION_CHECK,
    )
    status_reason: Mapped[str | None] = mapped_column(Text)
    viability_score: Mapped[Decimal | None] = mapped_column(Numeric(5, 2))
    confidence_score: Mapped[Decimal | None] = mapped_column(Numeric(5, 2))

    parcel: Mapped[RawParcel] = relationship(back_populates="evaluations")
    exclusion_events: Mapped[list[ParcelExclusionEvent]] = relationship(
        back_populates="evaluation",
        cascade="all, delete-orphan",
    )


class ParcelExclusionEvent(TimestampMixin, Base):
    __tablename__ = "parcel_exclusion_events"
    __table_args__ = (
        Index("ix_parcel_exclusion_events_run_parcel", "run_id", "parcel_id"),
    )

    exclusion_event_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("score_run.run_id", ondelete="CASCADE"),
        nullable=False,
    )
    parcel_id: Mapped[str] = mapped_column(
        ForeignKey("raw_parcels.parcel_id", ondelete="CASCADE"),
        nullable=False,
    )
    evaluation_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("parcel_evaluations.evaluation_id", ondelete="SET NULL"),
    )
    exclusion_code: Mapped[str] = mapped_column(String(64), nullable=False)
    exclusion_reason: Mapped[str] = mapped_column(Text, nullable=False)
    rule_version: Mapped[str | None] = mapped_column(String(64))

    evaluation: Mapped[ParcelEvaluation | None] = relationship(back_populates="exclusion_events")
