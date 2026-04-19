import uuid
from decimal import Decimal

from sqlalchemy import (
    Boolean,
    CheckConstraint,
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


class ScoreFactorDetail(TimestampMixin, Base):
    __tablename__ = "score_factor_detail"
    __table_args__ = (
        UniqueConstraint(
            "run_id",
            "parcel_id",
            "factor_id",
            name="uq_score_factor_detail_run_parcel_factor",
        ),
        CheckConstraint("points_awarded >= 0", name="factor_points_nonnegative"),
        Index("ix_score_factor_detail_run_parcel", "run_id", "parcel_id"),
    )

    factor_detail_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("score_run.run_id", ondelete="CASCADE"),
        nullable=False,
    )
    parcel_id: Mapped[str] = mapped_column(
        ForeignKey("raw_parcels.parcel_id", ondelete="CASCADE"),
        nullable=False,
    )
    factor_id: Mapped[str] = mapped_column(
        ForeignKey("factor_catalog.factor_id", ondelete="RESTRICT"),
        nullable=False,
    )
    points_awarded: Mapped[Decimal] = mapped_column(Numeric(5, 2), nullable=False, default=0)
    rationale: Mapped[str | None] = mapped_column(Text)


class ScoreFactorInput(TimestampMixin, Base):
    __tablename__ = "score_factor_input"
    __table_args__ = (
        UniqueConstraint(
            "run_id",
            "parcel_id",
            "factor_id",
            "input_name",
            name="uq_score_factor_input_run_parcel_factor_input",
        ),
        Index("ix_score_factor_input_run_parcel_factor", "run_id", "parcel_id", "factor_id"),
    )

    factor_input_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("score_run.run_id", ondelete="CASCADE"),
        nullable=False,
    )
    parcel_id: Mapped[str] = mapped_column(
        ForeignKey("raw_parcels.parcel_id", ondelete="CASCADE"),
        nullable=False,
    )
    factor_id: Mapped[str] = mapped_column(
        ForeignKey("factor_catalog.factor_id", ondelete="RESTRICT"),
        nullable=False,
    )
    input_name: Mapped[str] = mapped_column(String(128), nullable=False)
    input_value: Mapped[str] = mapped_column(Text, nullable=False)
    evidence_quality: Mapped[str] = mapped_column(String(32), nullable=False)


class ScoreBonusDetail(TimestampMixin, Base):
    __tablename__ = "score_bonus_detail"
    __table_args__ = (
        UniqueConstraint(
            "run_id",
            "parcel_id",
            "bonus_id",
            name="uq_score_bonus_detail_run_parcel_bonus",
        ),
        CheckConstraint("points_awarded >= 0", name="bonus_points_nonnegative"),
        Index("ix_score_bonus_detail_run_parcel", "run_id", "parcel_id"),
    )

    bonus_detail_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("score_run.run_id", ondelete="CASCADE"),
        nullable=False,
    )
    parcel_id: Mapped[str] = mapped_column(
        ForeignKey("raw_parcels.parcel_id", ondelete="CASCADE"),
        nullable=False,
    )
    bonus_id: Mapped[str] = mapped_column(
        ForeignKey("bonus_catalog.bonus_id", ondelete="RESTRICT"),
        nullable=False,
    )
    applied: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    points_awarded: Mapped[Decimal] = mapped_column(Numeric(5, 2), nullable=False, default=0)
    rationale: Mapped[str | None] = mapped_column(Text)
