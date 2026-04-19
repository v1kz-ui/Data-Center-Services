import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Enum, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
from app.db.models.enums import ScoringProfileStatus
from app.db.models.mixins import TimestampMixin


class SourceCatalog(TimestampMixin, Base):
    __tablename__ = "source_catalog"

    source_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    owner_name: Mapped[str] = mapped_column(String(255), nullable=False)
    refresh_cadence: Mapped[str] = mapped_column(String(64), nullable=False)
    block_refresh: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    metro_coverage: Mapped[str | None] = mapped_column(Text)
    target_table_name: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    interfaces: Mapped[list["SourceInterface"]] = relationship(back_populates="source")


class SourceInterface(TimestampMixin, Base):
    __tablename__ = "source_interface"
    __table_args__ = (
        UniqueConstraint("source_id", "interface_name", name="uq_source_interface_name"),
    )

    interface_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    source_id: Mapped[str] = mapped_column(
        ForeignKey("source_catalog.source_id", ondelete="CASCADE"),
        nullable=False,
    )
    interface_name: Mapped[str] = mapped_column(String(128), nullable=False)
    schema_version: Mapped[str] = mapped_column(String(64), nullable=False)
    load_mode: Mapped[str] = mapped_column(String(32), nullable=False, default="full")
    validation_notes: Mapped[str | None] = mapped_column(Text)

    source: Mapped["SourceCatalog"] = relationship(back_populates="interfaces")


class FactorCatalog(TimestampMixin, Base):
    __tablename__ = "factor_catalog"

    factor_id: Mapped[str] = mapped_column(String(16), primary_key=True)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class BonusCatalog(TimestampMixin, Base):
    __tablename__ = "bonus_catalog"

    bonus_id: Mapped[str] = mapped_column(String(16), primary_key=True)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    max_points: Mapped[int] = mapped_column(Integer, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class ScoringProfile(TimestampMixin, Base):
    __tablename__ = "scoring_profile"

    profile_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    profile_name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    version_label: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[ScoringProfileStatus] = mapped_column(
        Enum(ScoringProfileStatus, name="scoring_profile_status"),
        nullable=False,
        default=ScoringProfileStatus.DRAFT,
    )
    effective_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    effective_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    factors: Mapped[list["ScoringProfileFactor"]] = relationship(back_populates="profile")


class ScoringProfileFactor(TimestampMixin, Base):
    __tablename__ = "scoring_profile_factor"
    __table_args__ = (
        UniqueConstraint("profile_id", "factor_id", name="uq_profile_factor"),
        UniqueConstraint("profile_id", "ordinal", name="uq_profile_factor_ordinal"),
    )

    profile_factor_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    profile_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("scoring_profile.profile_id", ondelete="CASCADE"),
        nullable=False,
    )
    factor_id: Mapped[str] = mapped_column(
        ForeignKey("factor_catalog.factor_id", ondelete="RESTRICT"),
        nullable=False,
    )
    max_points: Mapped[int] = mapped_column(Integer, nullable=False)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)

    profile: Mapped["ScoringProfile"] = relationship(back_populates="factors")

