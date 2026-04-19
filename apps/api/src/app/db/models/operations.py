from __future__ import annotations

import uuid

from sqlalchemy import ForeignKey, Index, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.models.mixins import TimestampMixin


class OperatorActionEvent(TimestampMixin, Base):
    __tablename__ = "operator_action_event"
    __table_args__ = (
        Index("ix_operator_action_event_batch_id", "batch_id"),
        Index("ix_operator_action_event_run_id", "run_id"),
        Index("ix_operator_action_event_action_type", "action_type"),
    )

    action_event_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    action_type: Mapped[str] = mapped_column(String(64), nullable=False)
    target_type: Mapped[str] = mapped_column(String(32), nullable=False)
    target_id: Mapped[str] = mapped_column(String(64), nullable=False)
    batch_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("score_batch.batch_id", ondelete="SET NULL"),
    )
    run_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("score_run.run_id", ondelete="SET NULL"),
    )
    actor_name: Mapped[str] = mapped_column(String(255), nullable=False, default="operator")
    action_reason: Mapped[str | None] = mapped_column(Text)
    action_payload: Mapped[str | None] = mapped_column(Text)
