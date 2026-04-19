from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
from app.db.models.enums import (
    UatAcceptanceDecision,
    UatCycleStatus,
    UatDefectSeverity,
    UatDefectStatus,
    UatDistributionChannel,
    UatDistributionPacketStatus,
    UatDistributionRecipientStatus,
    UatExecutionStatus,
    UatLaunchDecisionOutcome,
    UatReleaseArchiveExportHandoffStatus,
)
from app.db.models.mixins import TimestampMixin


class UatCycle(TimestampMixin, Base):
    __tablename__ = "uat_cycle"
    __table_args__ = (
        UniqueConstraint("cycle_name", name="uq_uat_cycle_cycle_name"),
        Index("ix_uat_cycle_status", "status"),
    )

    cycle_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    cycle_name: Mapped[str] = mapped_column(String(255), nullable=False)
    environment_name: Mapped[str] = mapped_column(String(64), nullable=False)
    scenario_pack_path: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=UatCycleStatus.PLANNED.value,
    )
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
    summary_notes: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    scenario_executions: Mapped[list[UatScenarioExecution]] = relationship(
        back_populates="cycle",
        cascade="all, delete-orphan",
    )
    defects: Mapped[list[UatDefect]] = relationship(
        back_populates="cycle",
        cascade="all, delete-orphan",
    )
    events: Mapped[list[UatCycleEvent]] = relationship(
        back_populates="cycle",
        cascade="all, delete-orphan",
    )
    handoff_snapshots: Mapped[list[UatHandoffSnapshot]] = relationship(
        back_populates="cycle",
        cascade="all, delete-orphan",
    )


class UatScenarioExecution(TimestampMixin, Base):
    __tablename__ = "uat_scenario_execution"
    __table_args__ = (
        UniqueConstraint(
            "cycle_id",
            "scenario_id",
            name="uq_uat_scenario_execution_cycle_scenario",
        ),
        Index("ix_uat_scenario_execution_cycle_id", "cycle_id"),
        Index("ix_uat_scenario_execution_status", "status"),
    )

    execution_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    cycle_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("uat_cycle.cycle_id", ondelete="CASCADE"),
        nullable=False,
    )
    scenario_id: Mapped[str] = mapped_column(String(64), nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    actor_role: Mapped[str] = mapped_column(String(32), nullable=False)
    workflow: Mapped[str] = mapped_column(String(255), nullable=False)
    entrypoint: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=UatExecutionStatus.PLANNED.value,
    )
    execution_notes: Mapped[str | None] = mapped_column(Text)
    evidence_reference: Mapped[str | None] = mapped_column(Text)
    executed_by: Mapped[str | None] = mapped_column(String(255))
    executed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    cycle: Mapped[UatCycle] = relationship(back_populates="scenario_executions")


class UatDefect(TimestampMixin, Base):
    __tablename__ = "uat_defect"
    __table_args__ = (
        Index("ix_uat_defect_cycle_id", "cycle_id"),
        Index("ix_uat_defect_severity", "severity"),
        Index("ix_uat_defect_status", "status"),
    )

    defect_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    cycle_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("uat_cycle.cycle_id", ondelete="CASCADE"),
        nullable=False,
    )
    scenario_id: Mapped[str | None] = mapped_column(String(64))
    severity: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default=UatDefectSeverity.MEDIUM.value,
    )
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=UatDefectStatus.OPEN.value,
    )
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    reported_by: Mapped[str] = mapped_column(String(255), nullable=False)
    owner_name: Mapped[str | None] = mapped_column(String(255))
    external_reference: Mapped[str | None] = mapped_column(String(255))
    resolution_notes: Mapped[str | None] = mapped_column(Text)

    cycle: Mapped[UatCycle] = relationship(back_populates="defects")


class UatCycleEvent(TimestampMixin, Base):
    __tablename__ = "uat_cycle_event"
    __table_args__ = (
        Index("ix_uat_cycle_event_cycle_id", "cycle_id"),
        Index("ix_uat_cycle_event_event_type", "event_type"),
    )

    event_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    cycle_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("uat_cycle.cycle_id", ondelete="CASCADE"),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    actor_name: Mapped[str] = mapped_column(String(255), nullable=False)
    scenario_id: Mapped[str | None] = mapped_column(String(64))
    defect_id: Mapped[uuid.UUID | None] = mapped_column()
    event_notes: Mapped[str | None] = mapped_column(Text)
    event_payload: Mapped[str | None] = mapped_column(Text)

    cycle: Mapped[UatCycle] = relationship(back_populates="events")


class UatHandoffSnapshot(TimestampMixin, Base):
    __tablename__ = "uat_handoff_snapshot"
    __table_args__ = (
        UniqueConstraint(
            "cycle_id",
            "snapshot_name",
            name="uq_uat_handoff_snapshot_cycle_snapshot_name",
        ),
        Index("ix_uat_handoff_snapshot_cycle_id", "cycle_id"),
        Index("ix_uat_handoff_snapshot_approval_ready", "approval_ready"),
    )

    snapshot_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    cycle_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("uat_cycle.cycle_id", ondelete="CASCADE"),
        nullable=False,
    )
    snapshot_name: Mapped[str] = mapped_column(String(255), nullable=False)
    report_version: Mapped[str] = mapped_column(String(64), nullable=False)
    export_scope: Mapped[str] = mapped_column(String(32), nullable=False)
    cycle_status: Mapped[str] = mapped_column(String(32), nullable=False)
    approval_ready: Mapped[bool] = mapped_column(Boolean, nullable=False)
    blocking_issue_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    open_defect_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    open_high_severity_defect_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
    )
    distribution_summary: Mapped[str] = mapped_column(Text, nullable=False)
    report_payload: Mapped[str] = mapped_column(Text, nullable=False)
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)

    cycle: Mapped[UatCycle] = relationship(back_populates="handoff_snapshots")
    acceptance_artifacts: Mapped[list[UatAcceptanceArtifact]] = relationship(
        back_populates="snapshot",
        cascade="all, delete-orphan",
    )
    distribution_packets: Mapped[list[UatDistributionPacket]] = relationship(
        back_populates="snapshot",
        cascade="all, delete-orphan",
    )
    launch_decisions: Mapped[list[UatLaunchDecisionRecord]] = relationship(
        back_populates="snapshot",
        cascade="all, delete-orphan",
    )
    release_archives: Mapped[list[UatReleaseArchive]] = relationship(
        back_populates="snapshot",
        cascade="all, delete-orphan",
    )


class UatAcceptanceArtifact(TimestampMixin, Base):
    __tablename__ = "uat_acceptance_artifact"
    __table_args__ = (
        Index("ix_uat_acceptance_artifact_snapshot_id", "snapshot_id"),
        Index("ix_uat_acceptance_artifact_decision", "decision"),
    )

    artifact_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    snapshot_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("uat_handoff_snapshot.snapshot_id", ondelete="CASCADE"),
        nullable=False,
    )
    decision: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=UatAcceptanceDecision.FOLLOW_UP_REQUIRED.value,
    )
    stakeholder_name: Mapped[str] = mapped_column(String(255), nullable=False)
    stakeholder_role: Mapped[str | None] = mapped_column(String(255))
    stakeholder_organization: Mapped[str | None] = mapped_column(String(255))
    decision_notes: Mapped[str | None] = mapped_column(Text)
    recorded_by: Mapped[str] = mapped_column(String(255), nullable=False)

    snapshot: Mapped[UatHandoffSnapshot] = relationship(back_populates="acceptance_artifacts")


class UatDistributionPacket(TimestampMixin, Base):
    __tablename__ = "uat_distribution_packet"
    __table_args__ = (
        UniqueConstraint(
            "snapshot_id",
            "packet_name",
            name="uq_uat_distribution_packet_snapshot_packet_name",
        ),
        Index("ix_uat_distribution_packet_snapshot_id", "snapshot_id"),
        Index("ix_uat_distribution_packet_distribution_status", "distribution_status"),
        Index("ix_uat_distribution_packet_ready_to_send", "ready_to_send"),
    )

    packet_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    snapshot_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("uat_handoff_snapshot.snapshot_id", ondelete="CASCADE"),
        nullable=False,
    )
    packet_name: Mapped[str] = mapped_column(String(255), nullable=False)
    channel: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        default=UatDistributionChannel.STAKEHOLDER_BRIEFING.value,
    )
    distribution_status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=UatDistributionPacketStatus.DRAFT.value,
    )
    ready_to_send: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    subject_line: Mapped[str] = mapped_column(String(255), nullable=False)
    summary_excerpt: Mapped[str] = mapped_column(Text, nullable=False)
    briefing_body: Mapped[str] = mapped_column(Text, nullable=False)
    distribution_notes: Mapped[str | None] = mapped_column(Text)
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
    distributed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    snapshot: Mapped[UatHandoffSnapshot] = relationship(back_populates="distribution_packets")
    recipients: Mapped[list[UatDistributionRecipient]] = relationship(
        back_populates="packet",
        cascade="all, delete-orphan",
    )


class UatDistributionRecipient(TimestampMixin, Base):
    __tablename__ = "uat_distribution_recipient"
    __table_args__ = (
        Index("ix_uat_distribution_recipient_packet_id", "packet_id"),
        Index("ix_uat_distribution_recipient_delivery_status", "delivery_status"),
        Index("ix_uat_distribution_recipient_required_for_ack", "required_for_ack"),
    )

    recipient_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    packet_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("uat_distribution_packet.packet_id", ondelete="CASCADE"),
        nullable=False,
    )
    recipient_name: Mapped[str] = mapped_column(String(255), nullable=False)
    recipient_role: Mapped[str | None] = mapped_column(String(255))
    recipient_organization: Mapped[str | None] = mapped_column(String(255))
    recipient_contact: Mapped[str | None] = mapped_column(String(255))
    required_for_ack: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    delivery_status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=UatDistributionRecipientStatus.PENDING.value,
    )
    delivery_notes: Mapped[str | None] = mapped_column(Text)
    acknowledgement_notes: Mapped[str | None] = mapped_column(Text)
    acknowledged_by: Mapped[str | None] = mapped_column(String(255))
    recorded_by: Mapped[str] = mapped_column(String(255), nullable=False)
    last_status_updated_by: Mapped[str] = mapped_column(String(255), nullable=False)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    packet: Mapped[UatDistributionPacket] = relationship(back_populates="recipients")


class UatLaunchDecisionRecord(TimestampMixin, Base):
    __tablename__ = "uat_launch_decision_record"
    __table_args__ = (
        Index("ix_uat_launch_decision_record_snapshot_id", "snapshot_id"),
        Index("ix_uat_launch_decision_record_decision", "decision"),
    )

    decision_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    snapshot_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("uat_handoff_snapshot.snapshot_id", ondelete="CASCADE"),
        nullable=False,
    )
    decision: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=UatLaunchDecisionOutcome.HOLD.value,
    )
    reviewer_name: Mapped[str] = mapped_column(String(255), nullable=False)
    reviewer_role: Mapped[str | None] = mapped_column(String(255))
    reviewer_organization: Mapped[str | None] = mapped_column(String(255))
    decision_notes: Mapped[str | None] = mapped_column(Text)
    recorded_by: Mapped[str] = mapped_column(String(255), nullable=False)

    snapshot: Mapped[UatHandoffSnapshot] = relationship(back_populates="launch_decisions")


class UatReleaseArchive(TimestampMixin, Base):
    __tablename__ = "uat_release_archive"
    __table_args__ = (
        UniqueConstraint(
            "snapshot_id",
            "archive_name",
            name="uq_uat_release_archive_snapshot_archive_name",
        ),
        Index("ix_uat_release_archive_snapshot_id", "snapshot_id"),
        Index("ix_uat_release_archive_recommended_outcome", "recommended_outcome"),
        Index("ix_uat_release_archive_retention_review_at", "retention_review_at"),
        Index("ix_uat_release_archive_superseded_by_archive_id", "superseded_by_archive_id"),
    )

    archive_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    snapshot_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("uat_handoff_snapshot.snapshot_id", ondelete="CASCADE"),
        nullable=False,
    )
    archive_name: Mapped[str] = mapped_column(String(255), nullable=False)
    recommended_outcome: Mapped[str] = mapped_column(String(32), nullable=False)
    blocking_exception_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    attention_exception_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    support_handoff_owner: Mapped[str | None] = mapped_column(String(255))
    support_handoff_summary: Mapped[str] = mapped_column(Text, nullable=False)
    operations_runbook_reference: Mapped[str | None] = mapped_column(String(255))
    archive_summary: Mapped[str] = mapped_column(Text, nullable=False)
    manifest_payload: Mapped[str] = mapped_column(Text, nullable=False)
    archive_checksum: Mapped[str] = mapped_column(String(64), nullable=False)
    retention_review_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    superseded_by_archive_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("uat_release_archive.archive_id", ondelete="SET NULL")
    )
    supersession_reason: Mapped[str | None] = mapped_column(Text)
    superseded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
    sealed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    snapshot: Mapped[UatHandoffSnapshot] = relationship(back_populates="release_archives")
    evidence_items: Mapped[list[UatReleaseArchiveEvidenceItem]] = relationship(
        back_populates="archive",
        cascade="all, delete-orphan",
    )
    export_records: Mapped[list[UatReleaseArchiveExport]] = relationship(
        back_populates="archive",
        cascade="all, delete-orphan",
    )
    delivery_events: Mapped[list[UatReleaseArchiveExportDeliveryEvent]] = relationship(
        back_populates="archive",
        cascade="all, delete-orphan",
    )
    retention_actions: Mapped[list[UatReleaseArchiveRetentionAction]] = relationship(
        back_populates="archive",
        cascade="all, delete-orphan",
    )


class UatReleaseArchiveEvidenceItem(TimestampMixin, Base):
    __tablename__ = "uat_release_archive_evidence_item"
    __table_args__ = (
        Index("ix_uat_release_archive_evidence_item_archive_id", "archive_id"),
        Index("ix_uat_release_archive_evidence_item_evidence_type", "evidence_type"),
        Index("ix_uat_release_archive_evidence_item_retention_label", "retention_label"),
    )

    evidence_item_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    archive_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("uat_release_archive.archive_id", ondelete="CASCADE"),
        nullable=False,
    )
    evidence_type: Mapped[str] = mapped_column(String(64), nullable=False)
    evidence_status: Mapped[str] = mapped_column(String(32), nullable=False)
    reference_id: Mapped[str] = mapped_column(String(255), nullable=False)
    reference_name: Mapped[str] = mapped_column(String(255), nullable=False)
    retention_label: Mapped[str] = mapped_column(String(64), nullable=False)
    evidence_summary: Mapped[str] = mapped_column(Text, nullable=False)
    source_recorded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source_location: Mapped[str | None] = mapped_column(String(255))

    archive: Mapped[UatReleaseArchive] = relationship(back_populates="evidence_items")


class UatReleaseArchiveExport(TimestampMixin, Base):
    __tablename__ = "uat_release_archive_export"
    __table_args__ = (
        UniqueConstraint(
            "archive_id",
            "export_name",
            name="uq_uat_release_archive_export_archive_export_name",
        ),
        Index("ix_uat_release_archive_export_archive_id", "archive_id"),
        Index("ix_uat_release_archive_export_export_scope", "export_scope"),
        Index("ix_uat_release_archive_export_destination_system", "destination_system"),
        Index("ix_uat_release_archive_export_handoff_status", "handoff_status"),
    )

    export_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    archive_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("uat_release_archive.archive_id", ondelete="CASCADE"),
        nullable=False,
    )
    export_name: Mapped[str] = mapped_column(String(255), nullable=False)
    export_scope: Mapped[str] = mapped_column(String(64), nullable=False)
    destination_system: Mapped[str] = mapped_column(String(128), nullable=False)
    destination_reference: Mapped[str | None] = mapped_column(String(255))
    handoff_status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=UatReleaseArchiveExportHandoffStatus.PREPARED.value,
    )
    trigger_reason: Mapped[str | None] = mapped_column(Text)
    handoff_notes: Mapped[str | None] = mapped_column(Text)
    export_payload: Mapped[str] = mapped_column(Text, nullable=False)
    export_checksum: Mapped[str] = mapped_column(String(64), nullable=False)
    exported_by: Mapped[str] = mapped_column(String(255), nullable=False)
    exported_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    delivery_confirmed_by: Mapped[str | None] = mapped_column(String(255))
    delivery_confirmed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_status_updated_by: Mapped[str | None] = mapped_column(String(255))

    archive: Mapped[UatReleaseArchive] = relationship(back_populates="export_records")
    delivery_events: Mapped[list[UatReleaseArchiveExportDeliveryEvent]] = relationship(
        back_populates="export",
        cascade="all, delete-orphan",
    )
    retention_actions: Mapped[list[UatReleaseArchiveRetentionAction]] = relationship(
        back_populates="related_export"
    )


class UatReleaseArchiveExportDeliveryEvent(TimestampMixin, Base):
    __tablename__ = "uat_release_archive_export_delivery_event"
    __table_args__ = (
        Index("ix_uat_release_archive_export_delivery_event_archive_id", "archive_id"),
        Index("ix_uat_release_archive_export_delivery_event_export_id", "export_id"),
        Index("ix_uat_release_archive_export_delivery_event_event_type", "event_type"),
        Index("ix_uat_release_archive_export_delivery_event_occurred_at", "occurred_at"),
    )

    event_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    archive_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("uat_release_archive.archive_id", ondelete="CASCADE"),
        nullable=False,
    )
    export_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("uat_release_archive_export.export_id", ondelete="CASCADE"),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    target_name: Mapped[str] = mapped_column(String(255), nullable=False)
    delivery_channel: Mapped[str | None] = mapped_column(String(64))
    external_reference: Mapped[str | None] = mapped_column(String(255))
    event_notes: Mapped[str | None] = mapped_column(Text)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    recorded_by: Mapped[str] = mapped_column(String(255), nullable=False)

    archive: Mapped[UatReleaseArchive] = relationship(back_populates="delivery_events")
    export: Mapped[UatReleaseArchiveExport] = relationship(back_populates="delivery_events")


class UatReleaseArchiveRetentionAction(TimestampMixin, Base):
    __tablename__ = "uat_release_archive_retention_action"
    __table_args__ = (
        Index("ix_uat_release_archive_retention_action_archive_id", "archive_id"),
        Index("ix_uat_release_archive_retention_action_action_type", "action_type"),
        Index(
            "ix_uat_release_archive_retention_action_related_export_id",
            "related_export_id",
        ),
    )

    action_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    archive_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("uat_release_archive.archive_id", ondelete="CASCADE"),
        nullable=False,
    )
    related_export_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("uat_release_archive_export.export_id", ondelete="SET NULL")
    )
    action_type: Mapped[str] = mapped_column(String(64), nullable=False)
    previous_retention_review_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    next_retention_review_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    scheduled_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    action_notes: Mapped[str | None] = mapped_column(Text)
    recorded_by: Mapped[str] = mapped_column(String(255), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    archive: Mapped[UatReleaseArchive] = relationship(back_populates="retention_actions")
    related_export: Mapped[UatReleaseArchiveExport | None] = relationship(
        back_populates="retention_actions"
    )
