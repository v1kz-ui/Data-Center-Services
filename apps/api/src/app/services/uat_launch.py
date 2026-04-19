from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models.enums import (
    UatAcceptanceDecision,
    UatDistributionPacketStatus,
    UatDistributionRecipientStatus,
    UatLaunchDecisionOutcome,
)
from app.db.models.uat import (
    UatAcceptanceArtifact,
    UatDistributionPacket,
    UatDistributionRecipient,
    UatHandoffSnapshot,
    UatLaunchDecisionRecord,
)
from app.services.uat_handoff import UatHandoffSnapshotNotFoundError


@dataclass(slots=True)
class UatLaunchCountRecord:
    category: str
    count: int


@dataclass(slots=True)
class UatLaunchDecisionRecordEntry:
    decision_id: str
    snapshot_id: str
    decision: str
    reviewer_name: str
    reviewer_role: str | None
    reviewer_organization: str | None
    decision_notes: str | None
    recorded_by: str
    created_at: datetime
    updated_at: datetime


@dataclass(slots=True)
class UatLaunchExceptionItem:
    source_type: str
    source_id: str
    severity: str
    status: str
    summary: str
    owner_name: str | None = None


@dataclass(slots=True)
class UatLaunchPacketSummary:
    packet_id: str
    snapshot_id: str
    packet_name: str
    channel: str
    distribution_status: str
    ready_to_send: bool
    recipient_count: int
    required_recipient_count: int
    acknowledged_recipient_count: int
    pending_recipient_count: int
    distributed_at: datetime | None
    completed_at: datetime | None


@dataclass(slots=True)
class UatLaunchReadiness:
    snapshot_id: str
    cycle_id: str
    snapshot_name: str
    cycle_status: str
    approval_ready: bool
    distribution_summary: str
    recommended_outcome: str
    blocking_exception_count: int
    attention_exception_count: int
    distribution_packet_count: int
    completed_packet_count: int
    required_recipient_count: int
    acknowledged_required_recipient_count: int
    acceptance_decision_counts: list[UatLaunchCountRecord]
    launch_decision_counts: list[UatLaunchCountRecord]
    exception_queue: list[UatLaunchExceptionItem] = field(default_factory=list)
    decision_records: list[UatLaunchDecisionRecordEntry] = field(default_factory=list)


@dataclass(slots=True)
class UatLaunchCloseoutReport:
    report_version: str
    export_scope: str
    exported_at: datetime
    exported_by: str
    readiness: UatLaunchReadiness
    packet_summaries: list[UatLaunchPacketSummary]


def create_uat_launch_decision_record(
    session: Session,
    *,
    snapshot_id: str,
    decision: UatLaunchDecisionOutcome,
    reviewer_name: str,
    recorded_by: str,
    reviewer_role: str | None = None,
    reviewer_organization: str | None = None,
    decision_notes: str | None = None,
) -> UatLaunchReadiness:
    snapshot = _get_snapshot(session, snapshot_id)
    record = UatLaunchDecisionRecord(
        snapshot_id=snapshot.snapshot_id,
        decision=decision.value,
        reviewer_name=reviewer_name,
        reviewer_role=reviewer_role,
        reviewer_organization=reviewer_organization,
        decision_notes=decision_notes,
        recorded_by=recorded_by,
    )
    session.add(record)
    session.commit()
    return get_uat_launch_readiness(session, snapshot_id)


def get_uat_launch_readiness(
    session: Session,
    snapshot_id: str,
) -> UatLaunchReadiness:
    snapshot = _get_snapshot(session, snapshot_id)
    readiness, _ = _build_launch_readiness(session, snapshot)
    return readiness


def build_uat_launch_closeout_report(
    session: Session,
    snapshot_id: str,
    *,
    exported_by: str,
) -> UatLaunchCloseoutReport:
    snapshot = _get_snapshot(session, snapshot_id)
    readiness, packet_summaries = _build_launch_readiness(session, snapshot)
    return UatLaunchCloseoutReport(
        report_version="phase7-launch-closeout-v1",
        export_scope="launch_closeout",
        exported_at=datetime.now(UTC),
        exported_by=exported_by,
        readiness=readiness,
        packet_summaries=packet_summaries,
    )


def _build_launch_readiness(
    session: Session,
    snapshot: UatHandoffSnapshot,
) -> tuple[UatLaunchReadiness, list[UatLaunchPacketSummary]]:
    report_payload = json.loads(snapshot.report_payload)
    approval_readiness = report_payload.get("approval_readiness", {})
    acceptance_models = _list_acceptance_artifacts(session, snapshot.snapshot_id)
    packet_models = _list_distribution_packets(session, snapshot.snapshot_id)
    decision_models = _list_launch_decisions(session, snapshot.snapshot_id)

    exception_queue: list[UatLaunchExceptionItem] = []
    packet_summaries: list[UatLaunchPacketSummary] = []
    required_recipient_count = 0
    acknowledged_required_recipient_count = 0
    completed_packet_count = 0

    for issue in approval_readiness.get("blocking_issues", []):
        exception_queue.append(
            UatLaunchExceptionItem(
                source_type="approval_blocker",
                source_id=str(snapshot.snapshot_id),
                severity="blocking",
                status="open",
                summary=str(issue),
            )
        )

    for item in approval_readiness.get("attention_items", []):
        exception_queue.append(
            UatLaunchExceptionItem(
                source_type="approval_attention",
                source_id=str(snapshot.snapshot_id),
                severity="attention",
                status="open",
                summary=str(item),
            )
        )

    if not acceptance_models:
        exception_queue.append(
            UatLaunchExceptionItem(
                source_type="stakeholder_decision",
                source_id=str(snapshot.snapshot_id),
                severity="blocking",
                status="missing",
                summary="No stakeholder acceptance artifacts have been recorded.",
            )
        )

    for artifact in acceptance_models:
        if artifact.decision == UatAcceptanceDecision.REJECTED.value:
            exception_queue.append(
                UatLaunchExceptionItem(
                    source_type="stakeholder_decision",
                    source_id=str(artifact.artifact_id),
                    severity="blocking",
                    status=artifact.decision,
                    summary=f"{artifact.stakeholder_name} rejected the handoff package.",
                    owner_name=artifact.stakeholder_name,
                )
            )
        elif artifact.decision == UatAcceptanceDecision.FOLLOW_UP_REQUIRED.value:
            exception_queue.append(
                UatLaunchExceptionItem(
                    source_type="stakeholder_decision",
                    source_id=str(artifact.artifact_id),
                    severity="attention",
                    status=artifact.decision,
                    summary=(
                        f"{artifact.stakeholder_name} requested follow-up before final launch "
                        "closeout."
                    ),
                    owner_name=artifact.stakeholder_name,
                )
            )

    if not packet_models:
        exception_queue.append(
            UatLaunchExceptionItem(
                source_type="distribution_packet",
                source_id=str(snapshot.snapshot_id),
                severity="blocking",
                status="missing",
                summary="No UAT distribution packets have been created for this snapshot.",
            )
        )

    for packet in packet_models:
        recipient_models = _list_distribution_recipients(session, packet.packet_id)
        packet_summary = _build_packet_summary(packet, recipient_models)
        packet_summaries.append(packet_summary)

        if packet.distribution_status == UatDistributionPacketStatus.COMPLETED.value:
            completed_packet_count += 1

        if not packet.ready_to_send:
            exception_queue.append(
                UatLaunchExceptionItem(
                    source_type="distribution_packet",
                    source_id=str(packet.packet_id),
                    severity="blocking",
                    status=packet.distribution_status,
                    summary=(
                        f"Packet `{packet.packet_name}` is not ready to send and still needs "
                        "distribution setup."
                    ),
                    owner_name=packet.created_by,
                )
            )

        for recipient in recipient_models:
            if recipient.required_for_ack:
                required_recipient_count += 1
                if recipient.delivery_status == UatDistributionRecipientStatus.ACKNOWLEDGED.value:
                    acknowledged_required_recipient_count += 1

            exception = _build_recipient_exception(packet, recipient)
            if exception is not None:
                exception_queue.append(exception)

    acceptance_counts = _build_count_records(
        [artifact.decision for artifact in acceptance_models],
        [
            UatAcceptanceDecision.ACCEPTED.value,
            UatAcceptanceDecision.REJECTED.value,
            UatAcceptanceDecision.FOLLOW_UP_REQUIRED.value,
        ],
    )
    decision_counts = _build_count_records(
        [decision.decision for decision in decision_models],
        [
            UatLaunchDecisionOutcome.GO.value,
            UatLaunchDecisionOutcome.CONDITIONAL_GO.value,
            UatLaunchDecisionOutcome.HOLD.value,
            UatLaunchDecisionOutcome.NO_GO.value,
        ],
    )
    blocking_exception_count = sum(1 for item in exception_queue if item.severity == "blocking")
    attention_exception_count = sum(1 for item in exception_queue if item.severity == "attention")

    readiness = UatLaunchReadiness(
        snapshot_id=str(snapshot.snapshot_id),
        cycle_id=str(snapshot.cycle_id),
        snapshot_name=snapshot.snapshot_name,
        cycle_status=snapshot.cycle_status,
        approval_ready=snapshot.approval_ready,
        distribution_summary=snapshot.distribution_summary,
        recommended_outcome=_derive_recommended_outcome(
            snapshot=snapshot,
            acceptance_counts=acceptance_counts,
            decision_counts=decision_counts,
            blocking_exception_count=blocking_exception_count,
            attention_exception_count=attention_exception_count,
        ),
        blocking_exception_count=blocking_exception_count,
        attention_exception_count=attention_exception_count,
        distribution_packet_count=len(packet_models),
        completed_packet_count=completed_packet_count,
        required_recipient_count=required_recipient_count,
        acknowledged_required_recipient_count=acknowledged_required_recipient_count,
        acceptance_decision_counts=acceptance_counts,
        launch_decision_counts=decision_counts,
        exception_queue=exception_queue,
        decision_records=[
            UatLaunchDecisionRecordEntry(
                decision_id=str(record.decision_id),
                snapshot_id=str(record.snapshot_id),
                decision=record.decision,
                reviewer_name=record.reviewer_name,
                reviewer_role=record.reviewer_role,
                reviewer_organization=record.reviewer_organization,
                decision_notes=record.decision_notes,
                recorded_by=record.recorded_by,
                created_at=record.created_at,
                updated_at=record.updated_at,
            )
            for record in decision_models
        ],
    )
    return readiness, packet_summaries


def _build_packet_summary(
    packet: UatDistributionPacket,
    recipient_models: list[UatDistributionRecipient],
) -> UatLaunchPacketSummary:
    return UatLaunchPacketSummary(
        packet_id=str(packet.packet_id),
        snapshot_id=str(packet.snapshot_id),
        packet_name=packet.packet_name,
        channel=packet.channel,
        distribution_status=packet.distribution_status,
        ready_to_send=packet.ready_to_send,
        recipient_count=len(recipient_models),
        required_recipient_count=sum(
            1 for recipient in recipient_models if recipient.required_for_ack
        ),
        acknowledged_recipient_count=sum(
            1
            for recipient in recipient_models
            if recipient.delivery_status == UatDistributionRecipientStatus.ACKNOWLEDGED.value
        ),
        pending_recipient_count=sum(
            1
            for recipient in recipient_models
            if recipient.delivery_status == UatDistributionRecipientStatus.PENDING.value
        ),
        distributed_at=packet.distributed_at,
        completed_at=packet.completed_at,
    )


def _build_recipient_exception(
    packet: UatDistributionPacket,
    recipient: UatDistributionRecipient,
) -> UatLaunchExceptionItem | None:
    if recipient.delivery_status == UatDistributionRecipientStatus.ACKNOWLEDGED.value:
        return None

    if recipient.delivery_status == UatDistributionRecipientStatus.PENDING.value:
        severity = "blocking" if recipient.required_for_ack else "attention"
        summary = (
            f"{recipient.recipient_name} has not yet been sent `{packet.packet_name}`."
        )
    elif recipient.delivery_status == UatDistributionRecipientStatus.SENT.value:
        severity = "blocking" if recipient.required_for_ack else "attention"
        summary = (
            f"{recipient.recipient_name} is still awaiting acknowledgement for "
            f"`{packet.packet_name}`."
        )
    else:
        severity = "blocking" if recipient.required_for_ack else "attention"
        summary = (
            f"{recipient.recipient_name} requested follow-up on `{packet.packet_name}` before "
            "launch closeout."
        )

    return UatLaunchExceptionItem(
        source_type="distribution_recipient",
        source_id=str(recipient.recipient_id),
        severity=severity,
        status=recipient.delivery_status,
        summary=summary,
        owner_name=recipient.recipient_name,
    )


def _build_count_records(
    values: Iterable[str],
    categories: list[str],
) -> list[UatLaunchCountRecord]:
    counts_by_category = {category: 0 for category in categories}
    for value in values:
        counts_by_category[value] = counts_by_category.get(value, 0) + 1
    return [
        UatLaunchCountRecord(category=category, count=counts_by_category.get(category, 0))
        for category in categories
    ]


def _derive_recommended_outcome(
    *,
    snapshot: UatHandoffSnapshot,
    acceptance_counts: list[UatLaunchCountRecord],
    decision_counts: list[UatLaunchCountRecord],
    blocking_exception_count: int,
    attention_exception_count: int,
) -> str:
    acceptance_lookup = {item.category: item.count for item in acceptance_counts}
    decision_lookup = {item.category: item.count for item in decision_counts}

    if (
        acceptance_lookup.get(UatAcceptanceDecision.REJECTED.value, 0) > 0
        or decision_lookup.get(UatLaunchDecisionOutcome.NO_GO.value, 0) > 0
    ):
        return UatLaunchDecisionOutcome.NO_GO.value

    if (
        blocking_exception_count > 0
        or decision_lookup.get(UatLaunchDecisionOutcome.HOLD.value, 0) > 0
    ):
        return UatLaunchDecisionOutcome.HOLD.value

    if (
        attention_exception_count > 0
        or acceptance_lookup.get(UatAcceptanceDecision.FOLLOW_UP_REQUIRED.value, 0) > 0
        or decision_lookup.get(UatLaunchDecisionOutcome.CONDITIONAL_GO.value, 0) > 0
    ):
        return UatLaunchDecisionOutcome.CONDITIONAL_GO.value

    if (
        decision_lookup.get(UatLaunchDecisionOutcome.GO.value, 0) > 0
        or snapshot.approval_ready
    ):
        return UatLaunchDecisionOutcome.GO.value

    return UatLaunchDecisionOutcome.HOLD.value


def _list_acceptance_artifacts(
    session: Session,
    snapshot_id: UUID,
) -> list[UatAcceptanceArtifact]:
    return session.scalars(
        select(UatAcceptanceArtifact)
        .where(UatAcceptanceArtifact.snapshot_id == snapshot_id)
        .order_by(UatAcceptanceArtifact.created_at.asc(), UatAcceptanceArtifact.artifact_id.asc())
    ).all()


def _list_distribution_packets(
    session: Session,
    snapshot_id: UUID,
) -> list[UatDistributionPacket]:
    return session.scalars(
        select(UatDistributionPacket)
        .where(UatDistributionPacket.snapshot_id == snapshot_id)
        .order_by(UatDistributionPacket.created_at.asc(), UatDistributionPacket.packet_id.asc())
    ).all()


def _list_distribution_recipients(
    session: Session,
    packet_id: UUID,
) -> list[UatDistributionRecipient]:
    return session.scalars(
        select(UatDistributionRecipient)
        .where(UatDistributionRecipient.packet_id == packet_id)
        .order_by(
            UatDistributionRecipient.created_at.asc(),
            UatDistributionRecipient.recipient_id.asc(),
        )
    ).all()


def _list_launch_decisions(
    session: Session,
    snapshot_id: UUID,
) -> list[UatLaunchDecisionRecord]:
    return session.scalars(
        select(UatLaunchDecisionRecord)
        .where(UatLaunchDecisionRecord.snapshot_id == snapshot_id)
        .order_by(
            UatLaunchDecisionRecord.created_at.asc(),
            UatLaunchDecisionRecord.decision_id.asc(),
        )
    ).all()


def _get_snapshot(session: Session, snapshot_id: str) -> UatHandoffSnapshot:
    snapshot = session.get(UatHandoffSnapshot, UUID(snapshot_id))
    if snapshot is None:
        raise UatHandoffSnapshotNotFoundError(
            f"UAT handoff snapshot `{snapshot_id}` was not found."
        )
    return snapshot
