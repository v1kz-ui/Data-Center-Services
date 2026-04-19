from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db.models.enums import (
    UatDistributionChannel,
    UatDistributionPacketStatus,
    UatDistributionRecipientStatus,
)
from app.db.models.uat import (
    UatDistributionPacket,
    UatDistributionRecipient,
    UatHandoffSnapshot,
)
from app.services.uat_handoff import UatHandoffSnapshotNotFoundError


class UatDistributionPacketNotFoundError(LookupError):
    """Raised when a UAT distribution packet cannot be found."""


class UatDistributionPacketConflictError(ValueError):
    """Raised when a UAT distribution packet conflicts with existing state."""


class UatDistributionRecipientNotFoundError(LookupError):
    """Raised when a UAT distribution recipient cannot be found."""


@dataclass(slots=True)
class UatDistributionRecipientSeed:
    recipient_name: str
    recipient_role: str | None = None
    recipient_organization: str | None = None
    recipient_contact: str | None = None
    required_for_ack: bool = True


@dataclass(slots=True)
class UatDistributionRecipientRecord:
    recipient_id: str
    packet_id: str
    recipient_name: str
    recipient_role: str | None
    recipient_organization: str | None
    recipient_contact: str | None
    required_for_ack: bool
    delivery_status: str
    delivery_notes: str | None
    acknowledgement_notes: str | None
    acknowledged_by: str | None
    recorded_by: str
    last_status_updated_by: str
    delivered_at: datetime | None
    acknowledged_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(slots=True)
class UatDistributionPacketSummary:
    packet_id: str
    snapshot_id: str
    packet_name: str
    channel: str
    distribution_status: str
    ready_to_send: bool
    subject_line: str
    summary_excerpt: str
    distribution_notes: str | None
    created_by: str
    created_at: datetime
    updated_at: datetime
    distributed_at: datetime | None
    completed_at: datetime | None
    recipient_count: int
    required_recipient_count: int
    acknowledged_recipient_count: int
    pending_recipient_count: int


@dataclass(slots=True)
class UatDistributionPacketDetail(UatDistributionPacketSummary):
    briefing_body: str
    recipients: list[UatDistributionRecipientRecord] = field(default_factory=list)


def create_uat_distribution_packet(
    session: Session,
    *,
    snapshot_id: str,
    packet_name: str,
    channel: UatDistributionChannel,
    created_by: str,
    subject_line_override: str | None = None,
    summary_excerpt_override: str | None = None,
    briefing_body_override: str | None = None,
    distribution_notes: str | None = None,
    recipients: Sequence[UatDistributionRecipientSeed] | None = None,
) -> UatDistributionPacketDetail:
    snapshot = _get_snapshot(session, snapshot_id)
    report_payload = json.loads(snapshot.report_payload)
    packet = UatDistributionPacket(
        snapshot_id=snapshot.snapshot_id,
        packet_name=packet_name,
        channel=channel.value,
        subject_line=subject_line_override
        or _build_subject_line(snapshot, report_payload, channel),
        summary_excerpt=summary_excerpt_override or snapshot.distribution_summary,
        briefing_body=briefing_body_override
        or _build_briefing_body(snapshot, report_payload, channel),
        distribution_notes=distribution_notes,
        created_by=created_by,
    )
    session.add(packet)

    for recipient in recipients or []:
        packet.recipients.append(_build_distribution_recipient(recipient, actor_name=created_by))

    _reconcile_packet_state(packet)

    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        raise UatDistributionPacketConflictError(
            f"UAT distribution packet `{packet_name}` already exists for this snapshot."
        ) from exc

    return get_uat_distribution_packet(session, str(packet.packet_id))


def list_uat_distribution_packets(
    session: Session,
    *,
    snapshot_id: str,
) -> list[UatDistributionPacketSummary]:
    snapshot = _get_snapshot(session, snapshot_id)
    packet_models = session.scalars(
        select(UatDistributionPacket)
        .where(UatDistributionPacket.snapshot_id == snapshot.snapshot_id)
        .order_by(UatDistributionPacket.created_at.desc(), UatDistributionPacket.packet_id.desc())
    ).all()
    return [_build_packet_summary(session, packet) for packet in packet_models]


def get_uat_distribution_packet(
    session: Session,
    packet_id: str,
) -> UatDistributionPacketDetail:
    packet = _get_packet(session, packet_id)
    return _build_packet_detail(session, packet)


def create_uat_distribution_recipient(
    session: Session,
    *,
    packet_id: str,
    recipient_name: str,
    recorded_by: str,
    recipient_role: str | None = None,
    recipient_organization: str | None = None,
    recipient_contact: str | None = None,
    required_for_ack: bool = True,
) -> UatDistributionPacketDetail:
    packet = _get_packet(session, packet_id)
    packet.recipients.append(
        _build_distribution_recipient(
            UatDistributionRecipientSeed(
                recipient_name=recipient_name,
                recipient_role=recipient_role,
                recipient_organization=recipient_organization,
                recipient_contact=recipient_contact,
                required_for_ack=required_for_ack,
            ),
            actor_name=recorded_by,
        )
    )
    _reconcile_packet_state(packet)
    session.commit()
    return get_uat_distribution_packet(session, str(packet.packet_id))


def update_uat_distribution_recipient(
    session: Session,
    *,
    packet_id: str,
    recipient_id: str,
    updated_by: str,
    delivery_status: UatDistributionRecipientStatus | None = None,
    delivery_notes: str | None = None,
    acknowledgement_notes: str | None = None,
    acknowledged_by: str | None = None,
) -> UatDistributionPacketDetail:
    packet = _get_packet(session, packet_id)
    recipient = _get_packet_recipient(packet, recipient_id)
    status_changed = False
    now = datetime.now(UTC)

    if delivery_status is not None:
        status_changed = True
        recipient.delivery_status = delivery_status.value
        if delivery_status == UatDistributionRecipientStatus.PENDING:
            recipient.delivered_at = None
            recipient.acknowledged_at = None
            recipient.acknowledged_by = None
        elif delivery_status == UatDistributionRecipientStatus.SENT:
            recipient.delivered_at = recipient.delivered_at or now
            recipient.acknowledged_at = None
            recipient.acknowledged_by = None
        else:
            recipient.delivered_at = recipient.delivered_at or now
            recipient.acknowledged_at = now
            recipient.acknowledged_by = acknowledged_by or recipient.recipient_name

    if delivery_notes is not None:
        recipient.delivery_notes = delivery_notes
    if acknowledgement_notes is not None:
        recipient.acknowledgement_notes = acknowledgement_notes
    if acknowledged_by is not None and recipient.delivery_status in {
        UatDistributionRecipientStatus.ACKNOWLEDGED.value,
        UatDistributionRecipientStatus.FOLLOW_UP_REQUIRED.value,
    }:
        recipient.acknowledged_by = acknowledged_by

    recipient.last_status_updated_by = updated_by
    _reconcile_packet_state(packet, now=now if status_changed else None)
    session.commit()
    return get_uat_distribution_packet(session, str(packet.packet_id))


def _build_packet_summary(
    session: Session,
    packet: UatDistributionPacket,
    *,
    recipient_models: list[UatDistributionRecipient] | None = None,
) -> UatDistributionPacketSummary:
    recipients = recipient_models or _list_packet_recipients(session, packet.packet_id)
    recipient_count = len(recipients)
    required_recipient_count = sum(1 for recipient in recipients if recipient.required_for_ack)
    acknowledged_recipient_count = sum(
        1
        for recipient in recipients
        if recipient.delivery_status == UatDistributionRecipientStatus.ACKNOWLEDGED.value
    )
    pending_recipient_count = sum(
        1
        for recipient in recipients
        if recipient.delivery_status == UatDistributionRecipientStatus.PENDING.value
    )

    return UatDistributionPacketSummary(
        packet_id=str(packet.packet_id),
        snapshot_id=str(packet.snapshot_id),
        packet_name=packet.packet_name,
        channel=packet.channel,
        distribution_status=packet.distribution_status,
        ready_to_send=packet.ready_to_send,
        subject_line=packet.subject_line,
        summary_excerpt=packet.summary_excerpt,
        distribution_notes=packet.distribution_notes,
        created_by=packet.created_by,
        created_at=packet.created_at,
        updated_at=packet.updated_at,
        distributed_at=packet.distributed_at,
        completed_at=packet.completed_at,
        recipient_count=recipient_count,
        required_recipient_count=required_recipient_count,
        acknowledged_recipient_count=acknowledged_recipient_count,
        pending_recipient_count=pending_recipient_count,
    )


def _build_packet_detail(
    session: Session,
    packet: UatDistributionPacket,
) -> UatDistributionPacketDetail:
    recipient_models = _list_packet_recipients(session, packet.packet_id)
    summary = _build_packet_summary(session, packet, recipient_models=recipient_models)
    return UatDistributionPacketDetail(
        packet_id=summary.packet_id,
        snapshot_id=summary.snapshot_id,
        packet_name=summary.packet_name,
        channel=summary.channel,
        distribution_status=summary.distribution_status,
        ready_to_send=summary.ready_to_send,
        subject_line=summary.subject_line,
        summary_excerpt=summary.summary_excerpt,
        distribution_notes=summary.distribution_notes,
        created_by=summary.created_by,
        created_at=summary.created_at,
        updated_at=summary.updated_at,
        distributed_at=summary.distributed_at,
        completed_at=summary.completed_at,
        recipient_count=summary.recipient_count,
        required_recipient_count=summary.required_recipient_count,
        acknowledged_recipient_count=summary.acknowledged_recipient_count,
        pending_recipient_count=summary.pending_recipient_count,
        briefing_body=packet.briefing_body,
        recipients=[
            UatDistributionRecipientRecord(
                recipient_id=str(recipient.recipient_id),
                packet_id=str(recipient.packet_id),
                recipient_name=recipient.recipient_name,
                recipient_role=recipient.recipient_role,
                recipient_organization=recipient.recipient_organization,
                recipient_contact=recipient.recipient_contact,
                required_for_ack=recipient.required_for_ack,
                delivery_status=recipient.delivery_status,
                delivery_notes=recipient.delivery_notes,
                acknowledgement_notes=recipient.acknowledgement_notes,
                acknowledged_by=recipient.acknowledged_by,
                recorded_by=recipient.recorded_by,
                last_status_updated_by=recipient.last_status_updated_by,
                delivered_at=recipient.delivered_at,
                acknowledged_at=recipient.acknowledged_at,
                created_at=recipient.created_at,
                updated_at=recipient.updated_at,
            )
            for recipient in recipient_models
        ],
    )


def _build_distribution_recipient(
    recipient: UatDistributionRecipientSeed,
    *,
    actor_name: str,
) -> UatDistributionRecipient:
    return UatDistributionRecipient(
        recipient_name=recipient.recipient_name,
        recipient_role=recipient.recipient_role,
        recipient_organization=recipient.recipient_organization,
        recipient_contact=recipient.recipient_contact,
        required_for_ack=recipient.required_for_ack,
        delivery_status=UatDistributionRecipientStatus.PENDING.value,
        recorded_by=actor_name,
        last_status_updated_by=actor_name,
    )


def _build_subject_line(
    snapshot: UatHandoffSnapshot,
    report_payload: dict[str, Any],
    channel: UatDistributionChannel,
) -> str:
    cycle = report_payload.get("cycle", {})
    cycle_name = str(cycle.get("cycle_name", snapshot.snapshot_name))
    channel_label = channel.value.replace("_", " ")
    return _truncate(
        f"[UAT] {cycle_name} {snapshot.snapshot_name} {channel_label}",
        255,
    )


def _build_briefing_body(
    snapshot: UatHandoffSnapshot,
    report_payload: dict[str, Any],
    channel: UatDistributionChannel,
) -> str:
    cycle = report_payload.get("cycle", {})
    readiness = report_payload.get("approval_readiness", {})
    blocking_issues = readiness.get("blocking_issues", [])
    attention_items = readiness.get("attention_items", [])
    open_defects = report_payload.get("open_defects", [])
    channel_label = channel.value.replace("_", " ")

    lines = [
        f"Distribution packet: {snapshot.snapshot_name}",
        f"Channel: {channel_label}",
        "",
        f"Cycle: {cycle.get('cycle_name', snapshot.snapshot_name)}",
        f"Environment: {cycle.get('environment_name', 'unknown')}",
        f"Cycle status: {cycle.get('status', snapshot.cycle_status)}",
        f"Approval ready: {'yes' if snapshot.approval_ready else 'no'}",
        (
            "Terminal scenarios: "
            f"{readiness.get('terminal_scenario_count', 0)}/{cycle.get('scenario_count', 0)}"
        ),
        (
            "Evidence captured: "
            f"{readiness.get('evidence_captured_count', 0)}/"
            f"{readiness.get('terminal_scenario_count', 0)}"
        ),
        (
            "Open defects: "
            f"{snapshot.open_defect_count} "
            f"({snapshot.open_high_severity_defect_count} critical/high)"
        ),
        "",
        "Executive summary:",
        snapshot.distribution_summary,
    ]

    if blocking_issues:
        lines.extend(["", "Blocking issues:"])
        lines.extend(f"- {issue}" for issue in blocking_issues)
    if attention_items:
        lines.extend(["", "Attention items:"])
        lines.extend(f"- {item}" for item in attention_items)

    lines.extend(["", "Open defects:"])
    if open_defects:
        lines.extend(
            "- "
            + f"{defect.get('severity', 'unknown')} / {defect.get('status', 'unknown')} / "
            + f"{defect.get('title', 'Untitled defect')}"
            for defect in open_defects
        )
    else:
        lines.append("- None.")

    requested_action = (
        "Review this packet and record stakeholder acknowledgement for launch readiness."
        if snapshot.approval_ready
        else "Review blockers and record follow-up actions before launch readiness is approved."
    )
    lines.extend(["", f"Requested action: {requested_action}"])
    return "\n".join(lines)


def _reconcile_packet_state(
    packet: UatDistributionPacket,
    *,
    now: datetime | None = None,
) -> None:
    recipients = list(packet.recipients)
    recipient_count = len(recipients)
    packet.ready_to_send = bool(
        recipient_count
        and packet.subject_line.strip()
        and packet.summary_excerpt.strip()
        and packet.briefing_body.strip()
    )

    if not packet.ready_to_send:
        packet.distribution_status = UatDistributionPacketStatus.DRAFT.value
        packet.distributed_at = None
        packet.completed_at = None
        return

    statuses = {recipient.delivery_status for recipient in recipients}
    required_recipients = [recipient for recipient in recipients if recipient.required_for_ack]
    optional_recipients = [recipient for recipient in recipients if not recipient.required_for_ack]

    if required_recipients:
        required_complete = all(
            recipient.delivery_status == UatDistributionRecipientStatus.ACKNOWLEDGED.value
            for recipient in required_recipients
        )
        optional_complete = all(
            recipient.delivery_status
            in {
                UatDistributionRecipientStatus.SENT.value,
                UatDistributionRecipientStatus.ACKNOWLEDGED.value,
            }
            for recipient in optional_recipients
        )
        completed = required_complete and optional_complete
    else:
        completed = all(
            recipient.delivery_status
            in {
                UatDistributionRecipientStatus.SENT.value,
                UatDistributionRecipientStatus.ACKNOWLEDGED.value,
            }
            for recipient in recipients
        )

    has_distribution_progress = statuses != {UatDistributionRecipientStatus.PENDING.value}

    if completed:
        packet.distribution_status = UatDistributionPacketStatus.COMPLETED.value
        packet.distributed_at = packet.distributed_at or now or datetime.now(UTC)
        packet.completed_at = packet.completed_at or now or datetime.now(UTC)
    elif has_distribution_progress:
        packet.distribution_status = UatDistributionPacketStatus.DISTRIBUTED.value
        packet.distributed_at = packet.distributed_at or now or datetime.now(UTC)
        packet.completed_at = None
    else:
        packet.distribution_status = UatDistributionPacketStatus.READY.value
        packet.distributed_at = None
        packet.completed_at = None


def _list_packet_recipients(
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


def _get_snapshot(session: Session, snapshot_id: str) -> UatHandoffSnapshot:
    snapshot = session.get(UatHandoffSnapshot, UUID(snapshot_id))
    if snapshot is None:
        raise UatHandoffSnapshotNotFoundError(
            f"UAT handoff snapshot `{snapshot_id}` was not found."
        )
    return snapshot


def _get_packet(session: Session, packet_id: str) -> UatDistributionPacket:
    packet = session.get(UatDistributionPacket, UUID(packet_id))
    if packet is None:
        raise UatDistributionPacketNotFoundError(
            f"UAT distribution packet `{packet_id}` was not found."
        )
    return packet


def _get_packet_recipient(
    packet: UatDistributionPacket,
    recipient_id: str,
) -> UatDistributionRecipient:
    recipient_uuid = UUID(recipient_id)
    for recipient in packet.recipients:
        if recipient.recipient_id == recipient_uuid:
            return recipient

    raise UatDistributionRecipientNotFoundError(
        f"UAT distribution recipient `{recipient_id}` was not found for this packet."
    )


def _truncate(value: str, max_length: int) -> str:
    if len(value) <= max_length:
        return value
    return value[: max_length - 3].rstrip() + "..."
