from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.db.models.enums import UatReleaseArchiveExportDeliveryEventType
from app.db.models.uat import (
    UatReleaseArchive,
    UatReleaseArchiveExportDeliveryEvent,
)
from app.services.uat_release_archive_delivery_ledger import (
    UatReleaseArchiveDeliveryLedgerItem,
    build_uat_release_archive_delivery_ledger,
)


@dataclass(slots=True)
class UatReleaseArchiveClosureHistoryTimelineEvent:
    event_id: str
    export_id: str
    export_name: str
    event_type: str
    stage_label: str
    target_name: str
    delivery_channel: str | None
    external_reference: str | None
    event_notes: str | None
    occurred_at: datetime
    recorded_by: str


@dataclass(slots=True)
class UatReleaseArchiveClosureHistoryArchiveItem:
    archive_id: str
    snapshot_id: str
    cycle_id: str
    snapshot_name: str
    archive_name: str
    owner_name: str
    latest_export_id: str | None
    latest_export_name: str | None
    latest_export_status: str | None
    destination_system: str | None
    destination_reference: str | None
    notification_acknowledgement_status: str
    escalation_status: str
    closure_status: str
    latest_notification_sent_at: datetime | None
    latest_notification_acknowledged_at: datetime | None
    latest_external_handoff_at: datetime | None
    latest_escalation_outcome_at: datetime | None
    latest_support_handback_acknowledged_at: datetime | None
    latest_closure_confirmed_at: datetime | None
    unresolved_actions: list[str]
    closure_summary: str
    timeline: list[UatReleaseArchiveClosureHistoryTimelineEvent]


@dataclass(slots=True)
class UatReleaseArchiveClosureHistoryOwner:
    owner_name: str
    archive_count: int
    closed_count: int
    awaiting_closure_confirmation_count: int
    awaiting_support_handback_acknowledgement_count: int
    remediation_in_progress_count: int
    blocked_count: int
    open_followup_count: int
    superseded_count: int
    archives: list[UatReleaseArchiveClosureHistoryArchiveItem]
    summary_message: str


@dataclass(slots=True)
class UatReleaseArchiveClosureHistoryReport:
    report_version: str
    generated_at: datetime
    generated_by: str
    as_of: datetime
    review_window_days: int
    stale_reply_after_hours: int
    total_archive_count: int
    included_archive_count: int
    owner_count: int
    closed_count: int
    awaiting_closure_confirmation_count: int
    awaiting_support_handback_acknowledgement_count: int
    remediation_in_progress_count: int
    blocked_count: int
    open_followup_count: int
    superseded_count: int
    owners: list[UatReleaseArchiveClosureHistoryOwner]


def build_uat_release_archive_closure_history_report(
    session: Session,
    *,
    generated_by: str,
    review_window_days: int = 30,
    stale_reply_after_hours: int = 48,
    include_closed: bool = True,
    as_of: datetime | None = None,
) -> UatReleaseArchiveClosureHistoryReport:
    ledger = build_uat_release_archive_delivery_ledger(
        session,
        generated_by=generated_by,
        review_window_days=review_window_days,
        stale_reply_after_hours=stale_reply_after_hours,
        include_resolved=True,
        as_of=as_of,
    )
    ledger_items_by_archive_id = {item.archive_id: item for item in ledger.items}

    archive_models = session.scalars(
        select(UatReleaseArchive)
        .options(
            selectinload(UatReleaseArchive.snapshot),
            selectinload(UatReleaseArchive.export_records),
            selectinload(UatReleaseArchive.delivery_events),
        )
        .order_by(UatReleaseArchive.archive_name.asc(), UatReleaseArchive.archive_id.asc())
    ).all()

    owner_archives: dict[str, list[UatReleaseArchiveClosureHistoryArchiveItem]] = {}
    total_archive_count = 0
    included_archive_count = 0
    closed_count = 0
    awaiting_closure_confirmation_count = 0
    awaiting_support_handback_acknowledgement_count = 0
    remediation_in_progress_count = 0
    blocked_count = 0
    open_followup_count = 0
    superseded_count = 0

    for archive in archive_models:
        total_archive_count += 1
        ledger_item = ledger_items_by_archive_id[str(archive.archive_id)]
        export_lookup = {
            str(export_record.export_id): export_record.export_name
            for export_record in archive.export_records
        }
        timeline = _build_timeline(
            event_records=list(archive.delivery_events),
            export_lookup=export_lookup,
        )
        latest_escalation_outcome_event = _get_latest_event(
            list(archive.delivery_events),
            UatReleaseArchiveExportDeliveryEventType.ESCALATION_OUTCOME_RECORDED.value,
        )
        latest_support_handback_acknowledged_event = _get_latest_event(
            list(archive.delivery_events),
            UatReleaseArchiveExportDeliveryEventType.SUPPORT_HANDBACK_ACKNOWLEDGED.value,
        )
        latest_closure_confirmed_event = _get_latest_event(
            list(archive.delivery_events),
            UatReleaseArchiveExportDeliveryEventType.CLOSURE_CONFIRMED.value,
        )
        closure_status = _derive_closure_status(
            archive=archive,
            ledger_item=ledger_item,
            latest_escalation_outcome_event=latest_escalation_outcome_event,
            latest_support_handback_acknowledged_event=latest_support_handback_acknowledged_event,
            latest_closure_confirmed_event=latest_closure_confirmed_event,
        )
        if not include_closed and closure_status in {"closed", "superseded"}:
            continue

        owner_name = archive.support_handoff_owner or "Unassigned"
        unresolved_actions = _build_unresolved_actions(
            closure_status=closure_status,
            attention_reasons=ledger_item.attention_reasons,
        )
        item = UatReleaseArchiveClosureHistoryArchiveItem(
            archive_id=str(archive.archive_id),
            snapshot_id=str(archive.snapshot_id),
            cycle_id=str(archive.snapshot.cycle_id),
            snapshot_name=archive.snapshot.snapshot_name,
            archive_name=archive.archive_name,
            owner_name=owner_name,
            latest_export_id=ledger_item.latest_export_id,
            latest_export_name=ledger_item.latest_export_name,
            latest_export_status=ledger_item.latest_export_status,
            destination_system=ledger_item.destination_system,
            destination_reference=ledger_item.destination_reference,
            notification_acknowledgement_status=ledger_item.notification_acknowledgement_status,
            escalation_status=ledger_item.escalation_status,
            closure_status=closure_status,
            latest_notification_sent_at=ledger_item.latest_notification_sent_at,
            latest_notification_acknowledged_at=ledger_item.latest_notification_acknowledged_at,
            latest_external_handoff_at=ledger_item.latest_external_handoff_at,
            latest_escalation_outcome_at=_get_event_time(latest_escalation_outcome_event),
            latest_support_handback_acknowledged_at=_get_event_time(
                latest_support_handback_acknowledged_event
            ),
            latest_closure_confirmed_at=_get_event_time(latest_closure_confirmed_event),
            unresolved_actions=unresolved_actions,
            closure_summary=_build_closure_summary(
                archive_name=archive.archive_name,
                closure_status=closure_status,
                unresolved_actions=unresolved_actions,
                latest_closure_confirmed_at=_get_event_time(latest_closure_confirmed_event),
            ),
            timeline=timeline,
        )
        owner_archives.setdefault(owner_name, []).append(item)
        included_archive_count += 1

        if closure_status == "closed":
            closed_count += 1
        elif closure_status == "awaiting_closure_confirmation":
            awaiting_closure_confirmation_count += 1
        elif closure_status == "awaiting_support_handback_acknowledgement":
            awaiting_support_handback_acknowledgement_count += 1
        elif closure_status == "remediation_in_progress":
            remediation_in_progress_count += 1
        elif closure_status == "blocked":
            blocked_count += 1
        elif closure_status == "open_followup":
            open_followup_count += 1
        elif closure_status == "superseded":
            superseded_count += 1

    owners: list[UatReleaseArchiveClosureHistoryOwner] = []
    for owner_name, archive_items in sorted(owner_archives.items()):
        archive_items.sort(
            key=lambda item: (_closure_priority(item.closure_status), item.archive_name)
        )
        owner_closed_count = sum(
            1 for item in archive_items if item.closure_status == "closed"
        )
        owner_awaiting_closure_count = sum(
            1
            for item in archive_items
            if item.closure_status == "awaiting_closure_confirmation"
        )
        owner_support_ack_count = sum(
            1
            for item in archive_items
            if item.closure_status == "awaiting_support_handback_acknowledgement"
        )
        owner_remediation_count = sum(
            1
            for item in archive_items
            if item.closure_status == "remediation_in_progress"
        )
        owner_blocked_count = sum(
            1 for item in archive_items if item.closure_status == "blocked"
        )
        owner_open_count = sum(
            1 for item in archive_items if item.closure_status == "open_followup"
        )
        owner_superseded_count = sum(
            1 for item in archive_items if item.closure_status == "superseded"
        )
        owners.append(
            UatReleaseArchiveClosureHistoryOwner(
                owner_name=owner_name,
                archive_count=len(archive_items),
                closed_count=owner_closed_count,
                awaiting_closure_confirmation_count=owner_awaiting_closure_count,
                awaiting_support_handback_acknowledgement_count=owner_support_ack_count,
                remediation_in_progress_count=owner_remediation_count,
                blocked_count=owner_blocked_count,
                open_followup_count=owner_open_count,
                superseded_count=owner_superseded_count,
                archives=archive_items,
                summary_message=_build_owner_summary_message(
                    owner_name=owner_name,
                    archive_count=len(archive_items),
                    closed_count=owner_closed_count,
                    awaiting_closure_confirmation_count=owner_awaiting_closure_count,
                    awaiting_support_handback_acknowledgement_count=owner_support_ack_count,
                    remediation_in_progress_count=owner_remediation_count,
                    blocked_count=owner_blocked_count,
                    open_followup_count=owner_open_count,
                ),
            )
        )

    return UatReleaseArchiveClosureHistoryReport(
        report_version="phase7-release-archive-closure-history-v1",
        generated_at=datetime.now(UTC),
        generated_by=generated_by,
        as_of=ledger.as_of,
        review_window_days=review_window_days,
        stale_reply_after_hours=stale_reply_after_hours,
        total_archive_count=total_archive_count,
        included_archive_count=included_archive_count,
        owner_count=len(owners),
        closed_count=closed_count,
        awaiting_closure_confirmation_count=awaiting_closure_confirmation_count,
        awaiting_support_handback_acknowledgement_count=(
            awaiting_support_handback_acknowledgement_count
        ),
        remediation_in_progress_count=remediation_in_progress_count,
        blocked_count=blocked_count,
        open_followup_count=open_followup_count,
        superseded_count=superseded_count,
        owners=owners,
    )


def _build_timeline(
    *,
    event_records: list[UatReleaseArchiveExportDeliveryEvent],
    export_lookup: dict[str, str],
) -> list[UatReleaseArchiveClosureHistoryTimelineEvent]:
    ordered_events = sorted(
        event_records,
        key=lambda event_record: (
            _normalize_datetime(event_record.occurred_at),
            str(event_record.event_id),
        ),
    )
    return [
        UatReleaseArchiveClosureHistoryTimelineEvent(
            event_id=str(event_record.event_id),
            export_id=str(event_record.export_id),
            export_name=export_lookup.get(str(event_record.export_id), "Unknown export"),
            event_type=event_record.event_type,
            stage_label=_timeline_stage_label(event_record.event_type),
            target_name=event_record.target_name,
            delivery_channel=event_record.delivery_channel,
            external_reference=event_record.external_reference,
            event_notes=event_record.event_notes,
            occurred_at=_normalize_datetime(event_record.occurred_at),
            recorded_by=event_record.recorded_by,
        )
        for event_record in ordered_events
    ]


def _derive_closure_status(
    *,
    archive: UatReleaseArchive,
    ledger_item: UatReleaseArchiveDeliveryLedgerItem,
    latest_escalation_outcome_event: UatReleaseArchiveExportDeliveryEvent | None,
    latest_support_handback_acknowledged_event: UatReleaseArchiveExportDeliveryEvent | None,
    latest_closure_confirmed_event: UatReleaseArchiveExportDeliveryEvent | None,
) -> str:
    if archive.superseded_by_archive_id is not None:
        return "superseded"
    if latest_closure_confirmed_event is not None:
        return "closed"
    if latest_support_handback_acknowledged_event is not None or (
        ledger_item.latest_export_status == "acknowledged"
    ):
        return "awaiting_closure_confirmation"
    if latest_escalation_outcome_event is not None:
        return "remediation_in_progress"
    if any(
        reason in ledger_item.attention_reasons
        for reason in {"missing_export", "stale_notification_reply", "overdue_review"}
    ):
        return "blocked"
    if (
        ledger_item.latest_external_handoff_at is not None
        or ledger_item.notification_acknowledgement_status == "acknowledged"
        or ledger_item.latest_export_status in {"prepared", "delivered"}
    ):
        return "awaiting_support_handback_acknowledgement"
    return "open_followup"


def _build_unresolved_actions(
    *,
    closure_status: str,
    attention_reasons: list[str],
) -> list[str]:
    actions: list[str] = []
    if "missing_export" in attention_reasons:
        actions.append("Create the downstream archive export.")
    if "stale_notification_reply" in attention_reasons:
        actions.append("Resolve the stale notification reply escalation.")
    if "re_export_due" in attention_reasons:
        actions.append("Execute the scheduled re-export.")
    if "re_export_scheduled" in attention_reasons:
        actions.append("Monitor the scheduled re-export window.")
    if "follow_up_required" in attention_reasons:
        actions.append("Complete the outstanding export follow-up.")
    if "notification_acknowledgement_pending" in attention_reasons:
        actions.append("Capture the notification acknowledgement from support.")
    if "acknowledgement_pending" in attention_reasons:
        actions.append("Confirm downstream export acknowledgement.")
    if "overdue_review" in attention_reasons:
        actions.append("Complete the overdue retention review.")
    if closure_status == "awaiting_support_handback_acknowledgement":
        actions.append("Capture downstream support handback acknowledgement.")
    if closure_status == "awaiting_closure_confirmation":
        actions.append("Record final closure confirmation.")
    if closure_status == "remediation_in_progress":
        actions.append("Track the remediation outcome through to closure.")
    if closure_status == "open_followup":
        actions.append("Continue delivery follow-up until handback is acknowledged.")
    return list(dict.fromkeys(actions))


def _build_closure_summary(
    *,
    archive_name: str,
    closure_status: str,
    unresolved_actions: list[str],
    latest_closure_confirmed_at: datetime | None,
) -> str:
    if closure_status == "closed" and latest_closure_confirmed_at is not None:
        return (
            f"{archive_name}: closed at {latest_closure_confirmed_at.isoformat()} after "
            "delivery follow-up and support handback acknowledgement."
        )
    if unresolved_actions:
        return (
            f"{archive_name}: {closure_status.replace('_', ' ')}. Next actions: "
            + "; ".join(unresolved_actions)
        )
    return f"{archive_name}: {closure_status.replace('_', ' ')}."


def _build_owner_summary_message(
    *,
    owner_name: str,
    archive_count: int,
    closed_count: int,
    awaiting_closure_confirmation_count: int,
    awaiting_support_handback_acknowledgement_count: int,
    remediation_in_progress_count: int,
    blocked_count: int,
    open_followup_count: int,
) -> str:
    return (
        f"{owner_name}: {archive_count} archives in closure scope; "
        f"{closed_count} closed, {awaiting_closure_confirmation_count} awaiting closure "
        f"confirmation, {awaiting_support_handback_acknowledgement_count} awaiting "
        f"support handback acknowledgement, {remediation_in_progress_count} in "
        f"remediation, {blocked_count} blocked, {open_followup_count} open."
    )


def _timeline_stage_label(event_type: str) -> str:
    if event_type == UatReleaseArchiveExportDeliveryEventType.NOTIFICATION_SENT.value:
        return "notification_sent"
    if event_type == UatReleaseArchiveExportDeliveryEventType.NOTIFICATION_ACKNOWLEDGED.value:
        return "notification_acknowledged"
    if event_type == UatReleaseArchiveExportDeliveryEventType.EXTERNAL_HANDOFF_LOGGED.value:
        return "external_handoff"
    if (
        event_type
        == UatReleaseArchiveExportDeliveryEventType.ESCALATION_OUTCOME_RECORDED.value
    ):
        return "escalation_outcome"
    if (
        event_type
        == UatReleaseArchiveExportDeliveryEventType.SUPPORT_HANDBACK_ACKNOWLEDGED.value
    ):
        return "support_handback_acknowledged"
    if event_type == UatReleaseArchiveExportDeliveryEventType.CLOSURE_CONFIRMED.value:
        return "closure_confirmed"
    return "delivery_event"


def _get_latest_event(
    event_records: list[UatReleaseArchiveExportDeliveryEvent],
    event_type: str,
) -> UatReleaseArchiveExportDeliveryEvent | None:
    matching_events = [
        event_record for event_record in event_records if event_record.event_type == event_type
    ]
    if not matching_events:
        return None
    return max(
        matching_events,
        key=lambda event_record: (
            _normalize_datetime(event_record.occurred_at),
            str(event_record.event_id),
        ),
    )


def _get_event_time(
    event_record: UatReleaseArchiveExportDeliveryEvent | None,
) -> datetime | None:
    if event_record is None:
        return None
    return _normalize_datetime(event_record.occurred_at)


def _closure_priority(closure_status: str) -> int:
    if closure_status == "blocked":
        return 0
    if closure_status == "remediation_in_progress":
        return 1
    if closure_status == "awaiting_support_handback_acknowledgement":
        return 2
    if closure_status == "awaiting_closure_confirmation":
        return 3
    if closure_status == "open_followup":
        return 4
    if closure_status == "closed":
        return 5
    return 6


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
