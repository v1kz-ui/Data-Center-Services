from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.db.models.enums import (
    UatReleaseArchiveExportDeliveryEventType,
    UatReleaseArchiveExportHandoffStatus,
)
from app.db.models.uat import (
    UatReleaseArchive,
    UatReleaseArchiveExport,
    UatReleaseArchiveExportDeliveryEvent,
)
from app.services.uat_release_archive_followup import (
    build_uat_release_archive_followup_dashboard,
)


class UatReleaseArchiveDeliveryLedgerValidationError(ValueError):
    """Raised when a delivery-ledger request is invalid."""


@dataclass(slots=True)
class UatReleaseArchiveDeliveryLedgerItem:
    archive_id: str
    snapshot_id: str
    cycle_id: str
    snapshot_name: str
    archive_name: str
    support_handoff_owner: str | None
    latest_export_id: str | None
    latest_export_name: str | None
    latest_export_status: str | None
    destination_system: str | None
    destination_reference: str | None
    latest_external_handoff_at: datetime | None
    latest_external_handoff_reference: str | None
    latest_notification_sent_at: datetime | None
    latest_notification_acknowledged_at: datetime | None
    latest_notification_channel: str | None
    latest_notification_reference: str | None
    notification_acknowledgement_status: str
    escalation_status: str
    escalation_due_at: datetime | None
    next_retry_at: datetime | None
    attention_reasons: list[str]
    recommended_action: str


@dataclass(slots=True)
class UatReleaseArchiveDeliveryLedger:
    report_version: str
    generated_at: datetime
    generated_by: str
    as_of: datetime
    review_window_days: int
    stale_reply_after_hours: int
    total_archive_count: int
    ledger_item_count: int
    action_required_count: int
    escalated_count: int
    notification_pending_count: int
    acknowledged_count: int
    missing_export_count: int
    re_export_due_count: int
    items: list[UatReleaseArchiveDeliveryLedgerItem]


def build_uat_release_archive_delivery_ledger(
    session: Session,
    *,
    generated_by: str,
    review_window_days: int = 30,
    stale_reply_after_hours: int = 48,
    include_resolved: bool = False,
    as_of: datetime | None = None,
) -> UatReleaseArchiveDeliveryLedger:
    if review_window_days <= 0:
        raise UatReleaseArchiveDeliveryLedgerValidationError(
            "Review window days must be greater than 0."
        )
    if stale_reply_after_hours <= 0:
        raise UatReleaseArchiveDeliveryLedgerValidationError(
            "Stale reply escalation window must be greater than 0 hours."
        )

    reference_time = _normalize_datetime(as_of or datetime.now(UTC))
    dashboard = build_uat_release_archive_followup_dashboard(
        session,
        review_window_days=review_window_days,
        include_resolved=True,
        as_of=reference_time,
    )
    dashboard_items_by_archive_id = {
        item.archive_id: item for item in dashboard.items
    }

    archive_models = session.scalars(
        select(UatReleaseArchive)
        .options(
            selectinload(UatReleaseArchive.snapshot),
            selectinload(UatReleaseArchive.export_records),
            selectinload(UatReleaseArchive.delivery_events),
        )
        .order_by(UatReleaseArchive.archive_name.asc(), UatReleaseArchive.archive_id.asc())
    ).all()

    items: list[UatReleaseArchiveDeliveryLedgerItem] = []
    action_required_count = 0
    escalated_count = 0
    notification_pending_count = 0
    acknowledged_count = 0
    missing_export_count = 0
    re_export_due_count = 0

    for archive in archive_models:
        dashboard_item = dashboard_items_by_archive_id[str(archive.archive_id)]
        latest_export = _get_latest_export(list(archive.export_records))
        latest_notification_sent_event = _get_latest_event(
            list(archive.delivery_events),
            UatReleaseArchiveExportDeliveryEventType.NOTIFICATION_SENT.value,
        )
        latest_notification_acknowledged_event = _get_latest_event(
            list(archive.delivery_events),
            UatReleaseArchiveExportDeliveryEventType.NOTIFICATION_ACKNOWLEDGED.value,
        )
        latest_external_handoff_event = _get_latest_event(
            list(archive.delivery_events),
            UatReleaseArchiveExportDeliveryEventType.EXTERNAL_HANDOFF_LOGGED.value,
        )
        notification_acknowledgement_status = _derive_notification_acknowledgement_status(
            latest_notification_sent_event=latest_notification_sent_event,
            latest_notification_acknowledged_event=latest_notification_acknowledged_event,
        )

        attention_reasons = list(dashboard_item.attention_reasons)
        escalation_due_at: datetime | None = None
        escalation_status = "none"
        if latest_export is None:
            attention_reasons.append("missing_export")
            escalation_status = "missing_export"
        if notification_acknowledgement_status == "pending":
            attention_reasons.append("notification_acknowledgement_pending")
            if latest_notification_sent_event is not None:
                escalation_due_at = _normalize_datetime(
                    latest_notification_sent_event.occurred_at
                ) + timedelta(hours=stale_reply_after_hours)
                escalation_status = "pending"
                if escalation_due_at <= reference_time:
                    attention_reasons.append("stale_notification_reply")
                    escalation_status = "escalated"

        attention_reasons = list(dict.fromkeys(attention_reasons))
        if not include_resolved and not attention_reasons:
            continue

        if attention_reasons:
            action_required_count += 1
        if escalation_status == "escalated":
            escalated_count += 1
        if notification_acknowledgement_status == "pending":
            notification_pending_count += 1
        elif notification_acknowledgement_status == "acknowledged":
            acknowledged_count += 1
        if "missing_export" in attention_reasons:
            missing_export_count += 1
        if "re_export_due" in attention_reasons:
            re_export_due_count += 1

        items.append(
            UatReleaseArchiveDeliveryLedgerItem(
                archive_id=str(archive.archive_id),
                snapshot_id=str(archive.snapshot_id),
                cycle_id=str(archive.snapshot.cycle_id),
                snapshot_name=archive.snapshot.snapshot_name,
                archive_name=archive.archive_name,
                support_handoff_owner=archive.support_handoff_owner,
                latest_export_id=(
                    str(latest_export.export_id) if latest_export is not None else None
                ),
                latest_export_name=latest_export.export_name if latest_export is not None else None,
                latest_export_status=(
                    latest_export.handoff_status if latest_export is not None else None
                ),
                destination_system=(
                    latest_export.destination_system if latest_export is not None else None
                ),
                destination_reference=(
                    latest_export.destination_reference if latest_export is not None else None
                ),
                latest_external_handoff_at=(
                    _normalize_datetime(latest_external_handoff_event.occurred_at)
                    if latest_external_handoff_event is not None
                    else None
                ),
                latest_external_handoff_reference=(
                    latest_external_handoff_event.external_reference
                    if latest_external_handoff_event is not None
                    else None
                ),
                latest_notification_sent_at=(
                    _normalize_datetime(latest_notification_sent_event.occurred_at)
                    if latest_notification_sent_event is not None
                    else None
                ),
                latest_notification_acknowledged_at=(
                    _normalize_datetime(latest_notification_acknowledged_event.occurred_at)
                    if latest_notification_acknowledged_event is not None
                    else None
                ),
                latest_notification_channel=(
                    latest_notification_sent_event.delivery_channel
                    if latest_notification_sent_event is not None
                    else None
                ),
                latest_notification_reference=(
                    latest_notification_sent_event.external_reference
                    if latest_notification_sent_event is not None
                    else None
                ),
                notification_acknowledgement_status=notification_acknowledgement_status,
                escalation_status=escalation_status,
                escalation_due_at=escalation_due_at,
                next_retry_at=dashboard_item.next_retry_at,
                attention_reasons=attention_reasons,
                recommended_action=_build_recommended_action(
                    latest_export=latest_export,
                    notification_acknowledgement_status=notification_acknowledgement_status,
                    escalation_status=escalation_status,
                    attention_reasons=attention_reasons,
                ),
            )
        )

    items.sort(
        key=lambda item: (
            _priority(item.attention_reasons, item.escalation_status),
            item.archive_name,
        )
    )
    return UatReleaseArchiveDeliveryLedger(
        report_version="phase7-release-archive-delivery-ledger-v1",
        generated_at=datetime.now(UTC),
        generated_by=generated_by,
        as_of=reference_time,
        review_window_days=review_window_days,
        stale_reply_after_hours=stale_reply_after_hours,
        total_archive_count=len(archive_models),
        ledger_item_count=len(items),
        action_required_count=action_required_count,
        escalated_count=escalated_count,
        notification_pending_count=notification_pending_count,
        acknowledged_count=acknowledged_count,
        missing_export_count=missing_export_count,
        re_export_due_count=re_export_due_count,
        items=items,
    )


def _get_latest_export(
    export_records: list[UatReleaseArchiveExport],
) -> UatReleaseArchiveExport | None:
    if not export_records:
        return None
    return max(
        export_records,
        key=lambda export_record: (
            _normalize_datetime(export_record.exported_at),
            str(export_record.export_id),
        ),
    )


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


def _derive_notification_acknowledgement_status(
    *,
    latest_notification_sent_event: UatReleaseArchiveExportDeliveryEvent | None,
    latest_notification_acknowledged_event: UatReleaseArchiveExportDeliveryEvent | None,
) -> str:
    if latest_notification_sent_event is None:
        return "not_sent"
    if (
        latest_notification_acknowledged_event is not None
        and _normalize_datetime(latest_notification_acknowledged_event.occurred_at)
        >= _normalize_datetime(latest_notification_sent_event.occurred_at)
    ):
        return "acknowledged"
    return "pending"


def _build_recommended_action(
    *,
    latest_export: UatReleaseArchiveExport | None,
    notification_acknowledgement_status: str,
    escalation_status: str,
    attention_reasons: list[str],
) -> str:
    if "missing_export" in attention_reasons:
        return "create_archive_export"
    if escalation_status == "escalated":
        return "escalate_notification_reply"
    if "re_export_due" in attention_reasons:
        return "execute_scheduled_reexport"
    if "re_export_scheduled" in attention_reasons:
        return "monitor_scheduled_reexport"
    if notification_acknowledgement_status == "pending":
        return "await_notification_reply"
    if latest_export is not None and latest_export.handoff_status in {
        UatReleaseArchiveExportHandoffStatus.FOLLOW_UP_REQUIRED.value,
        UatReleaseArchiveExportHandoffStatus.RE_EXPORT_SCHEDULED.value,
    }:
        return "review_export_followup"
    if latest_export is not None and latest_export.handoff_status in {
        UatReleaseArchiveExportHandoffStatus.PREPARED.value,
        UatReleaseArchiveExportHandoffStatus.DELIVERED.value,
    }:
        return "confirm_external_handoff"
    return "monitor"


def _priority(attention_reasons: list[str], escalation_status: str) -> int:
    if escalation_status == "escalated":
        return 0
    if "re_export_due" in attention_reasons:
        return 1
    if "missing_export" in attention_reasons:
        return 2
    if "overdue_review" in attention_reasons:
        return 3
    if "notification_acknowledgement_pending" in attention_reasons:
        return 4
    if "due_soon_review" in attention_reasons:
        return 5
    if "re_export_scheduled" in attention_reasons:
        return 6
    if "follow_up_required" in attention_reasons:
        return 7
    if "acknowledgement_pending" in attention_reasons:
        return 8
    return 9


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
