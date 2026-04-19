from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

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
from app.services.uat_release_archive_operations import (
    UatReleaseArchiveExportDeliveryEventValidationError,
    UatReleaseArchiveExportNotFoundError,
    create_uat_release_archive_export,
    record_uat_release_archive_export_delivery_event,
)

RETRY_NAME_SUFFIX_PATTERN = re.compile(r"-retry-\d+$")
DISPATCH_SLUG_PATTERN = re.compile(r"[^a-z0-9]+")


class UatReleaseArchiveAutomationValidationError(ValueError):
    """Raised when archive automation requests are invalid."""


@dataclass(slots=True)
class UatReleaseArchiveReexportExecutionItem:
    archive_id: str
    archive_name: str
    source_export_id: str
    source_export_name: str
    destination_system: str
    scheduled_retry_at: datetime
    action: str
    generated_export_id: str | None
    generated_export_name: str | None
    resulting_source_status: str
    note: str


@dataclass(slots=True)
class UatReleaseArchiveReexportExecutionRun:
    report_version: str
    run_at: datetime
    executed_by: str
    dry_run: bool
    due_export_count: int
    executed_count: int
    skipped_count: int
    items: list[UatReleaseArchiveReexportExecutionItem]


@dataclass(slots=True)
class UatReleaseArchiveNotificationDigestArchiveItem:
    archive_id: str
    archive_name: str
    snapshot_name: str
    retention_review_at: datetime
    next_retry_at: datetime | None
    latest_export_name: str | None
    latest_notification_sent_at: datetime | None
    latest_notification_acknowledged_at: datetime | None
    notification_acknowledgement_status: str
    attention_reasons: list[str]


@dataclass(slots=True)
class UatReleaseArchiveNotificationDigestRecipient:
    recipient_name: str
    archive_count: int
    overdue_review_count: int
    due_soon_review_count: int
    acknowledgement_pending_count: int
    re_export_due_count: int
    notification_acknowledgement_pending_count: int
    archives: list[UatReleaseArchiveNotificationDigestArchiveItem]
    digest_message: str


@dataclass(slots=True)
class UatReleaseArchiveNotificationDigest:
    report_version: str
    generated_at: datetime
    generated_by: str
    review_window_days: int
    action_required_count: int
    recipient_count: int
    recipients: list[UatReleaseArchiveNotificationDigestRecipient]


@dataclass(slots=True)
class UatReleaseArchiveNotificationDispatchArchiveItem:
    archive_id: str
    archive_name: str
    snapshot_name: str
    export_id: str | None
    export_name: str | None
    next_retry_at: datetime | None
    latest_notification_sent_at: datetime | None
    latest_notification_acknowledged_at: datetime | None
    notification_acknowledgement_status: str
    attention_reasons: list[str]
    action: str
    delivery_event_id: str | None
    external_reference: str | None
    note: str


@dataclass(slots=True)
class UatReleaseArchiveNotificationDispatchRecipient:
    recipient_name: str
    archive_count: int
    planned_archive_count: int
    recorded_archive_count: int
    skipped_archive_count: int
    failed_archive_count: int
    delivery_channel: str
    external_reference: str | None
    subject_line: str
    digest_message: str
    archives: list[UatReleaseArchiveNotificationDispatchArchiveItem]


@dataclass(slots=True)
class UatReleaseArchiveNotificationDispatchRun:
    report_version: str
    run_at: datetime
    executed_by: str
    dry_run: bool
    review_window_days: int
    delivery_channel: str
    recipient_count: int
    planned_archive_count: int
    recorded_archive_count: int
    skipped_archive_count: int
    failed_archive_count: int
    recipients: list[UatReleaseArchiveNotificationDispatchRecipient]


@dataclass(slots=True)
class _ArchiveNotificationState:
    latest_notification_sent_at: datetime | None
    latest_notification_acknowledged_at: datetime | None
    notification_acknowledgement_status: str


def execute_due_uat_release_archive_reexports(
    session: Session,
    *,
    executed_by: str,
    dry_run: bool = False,
    limit: int | None = None,
    as_of: datetime | None = None,
) -> UatReleaseArchiveReexportExecutionRun:
    execution_time = _normalize_datetime(as_of or datetime.now(UTC))
    if limit is not None and limit <= 0:
        raise UatReleaseArchiveAutomationValidationError("Execution limit must be greater than 0.")

    statement = (
        select(UatReleaseArchiveExport)
        .join(UatReleaseArchive, UatReleaseArchive.archive_id == UatReleaseArchiveExport.archive_id)
        .options(
            selectinload(UatReleaseArchiveExport.archive).selectinload(UatReleaseArchive.snapshot),
            selectinload(UatReleaseArchiveExport.archive).selectinload(
                UatReleaseArchive.export_records
            ),
        )
        .where(
            UatReleaseArchiveExport.handoff_status
            == UatReleaseArchiveExportHandoffStatus.RE_EXPORT_SCHEDULED.value,
            UatReleaseArchiveExport.next_retry_at.is_not(None),
            UatReleaseArchiveExport.next_retry_at <= execution_time,
            UatReleaseArchive.superseded_by_archive_id.is_(None),
        )
        .order_by(
            UatReleaseArchiveExport.next_retry_at.asc(),
            UatReleaseArchiveExport.export_id.asc(),
        )
    )
    if limit is not None:
        statement = statement.limit(limit)

    due_exports = session.scalars(statement).all()
    items: list[UatReleaseArchiveReexportExecutionItem] = []
    executed_count = 0

    for source_export in due_exports:
        archive = source_export.archive
        scheduled_retry_at = _normalize_datetime(source_export.next_retry_at)
        generated_export_name = _build_retry_export_name(archive, source_export)
        note = (
            f"Scheduled re-export generated as `{generated_export_name}` for "
            f"{source_export.destination_system}."
        )

        if dry_run:
            items.append(
                UatReleaseArchiveReexportExecutionItem(
                    archive_id=str(archive.archive_id),
                    archive_name=archive.archive_name,
                    source_export_id=str(source_export.export_id),
                    source_export_name=source_export.export_name,
                    destination_system=source_export.destination_system,
                    scheduled_retry_at=scheduled_retry_at,
                    action="preview",
                    generated_export_id=None,
                    generated_export_name=generated_export_name,
                    resulting_source_status=source_export.handoff_status,
                    note=note,
                )
            )
            continue

        new_export = create_uat_release_archive_export(
            session,
            archive_id=str(archive.archive_id),
            export_name=generated_export_name,
            export_scope=source_export.export_scope,
            destination_system=source_export.destination_system,
            destination_reference=source_export.destination_reference,
            trigger_reason=_build_retry_trigger_reason(source_export),
            handoff_notes=_build_retry_handoff_notes(source_export, execution_time),
            exported_by=executed_by,
        )

        source_export.handoff_status = (
            UatReleaseArchiveExportHandoffStatus.RE_EXPORT_COMPLETED.value
        )
        source_export.next_retry_at = None
        source_export.last_status_updated_by = executed_by
        source_export.delivery_confirmed_by = executed_by
        source_export.delivery_confirmed_at = execution_time
        source_export.handoff_notes = _append_execution_note(
            source_export.handoff_notes,
            generated_export_name=generated_export_name,
            execution_time=execution_time,
        )
        session.commit()
        executed_count += 1
        items.append(
            UatReleaseArchiveReexportExecutionItem(
                archive_id=str(archive.archive_id),
                archive_name=archive.archive_name,
                source_export_id=str(source_export.export_id),
                source_export_name=source_export.export_name,
                destination_system=source_export.destination_system,
                scheduled_retry_at=scheduled_retry_at,
                action="executed",
                generated_export_id=new_export.export_id,
                generated_export_name=new_export.export_name,
                resulting_source_status=source_export.handoff_status,
                note=note,
            )
        )

    skipped_count = len(due_exports) - executed_count if not dry_run else 0
    return UatReleaseArchiveReexportExecutionRun(
        report_version="phase7-release-archive-reexport-run-v1",
        run_at=execution_time,
        executed_by=executed_by,
        dry_run=dry_run,
        due_export_count=len(due_exports),
        executed_count=executed_count,
        skipped_count=skipped_count,
        items=items,
    )


def build_uat_release_archive_followup_notification_digest(
    session: Session,
    *,
    generated_by: str,
    review_window_days: int = 30,
) -> UatReleaseArchiveNotificationDigest:
    dashboard = build_uat_release_archive_followup_dashboard(
        session,
        review_window_days=review_window_days,
        include_resolved=False,
    )
    actionable_archives = {
        item.archive_id: item for item in dashboard.items if item.attention_reasons
    }
    if not actionable_archives:
        return UatReleaseArchiveNotificationDigest(
            report_version="phase7-release-archive-notification-digest-v1",
            generated_at=datetime.now(UTC),
            generated_by=generated_by,
            review_window_days=review_window_days,
            action_required_count=0,
            recipient_count=0,
            recipients=[],
        )

    actionable_archive_ids = [UUID(archive_id) for archive_id in actionable_archives]
    notification_events = session.scalars(
        select(UatReleaseArchiveExportDeliveryEvent)
        .where(
            UatReleaseArchiveExportDeliveryEvent.archive_id.in_(actionable_archive_ids),
            UatReleaseArchiveExportDeliveryEvent.event_type.in_(
                [
                    UatReleaseArchiveExportDeliveryEventType.NOTIFICATION_SENT.value,
                    UatReleaseArchiveExportDeliveryEventType.NOTIFICATION_ACKNOWLEDGED.value,
                ]
            ),
        )
        .order_by(
            UatReleaseArchiveExportDeliveryEvent.occurred_at.desc(),
            UatReleaseArchiveExportDeliveryEvent.event_id.desc(),
        )
    ).all()
    notification_states = _build_archive_notification_states(notification_events)

    archive_models = session.scalars(
        select(UatReleaseArchive)
        .options(selectinload(UatReleaseArchive.snapshot))
        .where(UatReleaseArchive.archive_id.in_(actionable_archive_ids))
        .order_by(UatReleaseArchive.archive_name.asc())
    ).all()

    recipient_archives: dict[str, list[UatReleaseArchiveNotificationDigestArchiveItem]] = {}
    for archive in archive_models:
        archive_id = str(archive.archive_id)
        dashboard_item = actionable_archives[archive_id]
        notification_state = notification_states.get(
            archive_id,
            _ArchiveNotificationState(
                latest_notification_sent_at=None,
                latest_notification_acknowledged_at=None,
                notification_acknowledgement_status="not_sent",
            ),
        )
        attention_reasons = list(dashboard_item.attention_reasons)
        if notification_state.notification_acknowledgement_status == "pending":
            attention_reasons.append("notification_acknowledgement_pending")
        attention_reasons = list(dict.fromkeys(attention_reasons))
        recipient_name = archive.support_handoff_owner or "Unassigned"
        recipient_archives.setdefault(recipient_name, []).append(
            UatReleaseArchiveNotificationDigestArchiveItem(
                archive_id=archive_id,
                archive_name=archive.archive_name,
                snapshot_name=archive.snapshot.snapshot_name,
                retention_review_at=_normalize_datetime(archive.retention_review_at),
                next_retry_at=dashboard_item.next_retry_at,
                latest_export_name=dashboard_item.latest_export_name,
                latest_notification_sent_at=notification_state.latest_notification_sent_at,
                latest_notification_acknowledged_at=(
                    notification_state.latest_notification_acknowledged_at
                ),
                notification_acknowledgement_status=(
                    notification_state.notification_acknowledgement_status
                ),
                attention_reasons=attention_reasons,
            )
        )

    recipients = [
        _build_recipient_digest(recipient_name, archive_items)
        for recipient_name, archive_items in sorted(recipient_archives.items())
    ]
    return UatReleaseArchiveNotificationDigest(
        report_version="phase7-release-archive-notification-digest-v1",
        generated_at=datetime.now(UTC),
        generated_by=generated_by,
        review_window_days=review_window_days,
        action_required_count=sum(len(recipient.archives) for recipient in recipients),
        recipient_count=len(recipients),
        recipients=recipients,
    )


def execute_uat_release_archive_followup_notification_dispatch(
    session: Session,
    *,
    executed_by: str,
    delivery_channel: str = "email",
    dry_run: bool = False,
    review_window_days: int = 30,
    recipient_limit: int | None = None,
    as_of: datetime | None = None,
) -> UatReleaseArchiveNotificationDispatchRun:
    if review_window_days <= 0:
        raise UatReleaseArchiveAutomationValidationError(
            "Review window days must be greater than 0."
        )
    if recipient_limit is not None and recipient_limit <= 0:
        raise UatReleaseArchiveAutomationValidationError(
            "Recipient limit must be greater than 0."
        )

    normalized_delivery_channel = delivery_channel.strip()
    if not normalized_delivery_channel:
        raise UatReleaseArchiveAutomationValidationError(
            "Delivery channel must be a non-empty value."
        )

    execution_time = _normalize_datetime(as_of or datetime.now(UTC))
    if not dry_run and execution_time > datetime.now(UTC):
        raise UatReleaseArchiveAutomationValidationError(
            "Notification dispatch runs cannot execute in the future."
        )

    digest = build_uat_release_archive_followup_notification_digest(
        session,
        generated_by=executed_by,
        review_window_days=review_window_days,
    )
    selected_recipients = (
        digest.recipients[:recipient_limit]
        if recipient_limit is not None
        else digest.recipients
    )
    if not selected_recipients:
        return UatReleaseArchiveNotificationDispatchRun(
            report_version="phase7-release-archive-notification-dispatch-run-v1",
            run_at=execution_time,
            executed_by=executed_by,
            dry_run=dry_run,
            review_window_days=review_window_days,
            delivery_channel=normalized_delivery_channel,
            recipient_count=0,
            planned_archive_count=0,
            recorded_archive_count=0,
            skipped_archive_count=0,
            failed_archive_count=0,
            recipients=[],
        )

    actionable_archive_ids = {
        UUID(archive_item.archive_id)
        for recipient in selected_recipients
        for archive_item in recipient.archives
    }
    archive_models = session.scalars(
        select(UatReleaseArchive)
        .options(selectinload(UatReleaseArchive.export_records))
        .where(UatReleaseArchive.archive_id.in_(actionable_archive_ids))
    ).all()
    archive_by_id = {str(archive.archive_id): archive for archive in archive_models}

    planned_archive_count = 0
    recorded_archive_count = 0
    skipped_archive_count = 0
    failed_archive_count = 0
    dispatch_recipients: list[UatReleaseArchiveNotificationDispatchRecipient] = []

    for recipient in selected_recipients:
        dispatch_reference = _build_notification_dispatch_reference(
            execution_time,
            recipient.recipient_name,
        )
        planned_count = 0
        recorded_count = 0
        skipped_count = 0
        failed_count = 0
        dispatch_items: list[UatReleaseArchiveNotificationDispatchArchiveItem] = []

        for archive_item in recipient.archives:
            archive_model = archive_by_id.get(archive_item.archive_id)
            latest_export = (
                _get_latest_export(list(archive_model.export_records))
                if archive_model is not None
                else None
            )
            action = "preview_send"
            delivery_event_id: str | None = None
            external_reference: str | None = dispatch_reference
            note = "Notification dispatch preview is ready."

            if latest_export is None:
                action = "skipped_missing_export"
                external_reference = None
                note = "No archive export is available for notification journaling."
                skipped_count += 1
            elif archive_item.notification_acknowledgement_status == "pending":
                action = "skipped_pending_acknowledgement"
                external_reference = None
                note = "A prior notification is already awaiting acknowledgement."
                skipped_count += 1
            elif dry_run:
                planned_count += 1
            else:
                try:
                    event_record = record_uat_release_archive_export_delivery_event(
                        session,
                        export_id=str(latest_export.export_id),
                        event_type=UatReleaseArchiveExportDeliveryEventType.NOTIFICATION_SENT,
                        recorded_by=executed_by,
                        target_name=recipient.recipient_name,
                        delivery_channel=normalized_delivery_channel,
                        external_reference=dispatch_reference,
                        event_notes=_build_notification_dispatch_event_note(
                            execution_time,
                            archive_item.attention_reasons,
                        ),
                        occurred_at=execution_time,
                    )
                except (
                    UatReleaseArchiveExportDeliveryEventValidationError,
                    UatReleaseArchiveExportNotFoundError,
                ) as exc:
                    action = "failed"
                    external_reference = None
                    note = str(exc)
                    failed_count += 1
                else:
                    action = "recorded"
                    note = "Notification dispatch recorded."
                    delivery_event_id = event_record.event_id
                    recorded_count += 1

            dispatch_items.append(
                UatReleaseArchiveNotificationDispatchArchiveItem(
                    archive_id=archive_item.archive_id,
                    archive_name=archive_item.archive_name,
                    snapshot_name=archive_item.snapshot_name,
                    export_id=str(latest_export.export_id) if latest_export is not None else None,
                    export_name=latest_export.export_name if latest_export is not None else None,
                    next_retry_at=archive_item.next_retry_at,
                    latest_notification_sent_at=archive_item.latest_notification_sent_at,
                    latest_notification_acknowledged_at=(
                        archive_item.latest_notification_acknowledged_at
                    ),
                    notification_acknowledgement_status=(
                        archive_item.notification_acknowledgement_status
                    ),
                    attention_reasons=archive_item.attention_reasons,
                    action=action,
                    delivery_event_id=delivery_event_id,
                    external_reference=external_reference,
                    note=note,
                )
            )

        planned_archive_count += planned_count
        recorded_archive_count += recorded_count
        skipped_archive_count += skipped_count
        failed_archive_count += failed_count
        dispatch_recipients.append(
            UatReleaseArchiveNotificationDispatchRecipient(
                recipient_name=recipient.recipient_name,
                archive_count=recipient.archive_count,
                planned_archive_count=planned_count,
                recorded_archive_count=recorded_count,
                skipped_archive_count=skipped_count,
                failed_archive_count=failed_count,
                delivery_channel=normalized_delivery_channel,
                external_reference=(
                    dispatch_reference if planned_count or recorded_count else None
                ),
                subject_line=_build_notification_dispatch_subject(
                    recipient.recipient_name,
                    recipient.archive_count,
                ),
                digest_message=recipient.digest_message,
                archives=dispatch_items,
            )
        )

    return UatReleaseArchiveNotificationDispatchRun(
        report_version="phase7-release-archive-notification-dispatch-run-v1",
        run_at=execution_time,
        executed_by=executed_by,
        dry_run=dry_run,
        review_window_days=review_window_days,
        delivery_channel=normalized_delivery_channel,
        recipient_count=len(dispatch_recipients),
        planned_archive_count=planned_archive_count,
        recorded_archive_count=recorded_archive_count,
        skipped_archive_count=skipped_archive_count,
        failed_archive_count=failed_archive_count,
        recipients=dispatch_recipients,
    )


def _build_recipient_digest(
    recipient_name: str,
    archive_items: list[UatReleaseArchiveNotificationDigestArchiveItem],
) -> UatReleaseArchiveNotificationDigestRecipient:
    overdue_review_count = sum(
        1 for item in archive_items if "overdue_review" in item.attention_reasons
    )
    due_soon_review_count = sum(
        1 for item in archive_items if "due_soon_review" in item.attention_reasons
    )
    acknowledgement_pending_count = sum(
        1 for item in archive_items if "acknowledgement_pending" in item.attention_reasons
    )
    re_export_due_count = sum(
        1 for item in archive_items if "re_export_due" in item.attention_reasons
    )
    notification_acknowledgement_pending_count = sum(
        1
        for item in archive_items
        if "notification_acknowledgement_pending" in item.attention_reasons
    )
    digest_message = (
        f"{recipient_name}: {len(archive_items)} archives need follow-up; "
        f"{overdue_review_count} overdue reviews, {due_soon_review_count} due soon, "
        f"{acknowledgement_pending_count} awaiting acknowledgement, "
        f"{re_export_due_count} due re-exports, "
        f"{notification_acknowledgement_pending_count} pending notification replies."
    )
    archive_items.sort(key=lambda item: (item.retention_review_at, item.archive_name))
    return UatReleaseArchiveNotificationDigestRecipient(
        recipient_name=recipient_name,
        archive_count=len(archive_items),
        overdue_review_count=overdue_review_count,
        due_soon_review_count=due_soon_review_count,
        acknowledgement_pending_count=acknowledgement_pending_count,
        re_export_due_count=re_export_due_count,
        notification_acknowledgement_pending_count=notification_acknowledgement_pending_count,
        archives=archive_items,
        digest_message=digest_message,
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


def _build_notification_dispatch_subject(recipient_name: str, archive_count: int) -> str:
    archive_label = "archive" if archive_count == 1 else "archives"
    return f"[UAT Follow-up] {recipient_name} - {archive_count} {archive_label} need attention"


def _build_notification_dispatch_reference(execution_time: datetime, recipient_name: str) -> str:
    slug = DISPATCH_SLUG_PATTERN.sub("-", recipient_name.lower()).strip("-")
    if not slug:
        slug = "unassigned"
    return f"uat-followup://dispatch/{execution_time.strftime('%Y%m%d%H%M%S')}/{slug}"


def _build_notification_dispatch_event_note(
    execution_time: datetime,
    attention_reasons: list[str],
) -> str:
    reasons = ", ".join(attention_reasons) if attention_reasons else "general follow-up"
    return (
        f"Automated follow-up notification dispatch executed at "
        f"{_normalize_datetime(execution_time).isoformat()} for {reasons}."
    )


def _build_retry_export_name(
    archive: UatReleaseArchive,
    source_export: UatReleaseArchiveExport,
) -> str:
    existing_names = {export_record.export_name for export_record in archive.export_records}
    root_name = RETRY_NAME_SUFFIX_PATTERN.sub("", source_export.export_name)
    attempt = max(source_export.retry_count, 1)
    candidate = _truncate_export_name(f"{root_name}-retry-{attempt}")
    next_attempt = attempt
    while candidate in existing_names:
        next_attempt += 1
        candidate = _truncate_export_name(f"{root_name}-retry-{next_attempt}")
    return candidate


def _truncate_export_name(value: str) -> str:
    return value[:255]


def _build_retry_trigger_reason(source_export: UatReleaseArchiveExport) -> str:
    return (
        f"Scheduled re-export executed from `{source_export.export_name}` "
        f"for {source_export.destination_system}."
    )


def _build_retry_handoff_notes(
    source_export: UatReleaseArchiveExport,
    execution_time: datetime,
) -> str:
    return (
        f"Retry material generated from scheduled export `{source_export.export_name}` "
        f"at {_normalize_datetime(execution_time).isoformat()}."
    )


def _append_execution_note(
    handoff_notes: str | None,
    *,
    generated_export_name: str,
    execution_time: datetime,
) -> str:
    execution_note = (
        f" Scheduled re-export executed as `{generated_export_name}` at "
        f"{_normalize_datetime(execution_time).isoformat()}."
    )
    if handoff_notes:
        return f"{handoff_notes.rstrip()}{execution_note}"
    return execution_note.strip()


def _build_archive_notification_states(
    event_models: list[UatReleaseArchiveExportDeliveryEvent],
) -> dict[str, _ArchiveNotificationState]:
    latest_sent_at_by_archive: dict[str, datetime] = {}
    latest_acknowledged_at_by_archive: dict[str, datetime] = {}

    for event_model in event_models:
        archive_id = str(event_model.archive_id)
        occurred_at = _normalize_datetime(event_model.occurred_at)
        if (
            event_model.event_type
            == UatReleaseArchiveExportDeliveryEventType.NOTIFICATION_SENT.value
            and (
                archive_id not in latest_sent_at_by_archive
                or occurred_at > latest_sent_at_by_archive[archive_id]
            )
        ):
            latest_sent_at_by_archive[archive_id] = occurred_at
        elif (
            event_model.event_type
            == UatReleaseArchiveExportDeliveryEventType.NOTIFICATION_ACKNOWLEDGED.value
            and (
                archive_id not in latest_acknowledged_at_by_archive
                or occurred_at > latest_acknowledged_at_by_archive[archive_id]
            )
        ):
            latest_acknowledged_at_by_archive[archive_id] = occurred_at

    notification_states: dict[str, _ArchiveNotificationState] = {}
    archive_ids = set(latest_sent_at_by_archive) | set(latest_acknowledged_at_by_archive)
    for archive_id in archive_ids:
        latest_sent_at = latest_sent_at_by_archive.get(archive_id)
        latest_acknowledged_at = latest_acknowledged_at_by_archive.get(archive_id)
        if latest_sent_at is None:
            status = "not_sent"
        elif latest_acknowledged_at is not None and latest_acknowledged_at >= latest_sent_at:
            status = "acknowledged"
        else:
            status = "pending"
        notification_states[archive_id] = _ArchiveNotificationState(
            latest_notification_sent_at=latest_sent_at,
            latest_notification_acknowledged_at=latest_acknowledged_at,
            notification_acknowledgement_status=status,
        )

    return notification_states


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
