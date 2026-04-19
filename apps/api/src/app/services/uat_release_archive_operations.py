from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from app.db.models.enums import (
    UatReleaseArchiveExportDeliveryEventType,
    UatReleaseArchiveExportHandoffStatus,
    UatReleaseArchiveRetentionActionType,
)
from app.db.models.uat import (
    UatReleaseArchive,
    UatReleaseArchiveExport,
    UatReleaseArchiveExportDeliveryEvent,
    UatReleaseArchiveRetentionAction,
)
from app.services.uat_release_archive import (
    UatReleaseArchiveDetail,
    UatReleaseArchiveNotFoundError,
    get_uat_release_archive,
)


class UatReleaseArchiveExportNotFoundError(LookupError):
    """Raised when a UAT release archive export cannot be found."""


class UatReleaseArchiveExportConflictError(ValueError):
    """Raised when a UAT release archive export conflicts with existing state."""


class UatReleaseArchiveExportValidationError(ValueError):
    """Raised when a UAT release archive export update is invalid."""


class UatReleaseArchiveRetentionActionValidationError(ValueError):
    """Raised when a UAT release archive retention action is invalid."""


class UatReleaseArchiveExportDeliveryEventValidationError(ValueError):
    """Raised when a UAT release archive export delivery event is invalid."""


@dataclass(slots=True)
class UatReleaseArchiveRetentionQueueItem:
    archive_id: str
    snapshot_id: str
    cycle_id: str
    snapshot_name: str
    archive_name: str
    recommended_outcome: str
    retention_review_at: datetime
    retention_status: str
    support_handoff_owner: str | None
    superseded_by_archive_id: str | None
    latest_export_name: str | None
    latest_exported_at: datetime | None
    export_count: int
    days_until_review: int
    review_bucket: str


@dataclass(slots=True)
class UatReleaseArchiveRetentionQueue:
    generated_at: datetime
    review_window_days: int
    item_count: int
    overdue_count: int
    due_soon_count: int
    active_count: int
    superseded_count: int
    items: list[UatReleaseArchiveRetentionQueueItem]


@dataclass(slots=True)
class UatReleaseArchiveExportSummary:
    export_id: str
    archive_id: str
    export_name: str
    export_scope: str
    destination_system: str
    destination_reference: str | None
    handoff_status: str
    trigger_reason: str | None
    handoff_notes: str | None
    export_checksum: str
    exported_by: str
    exported_at: datetime
    delivery_confirmed_by: str | None
    delivery_confirmed_at: datetime | None
    next_retry_at: datetime | None
    retry_count: int
    last_status_updated_by: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(slots=True)
class UatReleaseArchiveExportDetail(UatReleaseArchiveExportSummary):
    export_payload: dict[str, object]


@dataclass(slots=True)
class UatReleaseArchiveExportDeliveryEventRecord:
    event_id: str
    export_id: str
    archive_id: str
    export_name: str
    archive_name: str
    event_type: str
    target_name: str
    delivery_channel: str | None
    external_reference: str | None
    event_notes: str | None
    occurred_at: datetime
    recorded_by: str
    created_at: datetime
    updated_at: datetime


@dataclass(slots=True)
class UatReleaseArchiveRetentionActionRecord:
    action_id: str
    archive_id: str
    related_export_id: str | None
    related_export_name: str | None
    related_export_status: str | None
    action_type: str
    previous_retention_review_at: datetime
    next_retention_review_at: datetime | None
    scheduled_retry_at: datetime | None
    action_notes: str | None
    recorded_by: str
    recorded_at: datetime
    created_at: datetime
    updated_at: datetime


def build_uat_release_archive_retention_queue(
    session: Session,
    *,
    review_window_days: int = 30,
    include_superseded: bool = False,
    include_active: bool = False,
) -> UatReleaseArchiveRetentionQueue:
    archive_models = session.scalars(
        select(UatReleaseArchive)
        .options(
            selectinload(UatReleaseArchive.snapshot),
            selectinload(UatReleaseArchive.export_records),
        )
        .order_by(UatReleaseArchive.retention_review_at.asc(), UatReleaseArchive.archive_id.asc())
    ).all()

    queue_items: list[UatReleaseArchiveRetentionQueueItem] = []
    overdue_count = 0
    due_soon_count = 0
    active_count = 0
    superseded_count = 0
    current_date = datetime.now(UTC).date()

    for archive in archive_models:
        review_at = _normalize_datetime(archive.retention_review_at)
        days_until_review = (review_at.date() - current_date).days
        retention_status = _derive_retention_status(archive)
        review_bucket = _derive_review_bucket(
            retention_status=retention_status,
            days_until_review=days_until_review,
            review_window_days=review_window_days,
        )

        if review_bucket == "overdue":
            overdue_count += 1
        elif review_bucket == "due_soon":
            due_soon_count += 1
        elif review_bucket == "active":
            active_count += 1
        else:
            superseded_count += 1

        if review_bucket == "superseded" and not include_superseded:
            continue
        if review_bucket == "active" and not include_active:
            continue

        latest_export = _get_latest_export(archive)
        queue_items.append(
            UatReleaseArchiveRetentionQueueItem(
                archive_id=str(archive.archive_id),
                snapshot_id=str(archive.snapshot_id),
                cycle_id=str(archive.snapshot.cycle_id),
                snapshot_name=archive.snapshot.snapshot_name,
                archive_name=archive.archive_name,
                recommended_outcome=archive.recommended_outcome,
                retention_review_at=review_at,
                retention_status=retention_status,
                support_handoff_owner=archive.support_handoff_owner,
                superseded_by_archive_id=(
                    str(archive.superseded_by_archive_id)
                    if archive.superseded_by_archive_id is not None
                    else None
                ),
                latest_export_name=latest_export.export_name if latest_export is not None else None,
                latest_exported_at=latest_export.exported_at if latest_export is not None else None,
                export_count=len(archive.export_records),
                days_until_review=days_until_review,
                review_bucket=review_bucket,
            )
        )

    return UatReleaseArchiveRetentionQueue(
        generated_at=datetime.now(UTC),
        review_window_days=review_window_days,
        item_count=len(queue_items),
        overdue_count=overdue_count,
        due_soon_count=due_soon_count,
        active_count=active_count,
        superseded_count=superseded_count,
        items=queue_items,
    )


def create_uat_release_archive_export(
    session: Session,
    *,
    archive_id: str,
    export_name: str,
    export_scope: str,
    destination_system: str,
    exported_by: str,
    destination_reference: str | None = None,
    trigger_reason: str | None = None,
    handoff_notes: str | None = None,
) -> UatReleaseArchiveExportDetail:
    archive = _get_archive(session, archive_id)
    archive_detail = get_uat_release_archive(session, archive_id)
    exported_at = datetime.now(UTC)
    export_payload = _build_export_payload(
        archive_detail,
        export_name=export_name,
        export_scope=export_scope,
        destination_system=destination_system,
        destination_reference=destination_reference,
        trigger_reason=trigger_reason,
        handoff_notes=handoff_notes,
        exported_by=exported_by,
        exported_at=exported_at,
    )
    export_json = json.dumps(export_payload, default=_json_default, sort_keys=True)

    export_record = UatReleaseArchiveExport(
        archive_id=archive.archive_id,
        export_name=export_name,
        export_scope=export_scope,
        destination_system=destination_system,
        destination_reference=destination_reference,
        handoff_status=UatReleaseArchiveExportHandoffStatus.PREPARED.value,
        trigger_reason=trigger_reason,
        handoff_notes=handoff_notes,
        export_payload=export_json,
        export_checksum=hashlib.sha256(export_json.encode("utf-8")).hexdigest(),
        exported_by=exported_by,
        exported_at=exported_at,
        retry_count=0,
        last_status_updated_by=exported_by,
    )
    session.add(export_record)

    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        raise UatReleaseArchiveExportConflictError(
            f"Release archive export `{export_name}` already exists for this archive."
        ) from exc

    return get_uat_release_archive_export(session, str(export_record.export_id))


def list_uat_release_archive_exports(
    session: Session,
    *,
    archive_id: str,
) -> list[UatReleaseArchiveExportSummary]:
    archive = _get_archive(session, archive_id)
    export_models = session.scalars(
        select(UatReleaseArchiveExport)
        .where(UatReleaseArchiveExport.archive_id == archive.archive_id)
        .order_by(
            UatReleaseArchiveExport.exported_at.desc(),
            UatReleaseArchiveExport.export_id.desc(),
        )
    ).all()
    return [_build_export_summary(export_model) for export_model in export_models]


def get_uat_release_archive_export(
    session: Session,
    export_id: str,
) -> UatReleaseArchiveExportDetail:
    export_record = _get_export_record(session, export_id)
    summary = _build_export_summary(export_record)
    return UatReleaseArchiveExportDetail(
        export_id=summary.export_id,
        archive_id=summary.archive_id,
        export_name=summary.export_name,
        export_scope=summary.export_scope,
        destination_system=summary.destination_system,
        destination_reference=summary.destination_reference,
        handoff_status=summary.handoff_status,
        trigger_reason=summary.trigger_reason,
        handoff_notes=summary.handoff_notes,
        export_checksum=summary.export_checksum,
        exported_by=summary.exported_by,
        exported_at=summary.exported_at,
        delivery_confirmed_by=summary.delivery_confirmed_by,
        delivery_confirmed_at=summary.delivery_confirmed_at,
        next_retry_at=summary.next_retry_at,
        retry_count=summary.retry_count,
        last_status_updated_by=summary.last_status_updated_by,
        created_at=summary.created_at,
        updated_at=summary.updated_at,
        export_payload=json.loads(export_record.export_payload),
    )


def update_uat_release_archive_export(
    session: Session,
    *,
    export_id: str,
    updated_by: str,
    handoff_status: UatReleaseArchiveExportHandoffStatus | None = None,
    destination_reference: str | None = None,
    handoff_notes: str | None = None,
    delivery_confirmed_by: str | None = None,
    next_retry_at: datetime | None = None,
) -> UatReleaseArchiveExportDetail:
    export_record = _get_export_record(session, export_id)
    now = datetime.now(UTC)
    normalized_next_retry_at = _normalize_datetime(next_retry_at) if next_retry_at else None

    if normalized_next_retry_at is not None and normalized_next_retry_at <= now:
        raise UatReleaseArchiveExportValidationError(
            "Export retry scheduling must use a future timestamp."
        )
    if (
        handoff_status is None
        and normalized_next_retry_at is not None
        and export_record.handoff_status
        not in {
            UatReleaseArchiveExportHandoffStatus.FOLLOW_UP_REQUIRED.value,
            UatReleaseArchiveExportHandoffStatus.RE_EXPORT_SCHEDULED.value,
        }
    ):
        raise UatReleaseArchiveExportValidationError(
            "Retry scheduling requires a follow-up export state."
        )
    if (
        handoff_status == UatReleaseArchiveExportHandoffStatus.RE_EXPORT_SCHEDULED
        and normalized_next_retry_at is None
    ):
        raise UatReleaseArchiveExportValidationError(
            "Re-export scheduling requires `next_retry_at`."
        )

    if destination_reference is not None:
        export_record.destination_reference = destination_reference
    if handoff_notes is not None:
        export_record.handoff_notes = handoff_notes
    if normalized_next_retry_at is not None:
        export_record.next_retry_at = normalized_next_retry_at

    if handoff_status is not None:
        export_record.handoff_status = handoff_status.value
        if handoff_status == UatReleaseArchiveExportHandoffStatus.PREPARED:
            export_record.delivery_confirmed_by = None
            export_record.delivery_confirmed_at = None
            export_record.next_retry_at = None
        elif handoff_status == UatReleaseArchiveExportHandoffStatus.DELIVERED:
            export_record.next_retry_at = None
            if delivery_confirmed_by is not None:
                export_record.delivery_confirmed_by = delivery_confirmed_by
                export_record.delivery_confirmed_at = now
        elif handoff_status == UatReleaseArchiveExportHandoffStatus.ACKNOWLEDGED:
            export_record.delivery_confirmed_by = delivery_confirmed_by or updated_by
            export_record.delivery_confirmed_at = now
            export_record.next_retry_at = None
        elif handoff_status == UatReleaseArchiveExportHandoffStatus.FOLLOW_UP_REQUIRED:
            if delivery_confirmed_by is not None:
                export_record.delivery_confirmed_by = delivery_confirmed_by
                export_record.delivery_confirmed_at = now
        elif handoff_status == UatReleaseArchiveExportHandoffStatus.RE_EXPORT_SCHEDULED:
            export_record.retry_count += 1

    final_handoff_status = (
        handoff_status.value if handoff_status is not None else export_record.handoff_status
    )
    if delivery_confirmed_by is not None and final_handoff_status in {
        UatReleaseArchiveExportHandoffStatus.ACKNOWLEDGED.value,
        UatReleaseArchiveExportHandoffStatus.FOLLOW_UP_REQUIRED.value,
    }:
        export_record.delivery_confirmed_by = delivery_confirmed_by
        export_record.delivery_confirmed_at = export_record.delivery_confirmed_at or now

    export_record.last_status_updated_by = updated_by
    session.commit()
    return get_uat_release_archive_export(session, str(export_record.export_id))


def record_uat_release_archive_export_delivery_event(
    session: Session,
    *,
    export_id: str,
    event_type: UatReleaseArchiveExportDeliveryEventType,
    recorded_by: str,
    target_name: str,
    delivery_channel: str | None = None,
    external_reference: str | None = None,
    event_notes: str | None = None,
    occurred_at: datetime | None = None,
) -> UatReleaseArchiveExportDeliveryEventRecord:
    export_record = session.scalar(
        select(UatReleaseArchiveExport)
        .options(selectinload(UatReleaseArchiveExport.archive))
        .where(UatReleaseArchiveExport.export_id == UUID(export_id))
    )
    if export_record is None:
        raise UatReleaseArchiveExportNotFoundError(
            f"UAT release archive export `{export_id}` was not found."
        )

    now = datetime.now(UTC)
    event_time = _normalize_datetime(occurred_at or now)
    if event_time > now:
        raise UatReleaseArchiveExportDeliveryEventValidationError(
            "Delivery events cannot be recorded in the future."
        )
    _validate_delivery_event_preconditions(
        session,
        export_record=export_record,
        export_id=export_id,
        event_type=event_type,
        occurred_at=event_time,
    )

    event_record = UatReleaseArchiveExportDeliveryEvent(
        archive_id=export_record.archive_id,
        export_id=export_record.export_id,
        event_type=event_type.value,
        target_name=target_name,
        delivery_channel=delivery_channel,
        external_reference=external_reference,
        event_notes=event_notes,
        occurred_at=event_time,
        recorded_by=recorded_by,
    )
    session.add(event_record)

    if event_type == UatReleaseArchiveExportDeliveryEventType.EXTERNAL_HANDOFF_LOGGED:
        if export_record.handoff_status != UatReleaseArchiveExportHandoffStatus.ACKNOWLEDGED.value:
            export_record.handoff_status = UatReleaseArchiveExportHandoffStatus.DELIVERED.value
        if external_reference is not None:
            export_record.destination_reference = external_reference
        export_record.delivery_confirmed_by = target_name
        export_record.delivery_confirmed_at = event_time
        export_record.next_retry_at = None
        export_record.handoff_notes = _append_delivery_event_note(
            export_record.handoff_notes,
            event_type=event_type,
            target_name=target_name,
            delivery_channel=delivery_channel,
            external_reference=external_reference,
            event_notes=event_notes,
            occurred_at=event_time,
        )
    elif event_type == UatReleaseArchiveExportDeliveryEventType.SUPPORT_HANDBACK_ACKNOWLEDGED:
        export_record.handoff_status = UatReleaseArchiveExportHandoffStatus.ACKNOWLEDGED.value
        export_record.delivery_confirmed_by = target_name
        export_record.delivery_confirmed_at = event_time
        export_record.next_retry_at = None
        export_record.handoff_notes = _append_delivery_event_note(
            export_record.handoff_notes,
            event_type=event_type,
            target_name=target_name,
            delivery_channel=delivery_channel,
            external_reference=external_reference,
            event_notes=event_notes,
            occurred_at=event_time,
        )
    elif event_type in {
        UatReleaseArchiveExportDeliveryEventType.ESCALATION_OUTCOME_RECORDED,
        UatReleaseArchiveExportDeliveryEventType.CLOSURE_CONFIRMED,
    }:
        if event_type == UatReleaseArchiveExportDeliveryEventType.CLOSURE_CONFIRMED:
            export_record.handoff_status = UatReleaseArchiveExportHandoffStatus.ACKNOWLEDGED.value
            export_record.delivery_confirmed_by = export_record.delivery_confirmed_by or target_name
            export_record.delivery_confirmed_at = (
                export_record.delivery_confirmed_at or event_time
            )
            export_record.next_retry_at = None
        export_record.handoff_notes = _append_delivery_event_note(
            export_record.handoff_notes,
            event_type=event_type,
            target_name=target_name,
            delivery_channel=delivery_channel,
            external_reference=external_reference,
            event_notes=event_notes,
            occurred_at=event_time,
        )

    export_record.last_status_updated_by = recorded_by
    session.commit()
    return _build_delivery_event_record(event_record)


def list_uat_release_archive_export_delivery_events(
    session: Session,
    *,
    export_id: str,
) -> list[UatReleaseArchiveExportDeliveryEventRecord]:
    export_record = _get_export_record(session, export_id)
    event_models = session.scalars(
        select(UatReleaseArchiveExportDeliveryEvent)
        .options(
            selectinload(UatReleaseArchiveExportDeliveryEvent.export),
            selectinload(UatReleaseArchiveExportDeliveryEvent.archive),
        )
        .where(UatReleaseArchiveExportDeliveryEvent.export_id == export_record.export_id)
        .order_by(
            UatReleaseArchiveExportDeliveryEvent.occurred_at.desc(),
            UatReleaseArchiveExportDeliveryEvent.event_id.desc(),
        )
    ).all()
    return [_build_delivery_event_record(event_model) for event_model in event_models]


def create_uat_release_archive_retention_action(
    session: Session,
    *,
    archive_id: str,
    action_type: UatReleaseArchiveRetentionActionType,
    recorded_by: str,
    action_notes: str | None = None,
    next_retention_review_at: datetime | None = None,
    related_export_id: str | None = None,
    scheduled_retry_at: datetime | None = None,
) -> UatReleaseArchiveRetentionActionRecord:
    archive = _get_archive(session, archive_id)
    related_export = _get_related_export(session, archive, related_export_id)
    now = datetime.now(UTC)
    current_review_at = _normalize_datetime(archive.retention_review_at)
    normalized_next_review_at = (
        _normalize_datetime(next_retention_review_at) if next_retention_review_at else None
    )
    normalized_scheduled_retry_at = (
        _normalize_datetime(scheduled_retry_at) if scheduled_retry_at else None
    )

    if normalized_next_review_at is not None and normalized_next_review_at <= now:
        raise UatReleaseArchiveRetentionActionValidationError(
            "Retention review updates must use a future timestamp."
        )
    if normalized_scheduled_retry_at is not None and normalized_scheduled_retry_at <= now:
        raise UatReleaseArchiveRetentionActionValidationError(
            "Scheduled re-export timestamps must be in the future."
        )
    if action_type in {
        UatReleaseArchiveRetentionActionType.REVIEW_COMPLETED,
        UatReleaseArchiveRetentionActionType.RETENTION_EXTENDED,
    } and normalized_next_review_at is None:
        raise UatReleaseArchiveRetentionActionValidationError(
            "Retention review actions require `next_retention_review_at`."
        )
    if (
        action_type == UatReleaseArchiveRetentionActionType.RETENTION_EXTENDED
        and normalized_next_review_at is not None
        and normalized_next_review_at <= current_review_at
    ):
        raise UatReleaseArchiveRetentionActionValidationError(
            "Retention extensions must move the review date forward."
        )
    if action_type == UatReleaseArchiveRetentionActionType.RE_EXPORT_REQUESTED:
        if related_export is None:
            raise UatReleaseArchiveRetentionActionValidationError(
                "Re-export retention actions require `related_export_id`."
            )
        related_export.last_status_updated_by = recorded_by
        if normalized_scheduled_retry_at is not None:
            related_export.handoff_status = (
                UatReleaseArchiveExportHandoffStatus.RE_EXPORT_SCHEDULED.value
            )
            related_export.next_retry_at = normalized_scheduled_retry_at
            related_export.retry_count += 1
        else:
            related_export.handoff_status = (
                UatReleaseArchiveExportHandoffStatus.FOLLOW_UP_REQUIRED.value
            )
            related_export.next_retry_at = None

    if normalized_next_review_at is not None:
        archive.retention_review_at = normalized_next_review_at

    action_record = UatReleaseArchiveRetentionAction(
        archive_id=archive.archive_id,
        related_export_id=related_export.export_id if related_export is not None else None,
        action_type=action_type.value,
        previous_retention_review_at=current_review_at,
        next_retention_review_at=normalized_next_review_at,
        scheduled_retry_at=normalized_scheduled_retry_at,
        action_notes=action_notes,
        recorded_by=recorded_by,
        recorded_at=now,
    )
    action_record.related_export = related_export
    session.add(action_record)
    session.commit()
    return _build_retention_action_record(action_record)


def list_uat_release_archive_retention_actions(
    session: Session,
    *,
    archive_id: str,
) -> list[UatReleaseArchiveRetentionActionRecord]:
    archive = _get_archive(session, archive_id)
    action_models = session.scalars(
        select(UatReleaseArchiveRetentionAction)
        .options(selectinload(UatReleaseArchiveRetentionAction.related_export))
        .where(UatReleaseArchiveRetentionAction.archive_id == archive.archive_id)
        .order_by(
            UatReleaseArchiveRetentionAction.recorded_at.desc(),
            UatReleaseArchiveRetentionAction.action_id.desc(),
        )
    ).all()
    return [_build_retention_action_record(action_model) for action_model in action_models]


def _build_export_summary(export_record: UatReleaseArchiveExport) -> UatReleaseArchiveExportSummary:
    return UatReleaseArchiveExportSummary(
        export_id=str(export_record.export_id),
        archive_id=str(export_record.archive_id),
        export_name=export_record.export_name,
        export_scope=export_record.export_scope,
        destination_system=export_record.destination_system,
        destination_reference=export_record.destination_reference,
        handoff_status=export_record.handoff_status,
        trigger_reason=export_record.trigger_reason,
        handoff_notes=export_record.handoff_notes,
        export_checksum=export_record.export_checksum,
        exported_by=export_record.exported_by,
        exported_at=_normalize_datetime(export_record.exported_at),
        delivery_confirmed_by=export_record.delivery_confirmed_by,
        delivery_confirmed_at=_normalize_optional_datetime(export_record.delivery_confirmed_at),
        next_retry_at=_normalize_optional_datetime(export_record.next_retry_at),
        retry_count=export_record.retry_count,
        last_status_updated_by=export_record.last_status_updated_by,
        created_at=_normalize_datetime(export_record.created_at),
        updated_at=_normalize_datetime(export_record.updated_at),
    )


def _build_retention_action_record(
    action_record: UatReleaseArchiveRetentionAction,
) -> UatReleaseArchiveRetentionActionRecord:
    return UatReleaseArchiveRetentionActionRecord(
        action_id=str(action_record.action_id),
        archive_id=str(action_record.archive_id),
        related_export_id=(
            str(action_record.related_export_id)
            if action_record.related_export_id is not None
            else None
        ),
        related_export_name=(
            action_record.related_export.export_name
            if action_record.related_export is not None
            else None
        ),
        related_export_status=(
            action_record.related_export.handoff_status
            if action_record.related_export is not None
            else None
        ),
        action_type=action_record.action_type,
        previous_retention_review_at=_normalize_datetime(
            action_record.previous_retention_review_at
        ),
        next_retention_review_at=_normalize_optional_datetime(
            action_record.next_retention_review_at
        ),
        scheduled_retry_at=_normalize_optional_datetime(action_record.scheduled_retry_at),
        action_notes=action_record.action_notes,
        recorded_by=action_record.recorded_by,
        recorded_at=_normalize_datetime(action_record.recorded_at),
        created_at=_normalize_datetime(action_record.created_at),
        updated_at=_normalize_datetime(action_record.updated_at),
    )


def _build_delivery_event_record(
    event_record: UatReleaseArchiveExportDeliveryEvent,
) -> UatReleaseArchiveExportDeliveryEventRecord:
    return UatReleaseArchiveExportDeliveryEventRecord(
        event_id=str(event_record.event_id),
        export_id=str(event_record.export_id),
        archive_id=str(event_record.archive_id),
        export_name=event_record.export.export_name,
        archive_name=event_record.archive.archive_name,
        event_type=event_record.event_type,
        target_name=event_record.target_name,
        delivery_channel=event_record.delivery_channel,
        external_reference=event_record.external_reference,
        event_notes=event_record.event_notes,
        occurred_at=_normalize_datetime(event_record.occurred_at),
        recorded_by=event_record.recorded_by,
        created_at=_normalize_datetime(event_record.created_at),
        updated_at=_normalize_datetime(event_record.updated_at),
    )


def _build_export_payload(
    archive_detail: UatReleaseArchiveDetail,
    *,
    export_name: str,
    export_scope: str,
    destination_system: str,
    destination_reference: str | None,
    trigger_reason: str | None,
    handoff_notes: str | None,
    exported_by: str,
    exported_at: datetime,
) -> dict[str, object]:
    return {
        "report_version": "phase7-release-archive-export-v1",
        "export_scope": export_scope,
        "export_name": export_name,
        "exported_at": exported_at,
        "exported_by": exported_by,
        "destination": {
            "system": destination_system,
            "reference": destination_reference,
            "handoff_status": UatReleaseArchiveExportHandoffStatus.PREPARED.value,
        },
        "trigger_reason": trigger_reason,
        "handoff_notes": handoff_notes,
        "archive": asdict(archive_detail),
    }


def _get_archive(session: Session, archive_id: str) -> UatReleaseArchive:
    archive = session.scalar(
        select(UatReleaseArchive)
        .options(
            selectinload(UatReleaseArchive.snapshot),
            selectinload(UatReleaseArchive.export_records),
        )
        .where(UatReleaseArchive.archive_id == UUID(archive_id))
    )
    if archive is None:
        raise UatReleaseArchiveNotFoundError(
            f"UAT release archive `{archive_id}` was not found."
        )
    return archive


def _get_export_record(session: Session, export_id: str) -> UatReleaseArchiveExport:
    export_record = session.get(UatReleaseArchiveExport, UUID(export_id))
    if export_record is None:
        raise UatReleaseArchiveExportNotFoundError(
            f"UAT release archive export `{export_id}` was not found."
        )
    return export_record


def _get_related_export(
    session: Session,
    archive: UatReleaseArchive,
    related_export_id: str | None,
) -> UatReleaseArchiveExport | None:
    if related_export_id is None:
        return None
    export_record = _get_export_record(session, related_export_id)
    if export_record.archive_id != archive.archive_id:
        raise UatReleaseArchiveRetentionActionValidationError(
            "Retention actions can only reference exports from the same release archive."
        )
    return export_record


def _derive_retention_status(archive: UatReleaseArchive) -> str:
    if archive.superseded_by_archive_id is not None:
        return "superseded"
    if _normalize_datetime(archive.retention_review_at) <= datetime.now(UTC):
        return "review_due"
    return "active"


def _derive_review_bucket(
    *,
    retention_status: str,
    days_until_review: int,
    review_window_days: int,
) -> str:
    if retention_status == "superseded":
        return "superseded"
    if days_until_review < 0:
        return "overdue"
    if days_until_review <= review_window_days:
        return "due_soon"
    return "active"


def _get_latest_export(archive: UatReleaseArchive) -> UatReleaseArchiveExport | None:
    if not archive.export_records:
        return None
    return max(
        archive.export_records,
        key=lambda export_record: (
            _normalize_datetime(export_record.exported_at),
            str(export_record.export_id),
        ),
    )


def _has_prior_delivery_event(
    session: Session,
    *,
    export_id: str,
    event_type: UatReleaseArchiveExportDeliveryEventType,
    occurred_at: datetime,
) -> bool:
    prior_event = session.scalar(
        select(UatReleaseArchiveExportDeliveryEvent.event_id)
        .where(
            UatReleaseArchiveExportDeliveryEvent.export_id == UUID(export_id),
            UatReleaseArchiveExportDeliveryEvent.event_type == event_type.value,
            UatReleaseArchiveExportDeliveryEvent.occurred_at <= occurred_at,
        )
        .limit(1)
    )
    return prior_event is not None


def _validate_delivery_event_preconditions(
    session: Session,
    *,
    export_record: UatReleaseArchiveExport,
    export_id: str,
    event_type: UatReleaseArchiveExportDeliveryEventType,
    occurred_at: datetime,
) -> None:
    if (
        event_type == UatReleaseArchiveExportDeliveryEventType.NOTIFICATION_ACKNOWLEDGED
        and not _has_prior_delivery_event(
            session,
            export_id=export_id,
            event_type=UatReleaseArchiveExportDeliveryEventType.NOTIFICATION_SENT,
            occurred_at=occurred_at,
        )
    ):
        raise UatReleaseArchiveExportDeliveryEventValidationError(
            "Notification acknowledgement requires a prior notification_sent event."
        )
    if (
        event_type == UatReleaseArchiveExportDeliveryEventType.ESCALATION_OUTCOME_RECORDED
        and not _has_prior_delivery_event(
            session,
            export_id=export_id,
            event_type=UatReleaseArchiveExportDeliveryEventType.NOTIFICATION_SENT,
            occurred_at=occurred_at,
        )
    ):
        raise UatReleaseArchiveExportDeliveryEventValidationError(
            "Escalation outcome journaling requires a prior notification_sent event."
        )
    if (
        event_type == UatReleaseArchiveExportDeliveryEventType.SUPPORT_HANDBACK_ACKNOWLEDGED
        and not _has_prior_delivery_event(
            session,
            export_id=export_id,
            event_type=UatReleaseArchiveExportDeliveryEventType.EXTERNAL_HANDOFF_LOGGED,
            occurred_at=occurred_at,
        )
    ):
        raise UatReleaseArchiveExportDeliveryEventValidationError(
            "Support handback acknowledgement requires a prior external_handoff_logged event."
        )
    if (
        event_type == UatReleaseArchiveExportDeliveryEventType.CLOSURE_CONFIRMED
        and export_record.handoff_status != UatReleaseArchiveExportHandoffStatus.ACKNOWLEDGED.value
        and not _has_prior_delivery_event(
            session,
            export_id=export_id,
            event_type=UatReleaseArchiveExportDeliveryEventType.SUPPORT_HANDBACK_ACKNOWLEDGED,
            occurred_at=occurred_at,
        )
    ):
        raise UatReleaseArchiveExportDeliveryEventValidationError(
            "Closure confirmation requires a prior support_handback_acknowledged event."
        )


def _append_delivery_event_note(
    existing_notes: str | None,
    *,
    event_type: UatReleaseArchiveExportDeliveryEventType,
    target_name: str,
    delivery_channel: str | None,
    external_reference: str | None,
    event_notes: str | None,
    occurred_at: datetime,
) -> str:
    if event_type == UatReleaseArchiveExportDeliveryEventType.EXTERNAL_HANDOFF_LOGGED:
        summary = f"External handoff logged for {target_name}"
    elif event_type == UatReleaseArchiveExportDeliveryEventType.ESCALATION_OUTCOME_RECORDED:
        summary = f"Escalation outcome recorded with {target_name}"
    elif event_type == UatReleaseArchiveExportDeliveryEventType.SUPPORT_HANDBACK_ACKNOWLEDGED:
        summary = f"Support handback acknowledged by {target_name}"
    elif event_type == UatReleaseArchiveExportDeliveryEventType.CLOSURE_CONFIRMED:
        summary = f"Closure confirmed by {target_name}"
    else:
        summary = f"Delivery event recorded for {target_name}"
    if delivery_channel:
        summary = f"{summary} via {delivery_channel}"
    if external_reference:
        summary = f"{summary} ({external_reference})"
    summary = f"{summary} at {occurred_at.isoformat()}."
    if event_notes:
        summary = f"{summary} {event_notes}"
    if existing_notes:
        return f"{existing_notes}\n{summary}"
    return summary


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _normalize_optional_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return _normalize_datetime(value)


def _json_default(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable.")
