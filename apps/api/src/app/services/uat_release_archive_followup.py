from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.db.models.enums import (
    UatReleaseArchiveExportHandoffStatus,
    UatReleaseArchiveRetentionActionType,
)
from app.db.models.uat import (
    UatReleaseArchive,
    UatReleaseArchiveExport,
    UatReleaseArchiveRetentionAction,
)
from app.services.uat_release_archive import UatReleaseArchiveNotFoundError
from app.services.uat_release_archive_operations import (
    UatReleaseArchiveRetentionActionValidationError,
    create_uat_release_archive_retention_action,
)

BULK_SUPPORTED_ACTION_TYPES = {
    UatReleaseArchiveRetentionActionType.REVIEW_COMPLETED,
    UatReleaseArchiveRetentionActionType.RETENTION_EXTENDED,
}


class UatReleaseArchiveFollowupValidationError(ValueError):
    """Raised when a release-archive follow-up request is invalid."""


@dataclass(slots=True)
class UatReleaseArchiveFollowupDashboardItem:
    archive_id: str
    snapshot_id: str
    cycle_id: str
    snapshot_name: str
    archive_name: str
    retention_review_at: datetime
    retention_status: str
    days_until_review: int
    latest_export_name: str | None
    latest_handoff_status: str | None
    next_retry_at: datetime | None
    latest_retention_action_type: str | None
    latest_retention_action_at: datetime | None
    follow_up_export_count: int
    attention_reasons: list[str]


@dataclass(slots=True)
class UatReleaseArchiveFollowupDashboard:
    generated_at: datetime
    review_window_days: int
    total_archive_count: int
    action_required_count: int
    overdue_review_count: int
    due_soon_review_count: int
    acknowledgement_pending_count: int
    follow_up_export_count: int
    re_export_scheduled_count: int
    re_export_due_count: int
    items: list[UatReleaseArchiveFollowupDashboardItem]


@dataclass(slots=True)
class BulkUatReleaseArchiveRetentionActionResult:
    archive_id: str
    archive_name: str | None
    applied: bool
    action_id: str | None
    action_type: str
    retention_review_at: datetime | None
    error: str | None


@dataclass(slots=True)
class BulkUatReleaseArchiveRetentionActionOutcome:
    requested_count: int
    applied_count: int
    failed_count: int
    results: list[BulkUatReleaseArchiveRetentionActionResult]


def build_uat_release_archive_followup_dashboard(
    session: Session,
    *,
    review_window_days: int = 30,
    include_resolved: bool = False,
    as_of: datetime | None = None,
) -> UatReleaseArchiveFollowupDashboard:
    reference_time = _normalize_datetime(as_of or datetime.now(UTC))
    archive_models = session.scalars(
        select(UatReleaseArchive)
        .options(
            selectinload(UatReleaseArchive.snapshot),
            selectinload(UatReleaseArchive.export_records),
            selectinload(UatReleaseArchive.retention_actions),
        )
        .order_by(UatReleaseArchive.retention_review_at.asc(), UatReleaseArchive.archive_id.asc())
    ).all()

    current_date = reference_time.date()
    items: list[UatReleaseArchiveFollowupDashboardItem] = []
    action_required_count = 0
    overdue_review_count = 0
    due_soon_review_count = 0
    acknowledgement_pending_count = 0
    follow_up_export_count = 0
    re_export_scheduled_count = 0
    re_export_due_count = 0

    for archive in archive_models:
        review_at = _normalize_datetime(archive.retention_review_at)
        retention_status = _derive_retention_status(archive, reference_time=reference_time)
        days_until_review = (review_at.date() - current_date).days
        review_bucket = _derive_review_bucket(
            retention_status=retention_status,
            days_until_review=days_until_review,
            review_window_days=review_window_days,
        )
        export_records = list(archive.export_records)
        latest_export = _get_latest_export(export_records)
        latest_action = _get_latest_retention_action(list(archive.retention_actions))
        scheduled_retry_records = [
            export_record
            for export_record in export_records
            if export_record.handoff_status
            == UatReleaseArchiveExportHandoffStatus.RE_EXPORT_SCHEDULED.value
        ]
        follow_up_records = [
            export_record
            for export_record in export_records
            if export_record.handoff_status
            in {
                UatReleaseArchiveExportHandoffStatus.FOLLOW_UP_REQUIRED.value,
                UatReleaseArchiveExportHandoffStatus.RE_EXPORT_SCHEDULED.value,
            }
        ]
        due_retry_records = [
            export_record
            for export_record in scheduled_retry_records
            if export_record.next_retry_at is not None
            and _normalize_datetime(export_record.next_retry_at) <= reference_time
        ]

        attention_reasons: list[str] = []
        if review_bucket == "overdue":
            overdue_review_count += 1
            attention_reasons.append("overdue_review")
        elif review_bucket == "due_soon":
            due_soon_review_count += 1
            attention_reasons.append("due_soon_review")

        if latest_export is not None and latest_export.handoff_status in {
            UatReleaseArchiveExportHandoffStatus.PREPARED.value,
            UatReleaseArchiveExportHandoffStatus.DELIVERED.value,
        }:
            acknowledgement_pending_count += 1
            attention_reasons.append("acknowledgement_pending")

        if follow_up_records:
            follow_up_export_count += len(follow_up_records)
            attention_reasons.append("follow_up_required")
        if scheduled_retry_records:
            re_export_scheduled_count += len(scheduled_retry_records)
            attention_reasons.append("re_export_scheduled")
        if due_retry_records:
            re_export_due_count += len(due_retry_records)
            attention_reasons.append("re_export_due")

        attention_reasons = list(dict.fromkeys(attention_reasons))
        if attention_reasons:
            action_required_count += 1
        elif not include_resolved:
            continue

        items.append(
            UatReleaseArchiveFollowupDashboardItem(
                archive_id=str(archive.archive_id),
                snapshot_id=str(archive.snapshot_id),
                cycle_id=str(archive.snapshot.cycle_id),
                snapshot_name=archive.snapshot.snapshot_name,
                archive_name=archive.archive_name,
                retention_review_at=review_at,
                retention_status=retention_status,
                days_until_review=days_until_review,
                latest_export_name=latest_export.export_name if latest_export is not None else None,
                latest_handoff_status=(
                    latest_export.handoff_status if latest_export is not None else None
                ),
                next_retry_at=_get_next_retry_at(scheduled_retry_records),
                latest_retention_action_type=(
                    latest_action.action_type if latest_action is not None else None
                ),
                latest_retention_action_at=(
                    _normalize_datetime(latest_action.recorded_at)
                    if latest_action is not None
                    else None
                ),
                follow_up_export_count=len(follow_up_records),
                attention_reasons=attention_reasons,
            )
        )

    items.sort(
        key=lambda item: (
            _priority(item.attention_reasons),
            item.days_until_review,
            item.archive_name,
        )
    )
    return UatReleaseArchiveFollowupDashboard(
        generated_at=datetime.now(UTC),
        review_window_days=review_window_days,
        total_archive_count=len(archive_models),
        action_required_count=action_required_count,
        overdue_review_count=overdue_review_count,
        due_soon_review_count=due_soon_review_count,
        acknowledgement_pending_count=acknowledgement_pending_count,
        follow_up_export_count=follow_up_export_count,
        re_export_scheduled_count=re_export_scheduled_count,
        re_export_due_count=re_export_due_count,
        items=items,
    )


def apply_bulk_uat_release_archive_retention_action(
    session: Session,
    *,
    archive_ids: list[str],
    action_type: UatReleaseArchiveRetentionActionType,
    recorded_by: str,
    action_notes: str | None = None,
    next_retention_review_at: datetime | None = None,
) -> BulkUatReleaseArchiveRetentionActionOutcome:
    if action_type not in BULK_SUPPORTED_ACTION_TYPES:
        raise UatReleaseArchiveFollowupValidationError(
            "Bulk retention handling only supports review_completed and retention_extended."
        )

    results: list[BulkUatReleaseArchiveRetentionActionResult] = []
    seen_archive_ids: set[str] = set()

    for archive_id in archive_ids:
        if archive_id in seen_archive_ids:
            results.append(
                BulkUatReleaseArchiveRetentionActionResult(
                    archive_id=archive_id,
                    archive_name=None,
                    applied=False,
                    action_id=None,
                    action_type=action_type.value,
                    retention_review_at=None,
                    error="Duplicate archive id in bulk request.",
                )
            )
            continue
        seen_archive_ids.add(archive_id)

        try:
            action_record = create_uat_release_archive_retention_action(
                session,
                archive_id=archive_id,
                action_type=action_type,
                recorded_by=recorded_by,
                action_notes=action_notes,
                next_retention_review_at=next_retention_review_at,
            )
            archive = _get_archive_model(session, archive_id)
            results.append(
                BulkUatReleaseArchiveRetentionActionResult(
                    archive_id=archive_id,
                    archive_name=archive.archive_name,
                    applied=True,
                    action_id=action_record.action_id,
                    action_type=action_type.value,
                    retention_review_at=_normalize_datetime(archive.retention_review_at),
                    error=None,
                )
            )
        except (
            UatReleaseArchiveNotFoundError,
            UatReleaseArchiveRetentionActionValidationError,
            ValueError,
        ) as exc:
            results.append(
                BulkUatReleaseArchiveRetentionActionResult(
                    archive_id=archive_id,
                    archive_name=None,
                    applied=False,
                    action_id=None,
                    action_type=action_type.value,
                    retention_review_at=None,
                    error=str(exc),
                )
            )

    applied_count = sum(1 for result in results if result.applied)
    failed_count = len(results) - applied_count
    return BulkUatReleaseArchiveRetentionActionOutcome(
        requested_count=len(archive_ids),
        applied_count=applied_count,
        failed_count=failed_count,
        results=results,
    )


def _get_archive_model(session: Session, archive_id: str) -> UatReleaseArchive:
    archive = session.scalar(
        select(UatReleaseArchive)
        .options(selectinload(UatReleaseArchive.snapshot))
        .where(UatReleaseArchive.archive_id == UUID(archive_id))
    )
    if archive is None:
        raise UatReleaseArchiveNotFoundError(
            f"UAT release archive `{archive_id}` was not found."
        )
    return archive


def _derive_retention_status(
    archive: UatReleaseArchive,
    *,
    reference_time: datetime,
) -> str:
    if archive.superseded_by_archive_id is not None:
        return "superseded"
    if _normalize_datetime(archive.retention_review_at) <= reference_time:
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


def _get_latest_retention_action(
    action_records: list[UatReleaseArchiveRetentionAction],
) -> UatReleaseArchiveRetentionAction | None:
    if not action_records:
        return None
    return max(
        action_records,
        key=lambda action_record: (
            _normalize_datetime(action_record.recorded_at),
            str(action_record.action_id),
        ),
    )


def _get_next_retry_at(export_records: list[UatReleaseArchiveExport]) -> datetime | None:
    retry_datetimes = [
        _normalize_datetime(export_record.next_retry_at)
        for export_record in export_records
        if export_record.next_retry_at is not None
    ]
    if not retry_datetimes:
        return None
    return min(retry_datetimes)


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _priority(attention_reasons: list[str]) -> int:
    if "overdue_review" in attention_reasons:
        return 0
    if "re_export_due" in attention_reasons:
        return 1
    if "due_soon_review" in attention_reasons:
        return 2
    if "re_export_scheduled" in attention_reasons:
        return 3
    if "follow_up_required" in attention_reasons:
        return 4
    if "acknowledgement_pending" in attention_reasons:
        return 5
    return 6
