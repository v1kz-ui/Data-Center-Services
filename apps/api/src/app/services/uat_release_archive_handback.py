from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.db.models.uat import UatReleaseArchive
from app.services.uat_release_archive_delivery_ledger import (
    UatReleaseArchiveDeliveryLedgerItem,
    build_uat_release_archive_delivery_ledger,
)


@dataclass(slots=True)
class UatReleaseArchiveSupportHandbackArchiveItem:
    archive_id: str
    snapshot_id: str
    cycle_id: str
    snapshot_name: str
    archive_name: str
    owner_name: str
    support_handoff_summary: str
    operations_runbook_reference: str | None
    latest_export_id: str | None
    latest_export_name: str | None
    latest_export_status: str | None
    destination_system: str | None
    destination_reference: str | None
    notification_acknowledgement_status: str
    escalation_status: str
    closure_status: str
    closure_ready: bool
    closure_blockers: list[str]
    attention_reasons: list[str]
    recommended_action: str
    handback_summary: str


@dataclass(slots=True)
class UatReleaseArchiveSupportHandbackOwner:
    owner_name: str
    archive_count: int
    closure_ready_count: int
    unresolved_count: int
    pending_support_confirmation_count: int
    remediation_in_progress_count: int
    blocked_count: int
    superseded_count: int
    archives: list[UatReleaseArchiveSupportHandbackArchiveItem]
    summary_message: str


@dataclass(slots=True)
class UatReleaseArchiveSupportHandbackReport:
    report_version: str
    generated_at: datetime
    generated_by: str
    as_of: datetime
    review_window_days: int
    stale_reply_after_hours: int
    total_archive_count: int
    included_archive_count: int
    owner_count: int
    closure_ready_count: int
    unresolved_count: int
    pending_support_confirmation_count: int
    remediation_in_progress_count: int
    blocked_count: int
    superseded_count: int
    owners: list[UatReleaseArchiveSupportHandbackOwner]


def build_uat_release_archive_support_handback_report(
    session: Session,
    *,
    generated_by: str,
    review_window_days: int = 30,
    stale_reply_after_hours: int = 48,
    include_resolved: bool = True,
    as_of: datetime | None = None,
) -> UatReleaseArchiveSupportHandbackReport:
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
        .options(selectinload(UatReleaseArchive.snapshot))
        .order_by(UatReleaseArchive.archive_name.asc(), UatReleaseArchive.archive_id.asc())
    ).all()

    owner_archives: dict[str, list[UatReleaseArchiveSupportHandbackArchiveItem]] = {}
    total_archive_count = 0
    included_archive_count = 0
    closure_ready_count = 0
    pending_support_confirmation_count = 0
    remediation_in_progress_count = 0
    blocked_count = 0
    superseded_count = 0

    for archive in archive_models:
        total_archive_count += 1
        ledger_item = ledger_items_by_archive_id[str(archive.archive_id)]
        closure_status = _derive_closure_status(
            archive=archive,
            ledger_item=ledger_item,
        )
        closure_blockers = _build_closure_blockers(ledger_item.attention_reasons)
        closure_ready = closure_status in {"ready_for_handback", "superseded"}

        if not include_resolved and closure_ready:
            continue

        owner_name = archive.support_handoff_owner or "Unassigned"
        item = UatReleaseArchiveSupportHandbackArchiveItem(
            archive_id=str(archive.archive_id),
            snapshot_id=str(archive.snapshot_id),
            cycle_id=str(archive.snapshot.cycle_id),
            snapshot_name=archive.snapshot.snapshot_name,
            archive_name=archive.archive_name,
            owner_name=owner_name,
            support_handoff_summary=archive.support_handoff_summary,
            operations_runbook_reference=archive.operations_runbook_reference,
            latest_export_id=ledger_item.latest_export_id,
            latest_export_name=ledger_item.latest_export_name,
            latest_export_status=ledger_item.latest_export_status,
            destination_system=ledger_item.destination_system,
            destination_reference=ledger_item.destination_reference,
            notification_acknowledgement_status=ledger_item.notification_acknowledgement_status,
            escalation_status=ledger_item.escalation_status,
            closure_status=closure_status,
            closure_ready=closure_ready,
            closure_blockers=closure_blockers,
            attention_reasons=ledger_item.attention_reasons,
            recommended_action=ledger_item.recommended_action,
            handback_summary=_build_handback_summary(
                archive_name=archive.archive_name,
                closure_status=closure_status,
                recommended_action=ledger_item.recommended_action,
                closure_blockers=closure_blockers,
            ),
        )
        owner_archives.setdefault(owner_name, []).append(item)
        included_archive_count += 1

        if closure_status == "ready_for_handback":
            closure_ready_count += 1
        elif closure_status == "pending_support_confirmation":
            pending_support_confirmation_count += 1
        elif closure_status == "remediation_in_progress":
            remediation_in_progress_count += 1
        elif closure_status == "blocked":
            blocked_count += 1
        elif closure_status == "superseded":
            superseded_count += 1

    owners: list[UatReleaseArchiveSupportHandbackOwner] = []
    for owner_name, archive_items in sorted(owner_archives.items()):
        archive_items.sort(
            key=lambda item: (_closure_priority(item.closure_status), item.archive_name)
        )
        owner_ready_count = sum(
            1 for item in archive_items if item.closure_status == "ready_for_handback"
        )
        owner_pending_count = sum(
            1 for item in archive_items if item.closure_status == "pending_support_confirmation"
        )
        owner_remediation_count = sum(
            1 for item in archive_items if item.closure_status == "remediation_in_progress"
        )
        owner_blocked_count = sum(
            1 for item in archive_items if item.closure_status == "blocked"
        )
        owner_superseded_count = sum(
            1 for item in archive_items if item.closure_status == "superseded"
        )
        owner_unresolved_count = len(archive_items) - owner_ready_count - owner_superseded_count
        owners.append(
            UatReleaseArchiveSupportHandbackOwner(
                owner_name=owner_name,
                archive_count=len(archive_items),
                closure_ready_count=owner_ready_count,
                unresolved_count=owner_unresolved_count,
                pending_support_confirmation_count=owner_pending_count,
                remediation_in_progress_count=owner_remediation_count,
                blocked_count=owner_blocked_count,
                superseded_count=owner_superseded_count,
                archives=archive_items,
                summary_message=_build_owner_summary_message(
                    owner_name=owner_name,
                    archive_count=len(archive_items),
                    closure_ready_count=owner_ready_count,
                    unresolved_count=owner_unresolved_count,
                    pending_support_confirmation_count=owner_pending_count,
                    remediation_in_progress_count=owner_remediation_count,
                    blocked_count=owner_blocked_count,
                ),
            )
        )

    unresolved_count = included_archive_count - closure_ready_count - superseded_count
    return UatReleaseArchiveSupportHandbackReport(
        report_version="phase7-release-archive-support-handback-v1",
        generated_at=datetime.now(UTC),
        generated_by=generated_by,
        as_of=ledger.as_of,
        review_window_days=review_window_days,
        stale_reply_after_hours=stale_reply_after_hours,
        total_archive_count=total_archive_count,
        included_archive_count=included_archive_count,
        owner_count=len(owners),
        closure_ready_count=closure_ready_count,
        unresolved_count=unresolved_count,
        pending_support_confirmation_count=pending_support_confirmation_count,
        remediation_in_progress_count=remediation_in_progress_count,
        blocked_count=blocked_count,
        superseded_count=superseded_count,
        owners=owners,
    )


def _derive_closure_status(
    *,
    archive: UatReleaseArchive,
    ledger_item: UatReleaseArchiveDeliveryLedgerItem,
) -> str:
    if archive.superseded_by_archive_id is not None:
        return "superseded"
    if any(
        reason in ledger_item.attention_reasons
        for reason in {"missing_export", "stale_notification_reply", "overdue_review"}
    ):
        return "blocked"
    if any(
        reason in ledger_item.attention_reasons
        for reason in {"re_export_due", "re_export_scheduled", "follow_up_required"}
    ):
        return "remediation_in_progress"
    if any(
        reason in ledger_item.attention_reasons
        for reason in {"notification_acknowledgement_pending", "acknowledgement_pending"}
    ):
        return "pending_support_confirmation"
    if ledger_item.latest_export_status in {"prepared", "delivered"}:
        return "pending_support_confirmation"
    return "ready_for_handback"


def _build_closure_blockers(attention_reasons: list[str]) -> list[str]:
    blockers: list[str] = []
    if "missing_export" in attention_reasons:
        blockers.append("Archive export has not been created yet.")
    if "stale_notification_reply" in attention_reasons:
        blockers.append("Notification reply window expired without acknowledgement.")
    if "re_export_due" in attention_reasons:
        blockers.append("Scheduled re-export is due.")
    if "re_export_scheduled" in attention_reasons:
        blockers.append("Scheduled re-export is still pending.")
    if "follow_up_required" in attention_reasons:
        blockers.append("Export follow-up is still required.")
    if "notification_acknowledgement_pending" in attention_reasons:
        blockers.append("Notification acknowledgement is still pending.")
    if "acknowledgement_pending" in attention_reasons:
        blockers.append("Export delivery acknowledgement is still pending.")
    if "overdue_review" in attention_reasons:
        blockers.append("Retention review is overdue.")
    return blockers


def _build_handback_summary(
    *,
    archive_name: str,
    closure_status: str,
    recommended_action: str,
    closure_blockers: list[str],
) -> str:
    if closure_blockers:
        blocker_summary = "; ".join(closure_blockers)
        return (
            f"{archive_name}: {closure_status.replace('_', ' ')}. "
            f"Recommended action: {recommended_action}. Blockers: {blocker_summary}"
        )
    return (
        f"{archive_name}: {closure_status.replace('_', ' ')}. "
        f"Recommended action: {recommended_action}."
    )


def _build_owner_summary_message(
    *,
    owner_name: str,
    archive_count: int,
    closure_ready_count: int,
    unresolved_count: int,
    pending_support_confirmation_count: int,
    remediation_in_progress_count: int,
    blocked_count: int,
) -> str:
    return (
        f"{owner_name}: {archive_count} archives in handback scope; "
        f"{closure_ready_count} ready for closure, {unresolved_count} unresolved, "
        f"{pending_support_confirmation_count} pending support confirmation, "
        f"{remediation_in_progress_count} in remediation, {blocked_count} blocked."
    )


def _closure_priority(closure_status: str) -> int:
    if closure_status == "blocked":
        return 0
    if closure_status == "remediation_in_progress":
        return 1
    if closure_status == "pending_support_confirmation":
        return 2
    if closure_status == "ready_for_handback":
        return 3
    return 4
