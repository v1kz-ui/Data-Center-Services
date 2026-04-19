from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from app.db.models.uat import (
    UatAcceptanceArtifact,
    UatDistributionPacket,
    UatDistributionRecipient,
    UatHandoffSnapshot,
    UatLaunchDecisionRecord,
    UatReleaseArchive,
    UatReleaseArchiveEvidenceItem,
)
from app.services.uat_handoff import UatHandoffSnapshotNotFoundError
from app.services.uat_launch import build_uat_launch_closeout_report

RETENTION_STATUSES = {"active", "review_due", "superseded"}


class UatReleaseArchiveNotFoundError(LookupError):
    """Raised when a UAT release archive cannot be found."""


class UatReleaseArchiveConflictError(ValueError):
    """Raised when a UAT release archive conflicts with existing state."""


class UatReleaseArchiveValidationError(ValueError):
    """Raised when a UAT release archive request is invalid."""


@dataclass(slots=True)
class UatReleaseArchiveEvidenceItemRecord:
    evidence_item_id: str
    archive_id: str
    evidence_type: str
    evidence_status: str
    reference_id: str
    reference_name: str
    retention_label: str
    evidence_summary: str
    source_recorded_at: datetime | None
    source_location: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(slots=True)
class UatReleaseArchiveSummary:
    archive_id: str
    snapshot_id: str
    cycle_id: str
    snapshot_name: str
    archive_name: str
    recommended_outcome: str
    blocking_exception_count: int
    attention_exception_count: int
    support_handoff_owner: str | None
    support_handoff_summary: str
    operations_runbook_reference: str | None
    archive_summary: str
    archive_checksum: str
    retention_review_at: datetime
    retention_status: str
    superseded_by_archive_id: str | None
    supersession_reason: str | None
    superseded_at: datetime | None
    created_by: str
    created_at: datetime
    updated_at: datetime
    sealed_at: datetime
    evidence_item_count: int


@dataclass(slots=True)
class UatReleaseArchiveDetail(UatReleaseArchiveSummary):
    manifest_payload: dict[str, Any]
    evidence_items: list[UatReleaseArchiveEvidenceItemRecord]


def create_uat_release_archive(
    session: Session,
    *,
    snapshot_id: str,
    archive_name: str,
    created_by: str,
    support_handoff_owner: str | None = None,
    operations_runbook_reference: str | None = None,
    support_handoff_summary_override: str | None = None,
    release_manifest_notes: str | None = None,
    retention_review_at: datetime | None = None,
) -> UatReleaseArchiveDetail:
    snapshot = _get_snapshot(session, snapshot_id)
    closeout_report = build_uat_launch_closeout_report(
        session,
        snapshot_id,
        exported_by=created_by,
    )

    support_handoff_summary = support_handoff_summary_override or _build_support_handoff_summary(
        closeout_report,
        support_handoff_owner=support_handoff_owner,
        operations_runbook_reference=operations_runbook_reference,
    )
    sealed_at = datetime.now(UTC)
    retention_review_timestamp = retention_review_at or sealed_at + timedelta(days=365)
    manifest_payload = _build_manifest_payload(
        snapshot,
        closeout_report,
        archive_name=archive_name,
        created_by=created_by,
        support_handoff_owner=support_handoff_owner,
        operations_runbook_reference=operations_runbook_reference,
        support_handoff_summary=support_handoff_summary,
        release_manifest_notes=release_manifest_notes,
        retention_review_at=retention_review_timestamp,
    )
    manifest_json = json.dumps(manifest_payload, default=_json_default, sort_keys=True)

    archive = UatReleaseArchive(
        snapshot_id=snapshot.snapshot_id,
        archive_name=archive_name,
        recommended_outcome=closeout_report.readiness.recommended_outcome,
        blocking_exception_count=closeout_report.readiness.blocking_exception_count,
        attention_exception_count=closeout_report.readiness.attention_exception_count,
        support_handoff_owner=support_handoff_owner,
        support_handoff_summary=support_handoff_summary,
        operations_runbook_reference=operations_runbook_reference,
        archive_summary=_build_archive_summary(
            snapshot,
            closeout_report,
            support_handoff_owner=support_handoff_owner,
        ),
        manifest_payload=manifest_json,
        archive_checksum=hashlib.sha256(manifest_json.encode("utf-8")).hexdigest(),
        retention_review_at=retention_review_timestamp,
        created_by=created_by,
        sealed_at=sealed_at,
    )
    session.add(archive)

    try:
        session.flush()
        session.add_all(
            _build_evidence_items(
                session,
                snapshot=snapshot,
                archive=archive,
                closeout_report=closeout_report,
                support_handoff_owner=support_handoff_owner,
                operations_runbook_reference=operations_runbook_reference,
            )
        )
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        raise UatReleaseArchiveConflictError(
            f"UAT release archive `{archive_name}` already exists for this snapshot."
        ) from exc

    return get_uat_release_archive(session, str(archive.archive_id))


def list_uat_release_archives(
    session: Session,
    *,
    snapshot_id: str | None = None,
    cycle_id: str | None = None,
    recommended_outcome: str | None = None,
    retention_status: str | None = None,
    include_superseded: bool = True,
    search: str | None = None,
) -> list[UatReleaseArchiveSummary]:
    if retention_status is not None and retention_status not in RETENTION_STATUSES:
        raise UatReleaseArchiveValidationError(
            "Retention status must be one of: active, review_due, superseded."
        )

    statement = (
        select(UatReleaseArchive)
        .join(UatHandoffSnapshot, UatHandoffSnapshot.snapshot_id == UatReleaseArchive.snapshot_id)
        .options(
            selectinload(UatReleaseArchive.snapshot),
            selectinload(UatReleaseArchive.evidence_items),
        )
        .order_by(UatReleaseArchive.created_at.desc(), UatReleaseArchive.archive_id.desc())
    )

    if snapshot_id is not None:
        snapshot = _get_snapshot(session, snapshot_id)
        statement = statement.where(UatReleaseArchive.snapshot_id == snapshot.snapshot_id)
    if cycle_id is not None:
        statement = statement.where(UatHandoffSnapshot.cycle_id == UUID(cycle_id))
    if recommended_outcome is not None:
        statement = statement.where(UatReleaseArchive.recommended_outcome == recommended_outcome)
    if search:
        search_text = f"%{search.strip()}%"
        statement = statement.where(
            or_(
                UatReleaseArchive.archive_name.ilike(search_text),
                UatHandoffSnapshot.snapshot_name.ilike(search_text),
            )
        )

    archive_models = session.scalars(statement).all()
    summaries = [
        _build_archive_summary_record(archive)
        for archive in archive_models
        if include_superseded or archive.superseded_by_archive_id is None
    ]
    if retention_status is not None:
        summaries = [
            summary
            for summary in summaries
            if summary.retention_status == retention_status
        ]
    return summaries


def get_uat_release_archive(
    session: Session,
    archive_id: str,
) -> UatReleaseArchiveDetail:
    archive = _get_archive(session, archive_id)
    return _build_archive_detail(archive)


def list_uat_release_archive_evidence_items(
    session: Session,
    *,
    archive_id: str,
) -> list[UatReleaseArchiveEvidenceItemRecord]:
    archive = _get_archive(session, archive_id)
    evidence_items = session.scalars(
        select(UatReleaseArchiveEvidenceItem)
        .where(UatReleaseArchiveEvidenceItem.archive_id == archive.archive_id)
        .order_by(
            UatReleaseArchiveEvidenceItem.evidence_type.asc(),
            UatReleaseArchiveEvidenceItem.source_recorded_at.asc(),
            UatReleaseArchiveEvidenceItem.evidence_item_id.asc(),
        )
    ).all()
    return [_build_evidence_item_record(item) for item in evidence_items]


def supersede_uat_release_archive(
    session: Session,
    *,
    archive_id: str,
    superseded_by_archive_id: str,
    supersession_reason: str,
) -> UatReleaseArchiveDetail:
    archive = _get_archive(session, archive_id)
    successor = _get_archive(session, superseded_by_archive_id)

    if archive.archive_id == successor.archive_id:
        raise UatReleaseArchiveValidationError("A release archive cannot supersede itself.")
    if archive.snapshot.cycle_id != successor.snapshot.cycle_id:
        raise UatReleaseArchiveConflictError(
            "Release archives can only be superseded by another archive from the same UAT cycle."
        )
    if archive.superseded_by_archive_id is not None:
        raise UatReleaseArchiveConflictError(
            f"UAT release archive `{archive.archive_name}` has already been superseded."
        )

    archive.superseded_by_archive_id = successor.archive_id
    archive.supersession_reason = supersession_reason
    archive.superseded_at = datetime.now(UTC)
    session.commit()
    return get_uat_release_archive(session, archive_id)


def _build_archive_summary_record(archive: UatReleaseArchive) -> UatReleaseArchiveSummary:
    return UatReleaseArchiveSummary(
        archive_id=str(archive.archive_id),
        snapshot_id=str(archive.snapshot_id),
        cycle_id=str(archive.snapshot.cycle_id),
        snapshot_name=archive.snapshot.snapshot_name,
        archive_name=archive.archive_name,
        recommended_outcome=archive.recommended_outcome,
        blocking_exception_count=archive.blocking_exception_count,
        attention_exception_count=archive.attention_exception_count,
        support_handoff_owner=archive.support_handoff_owner,
        support_handoff_summary=archive.support_handoff_summary,
        operations_runbook_reference=archive.operations_runbook_reference,
        archive_summary=archive.archive_summary,
        archive_checksum=archive.archive_checksum,
        retention_review_at=archive.retention_review_at,
        retention_status=_derive_retention_status(archive),
        superseded_by_archive_id=(
            str(archive.superseded_by_archive_id)
            if archive.superseded_by_archive_id is not None
            else None
        ),
        supersession_reason=archive.supersession_reason,
        superseded_at=archive.superseded_at,
        created_by=archive.created_by,
        created_at=archive.created_at,
        updated_at=archive.updated_at,
        sealed_at=archive.sealed_at,
        evidence_item_count=len(archive.evidence_items),
    )


def _build_archive_detail(archive: UatReleaseArchive) -> UatReleaseArchiveDetail:
    summary = _build_archive_summary_record(archive)
    ordered_items = sorted(
        archive.evidence_items,
        key=lambda item: (
            item.evidence_type,
            item.source_recorded_at or datetime.min.replace(tzinfo=UTC),
            str(item.evidence_item_id),
        ),
    )
    return UatReleaseArchiveDetail(
        archive_id=summary.archive_id,
        snapshot_id=summary.snapshot_id,
        cycle_id=summary.cycle_id,
        snapshot_name=summary.snapshot_name,
        archive_name=summary.archive_name,
        recommended_outcome=summary.recommended_outcome,
        blocking_exception_count=summary.blocking_exception_count,
        attention_exception_count=summary.attention_exception_count,
        support_handoff_owner=summary.support_handoff_owner,
        support_handoff_summary=summary.support_handoff_summary,
        operations_runbook_reference=summary.operations_runbook_reference,
        archive_summary=summary.archive_summary,
        archive_checksum=summary.archive_checksum,
        retention_review_at=summary.retention_review_at,
        retention_status=summary.retention_status,
        superseded_by_archive_id=summary.superseded_by_archive_id,
        supersession_reason=summary.supersession_reason,
        superseded_at=summary.superseded_at,
        created_by=summary.created_by,
        created_at=summary.created_at,
        updated_at=summary.updated_at,
        sealed_at=summary.sealed_at,
        evidence_item_count=summary.evidence_item_count,
        manifest_payload=json.loads(archive.manifest_payload),
        evidence_items=[_build_evidence_item_record(item) for item in ordered_items],
    )


def _build_evidence_item_record(
    evidence_item: UatReleaseArchiveEvidenceItem,
) -> UatReleaseArchiveEvidenceItemRecord:
    return UatReleaseArchiveEvidenceItemRecord(
        evidence_item_id=str(evidence_item.evidence_item_id),
        archive_id=str(evidence_item.archive_id),
        evidence_type=evidence_item.evidence_type,
        evidence_status=evidence_item.evidence_status,
        reference_id=evidence_item.reference_id,
        reference_name=evidence_item.reference_name,
        retention_label=evidence_item.retention_label,
        evidence_summary=evidence_item.evidence_summary,
        source_recorded_at=evidence_item.source_recorded_at,
        source_location=evidence_item.source_location,
        created_at=evidence_item.created_at,
        updated_at=evidence_item.updated_at,
    )


def _build_manifest_payload(
    snapshot: UatHandoffSnapshot,
    closeout_report: Any,
    *,
    archive_name: str,
    created_by: str,
    support_handoff_owner: str | None,
    operations_runbook_reference: str | None,
    support_handoff_summary: str,
    release_manifest_notes: str | None,
    retention_review_at: datetime,
) -> dict[str, Any]:
    return {
        "report_version": "phase7-release-archive-v2",
        "export_scope": "release_archive",
        "archive_name": archive_name,
        "created_by": created_by,
        "snapshot_summary": {
            "snapshot_id": str(snapshot.snapshot_id),
            "cycle_id": str(snapshot.cycle_id),
            "snapshot_name": snapshot.snapshot_name,
            "cycle_status": snapshot.cycle_status,
            "approval_ready": snapshot.approval_ready,
            "distribution_summary": snapshot.distribution_summary,
        },
        "closeout_report": asdict(closeout_report),
        "retention": {
            "retention_review_at": retention_review_at,
            "retention_status": "active",
        },
        "support_handoff": {
            "owner_name": support_handoff_owner,
            "runbook_reference": operations_runbook_reference,
            "summary": support_handoff_summary,
            "release_manifest_notes": release_manifest_notes,
        },
    }


def _build_evidence_items(
    session: Session,
    *,
    snapshot: UatHandoffSnapshot,
    archive: UatReleaseArchive,
    closeout_report: Any,
    support_handoff_owner: str | None,
    operations_runbook_reference: str | None,
) -> list[UatReleaseArchiveEvidenceItem]:
    evidence_items = [
        UatReleaseArchiveEvidenceItem(
            archive_id=archive.archive_id,
            evidence_type="snapshot_report",
            evidence_status="sealed" if snapshot.approval_ready else "captured",
            reference_id=str(snapshot.snapshot_id),
            reference_name=snapshot.snapshot_name,
            retention_label="signoff_payload",
            evidence_summary=(
                f"Snapshot `{snapshot.snapshot_name}` captured cycle status "
                f"`{snapshot.cycle_status}` with recommended outcome "
                f"`{closeout_report.readiness.recommended_outcome}`."
            ),
            source_recorded_at=snapshot.created_at,
            source_location=snapshot.report_version,
        ),
        UatReleaseArchiveEvidenceItem(
            archive_id=archive.archive_id,
            evidence_type="support_handoff",
            evidence_status="sealed",
            reference_id="support_handoff",
            reference_name="Support Handoff",
            retention_label="operations_handoff",
            evidence_summary=archive.support_handoff_summary,
            source_recorded_at=archive.sealed_at,
            source_location=operations_runbook_reference,
        ),
    ]

    for artifact in _list_acceptance_artifacts(session, snapshot.snapshot_id):
        evidence_items.append(
            UatReleaseArchiveEvidenceItem(
                archive_id=archive.archive_id,
                evidence_type="acceptance_artifact",
                evidence_status=artifact.decision,
                reference_id=str(artifact.artifact_id),
                reference_name=artifact.stakeholder_name,
                retention_label="stakeholder_approval",
                evidence_summary=(
                    f"{artifact.stakeholder_name} recorded `{artifact.decision}` for the "
                    "handoff package."
                ),
                source_recorded_at=artifact.created_at,
                source_location=artifact.stakeholder_organization,
            )
        )

    for packet in _list_distribution_packets(session, snapshot.snapshot_id):
        recipient_models = _list_distribution_recipients(session, packet.packet_id)
        acknowledged_recipient_count = sum(
            1 for recipient in recipient_models if recipient.delivery_status == "acknowledged"
        )
        evidence_items.append(
            UatReleaseArchiveEvidenceItem(
                archive_id=archive.archive_id,
                evidence_type="distribution_packet",
                evidence_status=packet.distribution_status,
                reference_id=str(packet.packet_id),
                reference_name=packet.packet_name,
                retention_label="distribution_packet",
                evidence_summary=(
                    f"Packet `{packet.packet_name}` is `{packet.distribution_status}` with "
                    f"{acknowledged_recipient_count}/{len(recipient_models)} recipients "
                    "acknowledged."
                ),
                source_recorded_at=packet.created_at,
                source_location=packet.channel,
            )
        )
        for recipient in recipient_models:
            evidence_items.append(
                UatReleaseArchiveEvidenceItem(
                    archive_id=archive.archive_id,
                    evidence_type="distribution_recipient",
                    evidence_status=recipient.delivery_status,
                    reference_id=str(recipient.recipient_id),
                    reference_name=recipient.recipient_name,
                    retention_label=(
                        "required_acknowledgement"
                        if recipient.required_for_ack
                        else "distribution_notification"
                    ),
                    evidence_summary=(
                        f"{recipient.recipient_name} is `{recipient.delivery_status}` for "
                        f"packet `{packet.packet_name}`."
                    ),
                    source_recorded_at=recipient.updated_at,
                    source_location=recipient.recipient_contact,
                )
            )

    for decision in _list_launch_decisions(session, snapshot.snapshot_id):
        evidence_items.append(
            UatReleaseArchiveEvidenceItem(
                archive_id=archive.archive_id,
                evidence_type="launch_decision",
                evidence_status=decision.decision,
                reference_id=str(decision.decision_id),
                reference_name=decision.reviewer_name,
                retention_label="launch_governance",
                evidence_summary=(
                    f"{decision.reviewer_name} recorded `{decision.decision}` for launch "
                    "governance."
                ),
                source_recorded_at=decision.created_at,
                source_location=decision.reviewer_organization,
            )
        )

    if support_handoff_owner:
        evidence_items.append(
            UatReleaseArchiveEvidenceItem(
                archive_id=archive.archive_id,
                evidence_type="support_owner",
                evidence_status="assigned",
                reference_id="support_owner",
                reference_name=support_handoff_owner,
                retention_label="operations_handoff",
                evidence_summary=f"Support handoff owner assigned to {support_handoff_owner}.",
                source_recorded_at=archive.sealed_at,
                source_location=operations_runbook_reference,
            )
        )

    return evidence_items


def _build_support_handoff_summary(
    closeout_report: Any,
    *,
    support_handoff_owner: str | None,
    operations_runbook_reference: str | None,
) -> str:
    readiness = closeout_report.readiness
    summary_parts = [
        f"Recommended outcome: {readiness.recommended_outcome}.",
        (
            f"Exceptions: {readiness.blocking_exception_count} blocking / "
            f"{readiness.attention_exception_count} attention."
        ),
        (
            f"Required acknowledgements: {readiness.acknowledged_required_recipient_count}/"
            f"{readiness.required_recipient_count}."
        ),
    ]
    if support_handoff_owner:
        summary_parts.append(f"Support owner: {support_handoff_owner}.")
    if operations_runbook_reference:
        summary_parts.append(f"Runbook: {operations_runbook_reference}.")
    return " ".join(summary_parts)


def _build_archive_summary(
    snapshot: UatHandoffSnapshot,
    closeout_report: Any,
    *,
    support_handoff_owner: str | None,
) -> str:
    readiness = closeout_report.readiness
    owner_summary = f" Support owner: {support_handoff_owner}." if support_handoff_owner else ""
    return (
        f"Archive {snapshot.snapshot_name} sealed with outcome "
        f"{readiness.recommended_outcome}, {readiness.blocking_exception_count} blocking "
        f"exceptions, and {len(closeout_report.packet_summaries)} packet summaries."
        f"{owner_summary}"
    ).strip()


def _derive_retention_status(archive: UatReleaseArchive) -> str:
    if archive.superseded_by_archive_id is not None:
        return "superseded"
    if _normalize_datetime(archive.retention_review_at) <= datetime.now(UTC):
        return "review_due"
    return "active"


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable.")


def _get_snapshot(session: Session, snapshot_id: str) -> UatHandoffSnapshot:
    snapshot = session.get(UatHandoffSnapshot, UUID(snapshot_id))
    if snapshot is None:
        raise UatHandoffSnapshotNotFoundError(
            f"UAT handoff snapshot `{snapshot_id}` was not found."
        )
    return snapshot


def _get_archive(session: Session, archive_id: str) -> UatReleaseArchive:
    archive = session.scalar(
        select(UatReleaseArchive)
        .options(
            selectinload(UatReleaseArchive.snapshot),
            selectinload(UatReleaseArchive.evidence_items),
        )
        .where(UatReleaseArchive.archive_id == UUID(archive_id))
    )
    if archive is None:
        raise UatReleaseArchiveNotFoundError(
            f"UAT release archive `{archive_id}` was not found."
        )
    return archive


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
