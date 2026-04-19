from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db.models.enums import UatAcceptanceDecision
from app.db.models.uat import UatAcceptanceArtifact, UatCycle, UatHandoffSnapshot
from app.services.uat import UatCycleNotFoundError
from app.services.uat_reporting import build_uat_signoff_report


class UatHandoffSnapshotNotFoundError(LookupError):
    """Raised when a UAT handoff snapshot cannot be found."""


class UatHandoffSnapshotConflictError(ValueError):
    """Raised when a UAT handoff snapshot action conflicts with existing state."""


@dataclass(slots=True)
class UatAcceptanceArtifactRecord:
    artifact_id: str
    snapshot_id: str
    decision: str
    stakeholder_name: str
    stakeholder_role: str | None
    stakeholder_organization: str | None
    decision_notes: str | None
    recorded_by: str
    created_at: datetime
    updated_at: datetime


@dataclass(slots=True)
class UatHandoffSnapshotSummary:
    snapshot_id: str
    cycle_id: str
    snapshot_name: str
    report_version: str
    export_scope: str
    cycle_status: str
    approval_ready: bool
    blocking_issue_count: int
    open_defect_count: int
    open_high_severity_defect_count: int
    distribution_summary: str
    created_by: str
    created_at: datetime
    updated_at: datetime
    acceptance_artifact_count: int


@dataclass(slots=True)
class UatHandoffSnapshotDetail(UatHandoffSnapshotSummary):
    report_payload: dict[str, Any]
    acceptance_artifacts: list[UatAcceptanceArtifactRecord] = field(default_factory=list)


def create_uat_handoff_snapshot(
    session: Session,
    *,
    cycle_id: str,
    snapshot_name: str,
    created_by: str,
    distribution_summary_override: str | None = None,
) -> UatHandoffSnapshotDetail:
    cycle = _get_cycle(session, cycle_id)
    report = build_uat_signoff_report(session, cycle_id, exported_by=created_by)
    distribution_summary = distribution_summary_override or _build_distribution_summary(report)
    report_payload = asdict(report)

    snapshot = UatHandoffSnapshot(
        cycle_id=cycle.cycle_id,
        snapshot_name=snapshot_name,
        report_version=report.report_version,
        export_scope=report.export_scope,
        cycle_status=report.cycle.status,
        approval_ready=report.approval_readiness.approval_ready,
        blocking_issue_count=report.approval_readiness.blocking_issue_count,
        open_defect_count=report.approval_readiness.open_defect_count,
        open_high_severity_defect_count=(
            report.approval_readiness.open_high_severity_defect_count
        ),
        distribution_summary=distribution_summary,
        report_payload=json.dumps(report_payload, default=_json_default, sort_keys=True),
        created_by=created_by,
    )
    session.add(snapshot)

    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        raise UatHandoffSnapshotConflictError(
            f"UAT handoff snapshot `{snapshot_name}` already exists for this cycle."
        ) from exc

    return get_uat_handoff_snapshot(session, str(snapshot.snapshot_id))


def list_uat_handoff_snapshots(
    session: Session,
    *,
    cycle_id: str,
) -> list[UatHandoffSnapshotSummary]:
    cycle = _get_cycle(session, cycle_id)
    snapshot_models = session.scalars(
        select(UatHandoffSnapshot)
        .where(UatHandoffSnapshot.cycle_id == cycle.cycle_id)
        .order_by(UatHandoffSnapshot.created_at.desc(), UatHandoffSnapshot.snapshot_id.desc())
    ).all()
    return [_build_snapshot_summary(session, snapshot) for snapshot in snapshot_models]


def get_uat_handoff_snapshot(session: Session, snapshot_id: str) -> UatHandoffSnapshotDetail:
    snapshot = _get_snapshot(session, snapshot_id)
    return _build_snapshot_detail(session, snapshot)


def create_uat_acceptance_artifact(
    session: Session,
    *,
    snapshot_id: str,
    decision: UatAcceptanceDecision,
    stakeholder_name: str,
    recorded_by: str,
    stakeholder_role: str | None = None,
    stakeholder_organization: str | None = None,
    decision_notes: str | None = None,
) -> UatHandoffSnapshotDetail:
    snapshot = _get_snapshot(session, snapshot_id)
    artifact = UatAcceptanceArtifact(
        snapshot_id=snapshot.snapshot_id,
        decision=decision.value,
        stakeholder_name=stakeholder_name,
        stakeholder_role=stakeholder_role,
        stakeholder_organization=stakeholder_organization,
        decision_notes=decision_notes,
        recorded_by=recorded_by,
    )
    session.add(artifact)
    session.commit()
    return get_uat_handoff_snapshot(session, str(snapshot.snapshot_id))


def _build_snapshot_summary(
    session: Session,
    snapshot: UatHandoffSnapshot,
) -> UatHandoffSnapshotSummary:
    acceptance_artifact_count = session.scalar(
        select(func.count())
        .select_from(UatAcceptanceArtifact)
        .where(UatAcceptanceArtifact.snapshot_id == snapshot.snapshot_id)
    )
    return UatHandoffSnapshotSummary(
        snapshot_id=str(snapshot.snapshot_id),
        cycle_id=str(snapshot.cycle_id),
        snapshot_name=snapshot.snapshot_name,
        report_version=snapshot.report_version,
        export_scope=snapshot.export_scope,
        cycle_status=snapshot.cycle_status,
        approval_ready=snapshot.approval_ready,
        blocking_issue_count=snapshot.blocking_issue_count,
        open_defect_count=snapshot.open_defect_count,
        open_high_severity_defect_count=snapshot.open_high_severity_defect_count,
        distribution_summary=snapshot.distribution_summary,
        created_by=snapshot.created_by,
        created_at=snapshot.created_at,
        updated_at=snapshot.updated_at,
        acceptance_artifact_count=acceptance_artifact_count or 0,
    )


def _build_snapshot_detail(
    session: Session,
    snapshot: UatHandoffSnapshot,
) -> UatHandoffSnapshotDetail:
    summary = _build_snapshot_summary(session, snapshot)
    artifact_models = session.scalars(
        select(UatAcceptanceArtifact)
        .where(UatAcceptanceArtifact.snapshot_id == snapshot.snapshot_id)
        .order_by(UatAcceptanceArtifact.created_at.asc(), UatAcceptanceArtifact.artifact_id.asc())
    ).all()
    return UatHandoffSnapshotDetail(
        snapshot_id=summary.snapshot_id,
        cycle_id=summary.cycle_id,
        snapshot_name=summary.snapshot_name,
        report_version=summary.report_version,
        export_scope=summary.export_scope,
        cycle_status=summary.cycle_status,
        approval_ready=summary.approval_ready,
        blocking_issue_count=summary.blocking_issue_count,
        open_defect_count=summary.open_defect_count,
        open_high_severity_defect_count=summary.open_high_severity_defect_count,
        distribution_summary=summary.distribution_summary,
        created_by=summary.created_by,
        created_at=summary.created_at,
        updated_at=summary.updated_at,
        acceptance_artifact_count=summary.acceptance_artifact_count,
        report_payload=json.loads(snapshot.report_payload),
        acceptance_artifacts=[
            UatAcceptanceArtifactRecord(
                artifact_id=str(artifact.artifact_id),
                snapshot_id=str(artifact.snapshot_id),
                decision=artifact.decision,
                stakeholder_name=artifact.stakeholder_name,
                stakeholder_role=artifact.stakeholder_role,
                stakeholder_organization=artifact.stakeholder_organization,
                decision_notes=artifact.decision_notes,
                recorded_by=artifact.recorded_by,
                created_at=artifact.created_at,
                updated_at=artifact.updated_at,
            )
            for artifact in artifact_models
        ],
    )


def _build_distribution_summary(report: Any) -> str:
    readiness = report.approval_readiness
    summary_parts = [
        f"Cycle {report.cycle.cycle_name} is {report.cycle.status}.",
        f"Approval ready: {'yes' if readiness.approval_ready else 'no'}.",
        (
            f"Terminal scenarios: {readiness.terminal_scenario_count}/"
            f"{report.cycle.scenario_count}."
        ),
        (
            f"Open defects: {readiness.open_defect_count} "
            f"({readiness.open_high_severity_defect_count} critical/high)."
        ),
    ]
    if readiness.blocking_issues:
        summary_parts.append("Blockers: " + " ".join(readiness.blocking_issues))
    elif readiness.attention_items:
        summary_parts.append("Attention: " + " ".join(readiness.attention_items))

    return " ".join(summary_parts)


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable.")


def _get_cycle(session: Session, cycle_id: str) -> UatCycle:
    cycle = session.get(UatCycle, UUID(cycle_id))
    if cycle is None:
        raise UatCycleNotFoundError(f"UAT cycle `{cycle_id}` was not found.")
    return cycle


def _get_snapshot(session: Session, snapshot_id: str) -> UatHandoffSnapshot:
    snapshot = session.get(UatHandoffSnapshot, UUID(snapshot_id))
    if snapshot is None:
        raise UatHandoffSnapshotNotFoundError(
            f"UAT handoff snapshot `{snapshot_id}` was not found."
        )
    return snapshot
