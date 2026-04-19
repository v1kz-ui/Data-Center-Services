from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db.models.enums import (
    UatCycleStatus,
    UatDefectSeverity,
    UatDefectStatus,
    UatExecutionStatus,
)
from app.db.models.uat import UatCycle, UatCycleEvent, UatDefect, UatScenarioExecution
from app.services.uat_readiness import load_uat_scenarios

REPO_ROOT = Path(__file__).resolve().parents[5]
TERMINAL_EXECUTION_STATUSES = {
    UatExecutionStatus.PASSED.value,
    UatExecutionStatus.FAILED.value,
    UatExecutionStatus.BLOCKED.value,
}
EVENT_TYPE_CYCLE_CREATED = "cycle_created"
EVENT_TYPE_SCENARIO_RESULT_RECORDED = "scenario_result_recorded"
EVENT_TYPE_DEFECT_LOGGED = "defect_logged"
EVENT_TYPE_DEFECT_UPDATED = "defect_updated"
EVENT_TYPE_CYCLE_FINALIZED = "cycle_finalized"


class UatCycleNotFoundError(LookupError):
    """Raised when a UAT cycle cannot be found."""


class UatScenarioExecutionNotFoundError(LookupError):
    """Raised when a cycle scenario execution cannot be found."""


class UatDefectNotFoundError(LookupError):
    """Raised when a cycle defect cannot be found."""


class UatCycleConflictError(ValueError):
    """Raised when a UAT cycle action conflicts with current state."""


class UatCycleFinalizeError(ValueError):
    """Raised when a UAT cycle cannot be finalized as requested."""


@dataclass(slots=True)
class UatCount:
    category: str
    count: int


@dataclass(slots=True)
class UatScenarioExecutionRecord:
    execution_id: str
    scenario_id: str
    title: str
    actor_role: str
    workflow: str
    entrypoint: str
    status: str
    execution_notes: str | None
    evidence_reference: str | None
    executed_by: str | None
    executed_at: datetime | None


@dataclass(slots=True)
class UatDefectRecord:
    defect_id: str
    scenario_id: str | None
    severity: str
    status: str
    title: str
    description: str
    reported_by: str
    owner_name: str | None
    external_reference: str | None
    resolution_notes: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(slots=True)
class UatCycleSummary:
    cycle_id: str
    cycle_name: str
    environment_name: str
    scenario_pack_path: str
    status: str
    created_by: str
    summary_notes: str | None
    started_at: datetime | None
    completed_at: datetime | None
    created_at: datetime
    updated_at: datetime
    scenario_count: int
    defect_count: int
    scenario_status_counts: list[UatCount] = field(default_factory=list)
    defect_severity_counts: list[UatCount] = field(default_factory=list)
    defect_status_counts: list[UatCount] = field(default_factory=list)


@dataclass(slots=True)
class UatCycleDetail(UatCycleSummary):
    scenario_executions: list[UatScenarioExecutionRecord] = field(default_factory=list)
    defects: list[UatDefectRecord] = field(default_factory=list)


def create_uat_cycle(
    session: Session,
    *,
    cycle_name: str,
    created_by: str,
    environment_name: str,
    scenario_pack_path: str,
    summary_notes: str | None = None,
) -> UatCycleDetail:
    resolved_pack_path = _resolve_pack_path(scenario_pack_path)
    scenario_definitions = load_uat_scenarios(resolved_pack_path)
    cycle = UatCycle(
        cycle_name=cycle_name,
        environment_name=environment_name,
        scenario_pack_path=str(resolved_pack_path),
        status=UatCycleStatus.PLANNED.value,
        created_by=created_by,
        summary_notes=summary_notes,
    )
    session.add(cycle)
    try:
        session.flush()

        for scenario in scenario_definitions:
            session.add(
                UatScenarioExecution(
                    cycle_id=cycle.cycle_id,
                    scenario_id=scenario.scenario_id,
                    title=scenario.title,
                    actor_role=scenario.actor_role,
                    workflow=scenario.workflow,
                    entrypoint=scenario.entrypoint,
                    status=UatExecutionStatus.PLANNED.value,
                )
            )
        _record_cycle_event(
            session,
            cycle_id=cycle.cycle_id,
            event_type=EVENT_TYPE_CYCLE_CREATED,
            actor_name=created_by,
            event_notes=summary_notes,
            event_payload={
                "environment_name": environment_name,
                "scenario_count": len(scenario_definitions),
                "scenario_pack_path": str(resolved_pack_path),
            },
        )
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        raise UatCycleConflictError(f"UAT cycle `{cycle_name}` already exists.") from exc

    return get_uat_cycle(session, str(cycle.cycle_id))


def list_uat_cycles(session: Session) -> list[UatCycleSummary]:
    cycle_models = session.scalars(select(UatCycle).order_by(UatCycle.created_at.desc())).all()
    return [_build_cycle_summary(session, cycle) for cycle in cycle_models]


def get_uat_cycle(session: Session, cycle_id: str) -> UatCycleDetail:
    cycle = _get_cycle(session, cycle_id)
    return _build_cycle_detail(session, cycle)


def record_uat_execution_result(
    session: Session,
    *,
    cycle_id: str,
    scenario_id: str,
    status: UatExecutionStatus,
    executed_by: str,
    execution_notes: str | None = None,
    evidence_reference: str | None = None,
) -> UatCycleDetail:
    cycle = _get_cycle(session, cycle_id)
    _assert_cycle_mutable(cycle)
    execution = _get_execution(session, cycle.cycle_id, scenario_id)

    execution.status = status.value
    execution.execution_notes = execution_notes
    execution.evidence_reference = evidence_reference
    execution.executed_by = executed_by
    if status is UatExecutionStatus.PLANNED:
        execution.executed_at = None
    else:
        execution.executed_at = datetime.now(UTC)
        cycle.started_at = cycle.started_at or execution.executed_at

    _refresh_cycle_status(session, cycle)
    _record_cycle_event(
        session,
        cycle_id=cycle.cycle_id,
        event_type=EVENT_TYPE_SCENARIO_RESULT_RECORDED,
        actor_name=executed_by,
        scenario_id=scenario_id,
        event_notes=execution_notes,
        event_payload={
            "cycle_status": cycle.status,
            "evidence_reference": evidence_reference,
            "status": status.value,
        },
    )
    session.commit()
    return get_uat_cycle(session, str(cycle.cycle_id))


def create_uat_defect(
    session: Session,
    *,
    cycle_id: str,
    reported_by: str,
    severity: UatDefectSeverity,
    title: str,
    description: str,
    scenario_id: str | None = None,
    owner_name: str | None = None,
    external_reference: str | None = None,
) -> UatCycleDetail:
    cycle = _get_cycle(session, cycle_id)
    _assert_cycle_mutable(cycle)
    if scenario_id is not None:
        _get_execution(session, cycle.cycle_id, scenario_id)

    defect = UatDefect(
        cycle_id=cycle.cycle_id,
        scenario_id=scenario_id,
        severity=severity.value,
        status=UatDefectStatus.OPEN.value,
        title=title,
        description=description,
        reported_by=reported_by,
        owner_name=owner_name,
        external_reference=external_reference,
    )
    session.add(defect)
    session.flush()
    _record_cycle_event(
        session,
        cycle_id=cycle.cycle_id,
        event_type=EVENT_TYPE_DEFECT_LOGGED,
        actor_name=reported_by,
        scenario_id=scenario_id,
        defect_id=defect.defect_id,
        event_notes=title,
        event_payload={
            "external_reference": external_reference,
            "owner_name": owner_name,
            "severity": severity.value,
            "status": UatDefectStatus.OPEN.value,
        },
    )
    session.commit()
    return get_uat_cycle(session, str(cycle.cycle_id))


def update_uat_defect(
    session: Session,
    *,
    cycle_id: str,
    defect_id: str,
    updated_by: str,
    status: UatDefectStatus | None = None,
    owner_name: str | None = None,
    external_reference: str | None = None,
    resolution_notes: str | None = None,
) -> UatCycleDetail:
    cycle = _get_cycle(session, cycle_id)
    _assert_cycle_mutable(cycle)
    defect = _get_defect(session, cycle.cycle_id, defect_id)

    if status is not None:
        defect.status = status.value
    if owner_name is not None:
        defect.owner_name = owner_name
    if external_reference is not None:
        defect.external_reference = external_reference
    if resolution_notes is not None:
        defect.resolution_notes = resolution_notes

    _record_cycle_event(
        session,
        cycle_id=cycle.cycle_id,
        event_type=EVENT_TYPE_DEFECT_UPDATED,
        actor_name=updated_by,
        scenario_id=defect.scenario_id,
        defect_id=defect.defect_id,
        event_notes=resolution_notes,
        event_payload={
            "external_reference": defect.external_reference,
            "owner_name": defect.owner_name,
            "status": defect.status,
        },
    )
    session.commit()
    return get_uat_cycle(session, str(cycle.cycle_id))


def finalize_uat_cycle(
    session: Session,
    *,
    cycle_id: str,
    finalized_by: str,
    status: UatCycleStatus,
    summary_notes: str | None = None,
) -> UatCycleDetail:
    if status not in {UatCycleStatus.APPROVED, UatCycleStatus.REWORK_REQUIRED}:
        raise UatCycleFinalizeError(
            "UAT cycles can only be finalized as approved or rework_required."
        )

    cycle = _get_cycle(session, cycle_id)
    if cycle.status == UatCycleStatus.APPROVED.value:
        raise UatCycleConflictError(f"UAT cycle `{cycle.cycle_name}` is already approved.")

    execution_models = session.scalars(
        select(UatScenarioExecution).where(UatScenarioExecution.cycle_id == cycle.cycle_id)
    ).all()
    if any(execution.status not in TERMINAL_EXECUTION_STATUSES for execution in execution_models):
        raise UatCycleFinalizeError(
            "All UAT scenarios must be terminal before the cycle can be finalized."
        )

    defect_models = session.scalars(
        select(UatDefect).where(UatDefect.cycle_id == cycle.cycle_id)
    ).all()
    has_open_high_severity_defect = any(
        defect.status == UatDefectStatus.OPEN.value
        and defect.severity in {UatDefectSeverity.CRITICAL.value, UatDefectSeverity.HIGH.value}
        for defect in defect_models
    )
    if status is UatCycleStatus.APPROVED and has_open_high_severity_defect:
        raise UatCycleFinalizeError(
            "UAT cycle approval is blocked while critical or high severity defects remain open."
        )

    cycle.status = status.value
    cycle.summary_notes = summary_notes
    cycle.completed_at = cycle.completed_at or datetime.now(UTC)
    _record_cycle_event(
        session,
        cycle_id=cycle.cycle_id,
        event_type=EVENT_TYPE_CYCLE_FINALIZED,
        actor_name=finalized_by,
        event_notes=summary_notes,
        event_payload={"status": status.value},
    )
    session.commit()
    return get_uat_cycle(session, str(cycle.cycle_id))


def _build_cycle_summary(session: Session, cycle: UatCycle) -> UatCycleSummary:
    execution_models = session.scalars(
        select(UatScenarioExecution)
        .where(UatScenarioExecution.cycle_id == cycle.cycle_id)
        .order_by(UatScenarioExecution.scenario_id)
    ).all()
    defect_models = session.scalars(
        select(UatDefect)
        .where(UatDefect.cycle_id == cycle.cycle_id)
        .order_by(UatDefect.created_at.desc(), UatDefect.defect_id.desc())
    ).all()
    return UatCycleSummary(
        cycle_id=str(cycle.cycle_id),
        cycle_name=cycle.cycle_name,
        environment_name=cycle.environment_name,
        scenario_pack_path=cycle.scenario_pack_path,
        status=cycle.status,
        created_by=cycle.created_by,
        summary_notes=cycle.summary_notes,
        started_at=cycle.started_at,
        completed_at=cycle.completed_at,
        created_at=cycle.created_at,
        updated_at=cycle.updated_at,
        scenario_count=len(execution_models),
        defect_count=len(defect_models),
        scenario_status_counts=_build_counts(
            [execution.status for execution in execution_models],
            [execution_status.value for execution_status in UatExecutionStatus],
        ),
        defect_severity_counts=_build_counts(
            [defect.severity for defect in defect_models],
            [severity.value for severity in UatDefectSeverity],
        ),
        defect_status_counts=_build_counts(
            [defect.status for defect in defect_models],
            [defect_status.value for defect_status in UatDefectStatus],
        ),
    )


def _build_cycle_detail(session: Session, cycle: UatCycle) -> UatCycleDetail:
    summary = _build_cycle_summary(session, cycle)
    execution_models = session.scalars(
        select(UatScenarioExecution)
        .where(UatScenarioExecution.cycle_id == cycle.cycle_id)
        .order_by(UatScenarioExecution.scenario_id)
    ).all()
    defect_models = session.scalars(
        select(UatDefect)
        .where(UatDefect.cycle_id == cycle.cycle_id)
        .order_by(UatDefect.created_at.desc(), UatDefect.defect_id.desc())
    ).all()
    return UatCycleDetail(
        cycle_id=summary.cycle_id,
        cycle_name=summary.cycle_name,
        environment_name=summary.environment_name,
        scenario_pack_path=summary.scenario_pack_path,
        status=summary.status,
        created_by=summary.created_by,
        summary_notes=summary.summary_notes,
        started_at=summary.started_at,
        completed_at=summary.completed_at,
        created_at=summary.created_at,
        updated_at=summary.updated_at,
        scenario_count=summary.scenario_count,
        defect_count=summary.defect_count,
        scenario_status_counts=summary.scenario_status_counts,
        defect_severity_counts=summary.defect_severity_counts,
        defect_status_counts=summary.defect_status_counts,
        scenario_executions=[
            UatScenarioExecutionRecord(
                execution_id=str(execution.execution_id),
                scenario_id=execution.scenario_id,
                title=execution.title,
                actor_role=execution.actor_role,
                workflow=execution.workflow,
                entrypoint=execution.entrypoint,
                status=execution.status,
                execution_notes=execution.execution_notes,
                evidence_reference=execution.evidence_reference,
                executed_by=execution.executed_by,
                executed_at=execution.executed_at,
            )
            for execution in execution_models
        ],
        defects=[
            UatDefectRecord(
                defect_id=str(defect.defect_id),
                scenario_id=defect.scenario_id,
                severity=defect.severity,
                status=defect.status,
                title=defect.title,
                description=defect.description,
                reported_by=defect.reported_by,
                owner_name=defect.owner_name,
                external_reference=defect.external_reference,
                resolution_notes=defect.resolution_notes,
                created_at=defect.created_at,
                updated_at=defect.updated_at,
            )
            for defect in defect_models
        ],
    )


def _build_counts(observed_values: list[str], ordered_categories: list[str]) -> list[UatCount]:
    counts = Counter(observed_values)
    return [
        UatCount(category=category, count=counts.get(category, 0))
        for category in ordered_categories
    ]


def _resolve_pack_path(scenario_pack_path: str) -> Path:
    resolved_path = Path(scenario_pack_path)
    if not resolved_path.is_absolute():
        resolved_path = REPO_ROOT / resolved_path
    if not resolved_path.exists():
        raise FileNotFoundError(f"UAT scenario pack `{resolved_path}` does not exist.")
    return resolved_path


def _get_cycle(session: Session, cycle_id: str) -> UatCycle:
    cycle = session.get(UatCycle, UUID(cycle_id))
    if cycle is None:
        raise UatCycleNotFoundError(f"UAT cycle `{cycle_id}` was not found.")
    return cycle


def _get_execution(session: Session, cycle_uuid: UUID, scenario_id: str) -> UatScenarioExecution:
    execution = session.scalar(
        select(UatScenarioExecution).where(
            UatScenarioExecution.cycle_id == cycle_uuid,
            UatScenarioExecution.scenario_id == scenario_id,
        )
    )
    if execution is None:
        raise UatScenarioExecutionNotFoundError(
            f"Scenario `{scenario_id}` is not registered for this UAT cycle."
        )
    return execution


def _get_defect(session: Session, cycle_uuid: UUID, defect_id: str) -> UatDefect:
    defect = session.scalar(
        select(UatDefect).where(
            UatDefect.cycle_id == cycle_uuid,
            UatDefect.defect_id == UUID(defect_id),
        )
    )
    if defect is None:
        raise UatDefectNotFoundError(
            f"Defect `{defect_id}` is not registered for this UAT cycle."
        )
    return defect


def _assert_cycle_mutable(cycle: UatCycle) -> None:
    if cycle.status == UatCycleStatus.APPROVED.value:
        raise UatCycleConflictError(f"UAT cycle `{cycle.cycle_name}` is already approved.")


def _record_cycle_event(
    session: Session,
    *,
    cycle_id: UUID,
    event_type: str,
    actor_name: str,
    scenario_id: str | None = None,
    defect_id: UUID | None = None,
    event_notes: str | None = None,
    event_payload: dict[str, object] | None = None,
) -> None:
    event_timestamp = datetime.now(UTC)
    session.add(
        UatCycleEvent(
            cycle_id=cycle_id,
            event_type=event_type,
            actor_name=actor_name,
            scenario_id=scenario_id,
            defect_id=defect_id,
            event_notes=event_notes,
            created_at=event_timestamp,
            event_payload=(
                json.dumps(event_payload, sort_keys=True) if event_payload is not None else None
            ),
            updated_at=event_timestamp,
        )
    )


def _refresh_cycle_status(session: Session, cycle: UatCycle) -> None:
    execution_models = session.scalars(
        select(UatScenarioExecution).where(UatScenarioExecution.cycle_id == cycle.cycle_id)
    ).all()
    if not execution_models or all(
        execution.status == UatExecutionStatus.PLANNED.value for execution in execution_models
    ):
        cycle.status = UatCycleStatus.PLANNED.value
        cycle.started_at = None
        cycle.completed_at = None
        return

    if all(execution.status in TERMINAL_EXECUTION_STATUSES for execution in execution_models):
        cycle.status = UatCycleStatus.COMPLETED.value
        cycle.completed_at = datetime.now(UTC)
        return

    cycle.status = UatCycleStatus.IN_PROGRESS.value
    cycle.completed_at = None
