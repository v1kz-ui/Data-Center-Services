from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models.enums import UatDefectSeverity, UatDefectStatus, UatExecutionStatus
from app.db.models.uat import UatCycleEvent
from app.services.uat import (
    EVENT_TYPE_CYCLE_CREATED,
    EVENT_TYPE_CYCLE_FINALIZED,
    EVENT_TYPE_DEFECT_LOGGED,
    EVENT_TYPE_DEFECT_UPDATED,
    EVENT_TYPE_SCENARIO_RESULT_RECORDED,
    TERMINAL_EXECUTION_STATUSES,
    UatCycleDetail,
    UatDefectRecord,
    get_uat_cycle,
)

EVENT_TYPE_SORT_ORDER = {
    EVENT_TYPE_CYCLE_CREATED: 0,
    EVENT_TYPE_SCENARIO_RESULT_RECORDED: 1,
    EVENT_TYPE_DEFECT_LOGGED: 2,
    EVENT_TYPE_DEFECT_UPDATED: 3,
    EVENT_TYPE_CYCLE_FINALIZED: 4,
}


@dataclass(slots=True)
class UatCycleEventRecord:
    event_id: str
    event_type: str
    actor_name: str
    scenario_id: str | None
    defect_id: str | None
    event_notes: str | None
    event_payload: dict[str, Any] | None
    created_at: datetime


@dataclass(slots=True)
class UatApprovalReadiness:
    approval_ready: bool
    blocking_issue_count: int
    blocking_issues: list[str]
    attention_item_count: int
    attention_items: list[str]
    terminal_scenario_count: int
    non_terminal_scenario_count: int
    evidence_captured_count: int
    missing_evidence_count: int
    open_defect_count: int
    open_high_severity_defect_count: int


@dataclass(slots=True)
class UatSignoffReport:
    report_version: str
    export_scope: str
    exported_at: datetime
    exported_by: str
    cycle: UatCycleDetail
    approval_readiness: UatApprovalReadiness
    open_defects: list[UatDefectRecord]
    event_history: list[UatCycleEventRecord]


def build_uat_signoff_report(
    session: Session,
    cycle_id: str,
    *,
    exported_by: str,
) -> UatSignoffReport:
    cycle = get_uat_cycle(session, cycle_id)
    readiness = _build_approval_readiness(cycle)
    event_models = session.scalars(
        select(UatCycleEvent)
        .where(UatCycleEvent.cycle_id == UUID(cycle.cycle_id))
        .order_by(UatCycleEvent.created_at.asc(), UatCycleEvent.event_id.asc())
    ).all()
    ordered_event_models = sorted(
        event_models,
        key=lambda event: (
            event.created_at,
            EVENT_TYPE_SORT_ORDER.get(event.event_type, 99),
            str(event.event_id),
        ),
    )

    return UatSignoffReport(
        report_version="phase7-uat-signoff-v1",
        export_scope="cycle_signoff",
        exported_at=datetime.now(UTC),
        exported_by=exported_by,
        cycle=cycle,
        approval_readiness=readiness,
        open_defects=[
            defect for defect in cycle.defects if defect.status == UatDefectStatus.OPEN.value
        ],
        event_history=[
            UatCycleEventRecord(
                event_id=str(event.event_id),
                event_type=event.event_type,
                actor_name=event.actor_name,
                scenario_id=event.scenario_id,
                defect_id=str(event.defect_id) if event.defect_id is not None else None,
                event_notes=event.event_notes,
                event_payload=(
                    json.loads(event.event_payload) if event.event_payload is not None else None
                ),
                created_at=event.created_at,
            )
            for event in ordered_event_models
        ],
    )


def _build_approval_readiness(cycle: UatCycleDetail) -> UatApprovalReadiness:
    terminal_scenarios = [
        scenario
        for scenario in cycle.scenario_executions
        if scenario.status in TERMINAL_EXECUTION_STATUSES
    ]
    non_terminal_scenarios = [
        scenario
        for scenario in cycle.scenario_executions
        if scenario.status not in TERMINAL_EXECUTION_STATUSES
    ]
    failed_or_blocked_scenarios = [
        scenario
        for scenario in cycle.scenario_executions
        if scenario.status in {UatExecutionStatus.FAILED.value, UatExecutionStatus.BLOCKED.value}
    ]
    missing_evidence_count = sum(
        1 for scenario in terminal_scenarios if not scenario.evidence_reference
    )
    evidence_captured_count = len(terminal_scenarios) - missing_evidence_count
    open_defects = [
        defect for defect in cycle.defects if defect.status == UatDefectStatus.OPEN.value
    ]
    open_high_severity_defects = [
        defect
        for defect in open_defects
        if defect.severity in {UatDefectSeverity.CRITICAL.value, UatDefectSeverity.HIGH.value}
    ]
    lower_severity_open_defects = [
        defect
        for defect in open_defects
        if defect.severity not in {UatDefectSeverity.CRITICAL.value, UatDefectSeverity.HIGH.value}
    ]

    blocking_issues: list[str] = []
    if non_terminal_scenarios:
        blocking_issues.append(
            f"{len(non_terminal_scenarios)} scenarios remain non-terminal."
        )
    if open_high_severity_defects:
        blocking_issues.append(
            f"{len(open_high_severity_defects)} critical or high severity defects remain open."
        )

    attention_items: list[str] = []
    if failed_or_blocked_scenarios:
        attention_items.append(
            f"{len(failed_or_blocked_scenarios)} scenarios ended failed or blocked."
        )
    if missing_evidence_count:
        attention_items.append(
            f"{missing_evidence_count} terminal scenarios are missing evidence references."
        )
    if lower_severity_open_defects:
        attention_items.append(
            f"{len(lower_severity_open_defects)} lower-severity defects remain open."
        )
    if cycle.status == "rework_required":
        attention_items.append(
            "Cycle is currently marked rework_required and needs a follow-up approval decision."
        )

    return UatApprovalReadiness(
        approval_ready=not blocking_issues,
        blocking_issue_count=len(blocking_issues),
        blocking_issues=blocking_issues,
        attention_item_count=len(attention_items),
        attention_items=attention_items,
        terminal_scenario_count=len(terminal_scenarios),
        non_terminal_scenario_count=len(non_terminal_scenarios),
        evidence_captured_count=evidence_captured_count,
        missing_evidence_count=missing_evidence_count,
        open_defect_count=len(open_defects),
        open_high_severity_defect_count=len(open_high_severity_defects),
    )
