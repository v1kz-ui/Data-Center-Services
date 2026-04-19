from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class BatchRunCounts:
    running: int
    failed: int
    completed: int


@dataclass(slots=True)
class PlannedRun:
    run_id: str
    batch_id: str
    metro_id: str
    profile_name: str | None
    status: str
    failure_reason: str | None
    started_at: datetime | None
    completed_at: datetime | None


@dataclass(slots=True)
class PlannedBatch:
    batch_id: str
    status: str
    expected_metros: int
    completed_metros: int
    activated_at: datetime | None
    activation_ready: bool
    run_counts: BatchRunCounts
    runs: list[PlannedRun]


@dataclass(slots=True)
class ActivationCheckIssue:
    code: str
    detail: str
    run_id: str | None
    metro_id: str | None


@dataclass(slots=True)
class ActivationCheck:
    batch_id: str
    status: str
    expected_metros: int
    completed_metros: int
    checked_at: datetime
    activation_ready: bool
    issue_count: int
    run_counts: BatchRunCounts
    issues: list[ActivationCheckIssue]


@dataclass(slots=True)
class OperatorActionRecord:
    action_event_id: str
    action_type: str
    target_type: str
    target_id: str
    batch_id: str | None
    run_id: str | None
    actor_name: str
    action_reason: str | None
    action_payload: dict[str, Any] | None
    created_at: datetime
