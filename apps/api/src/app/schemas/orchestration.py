from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class BatchRunCountResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    running: int
    failed: int
    completed: int


class PlannedRunResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    run_id: str
    batch_id: str
    metro_id: str
    profile_name: str | None
    status: str
    failure_reason: str | None
    started_at: datetime | None
    completed_at: datetime | None


class BatchPlanRequest(BaseModel):
    metro_ids: list[str] = Field(min_length=1)


class OperatorActionRequest(BaseModel):
    actor_name: str | None = Field(default=None, min_length=1, max_length=255)
    action_reason: str | None = Field(default=None, max_length=2000)


class ActivationCheckIssueResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    code: str
    detail: str
    run_id: str | None
    metro_id: str | None


class ActivationCheckResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    batch_id: str
    status: str
    expected_metros: int
    completed_metros: int
    checked_at: datetime
    activation_ready: bool
    issue_count: int
    run_counts: BatchRunCountResponse
    issues: list[ActivationCheckIssueResponse]


class BatchPlanResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    batch_id: str
    status: str
    expected_metros: int
    completed_metros: int
    activated_at: datetime | None
    activation_ready: bool
    run_counts: BatchRunCountResponse
    runs: list[PlannedRunResponse]


class RunRetryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    run: PlannedRunResponse
    batch: BatchPlanResponse


class BatchRerunResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    source_batch_id: str
    replacement_batch: BatchPlanResponse


class OperatorActionRecordResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

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
