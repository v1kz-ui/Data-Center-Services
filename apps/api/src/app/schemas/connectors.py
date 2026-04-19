from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class ConnectorDefinitionSummaryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    connector_key: str
    source_id: str
    metro_id: str
    interface_name: str
    adapter_type: str
    enabled: bool
    inventory_if_codes: list[str]
    priority: int
    description: str | None
    preprocess_strategy: str | None
    latest_snapshot_ts: datetime | None
    latest_snapshot_status: str | None
    checkpoint_ts: datetime | None
    checkpoint_cursor: str | None


class SourceRefreshPlanItemResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    connector_key: str
    source_id: str
    metro_id: str
    interface_name: str
    refresh_cadence: str
    enabled: bool
    priority: int
    preprocess_strategy: str | None
    due: bool
    due_reason: str
    next_due_at: datetime | None
    latest_snapshot_ts: datetime | None
    latest_snapshot_status: str | None
    checkpoint_ts: datetime | None
    checkpoint_cursor: str | None


class SourceRefreshJobReportResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    job_id: str
    source_id: str
    metro_id: str
    connector_key: str
    trigger_mode: str
    actor_name: str
    status: str
    started_at: datetime
    completed_at: datetime | None
    attempt_count: int
    source_version: str | None
    snapshot_id: str | None
    row_count: int
    accepted_count: int
    rejected_count: int
    checkpoint_in_ts: datetime | None
    checkpoint_out_ts: datetime | None
    checkpoint_cursor_in: str | None
    checkpoint_cursor_out: str | None
    error_message: str | None


class SourceRefreshBatchReportResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    requested_at: datetime
    total_due: int
    completed: int
    reports: list[SourceRefreshJobReportResponse]
