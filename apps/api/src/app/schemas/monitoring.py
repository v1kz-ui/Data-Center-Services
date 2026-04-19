from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict

from app.schemas.ingestion import MetroFreshnessReportResponse, SourceHealthSnapshotResponse
from app.schemas.orchestration import BatchPlanResponse, PlannedRunResponse


class MonitoringStatusCountResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    status: str
    count: int


class MonitoringAlertResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    severity: str
    code: str
    summary: str
    metro_id: str | None
    batch_id: str | None
    run_id: str | None


class MonitoringThresholdEvaluationResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    code: str
    severity: str
    observed_value: int
    threshold_value: int
    triggered: bool
    summary: str


class MonitoringOverviewResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    evaluated_at: datetime
    metro_id: str | None
    source_health: list[SourceHealthSnapshotResponse]
    freshness: MetroFreshnessReportResponse | None
    batch_status_counts: list[MonitoringStatusCountResponse]
    run_status_counts: list[MonitoringStatusCountResponse]
    latest_batch: BatchPlanResponse | None
    recent_failed_runs: list[PlannedRunResponse]
    alert_count: int
    alerts: list[MonitoringAlertResponse]
    threshold_trigger_count: int
    thresholds: list[MonitoringThresholdEvaluationResponse]
