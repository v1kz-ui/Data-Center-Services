from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class AdminSourceLoadRequest(BaseModel):
    source_version: str = Field(min_length=1)
    snapshot_ts: datetime | None = None
    records: list[dict[str, Any]] = Field(default_factory=list)


class RejectionDetailResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    row_number: int
    rejection_code: str
    rejection_message: str
    external_key: str | None
    raw_payload: str | None


class SourceLoadReportResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    snapshot_id: str
    source_id: str
    metro_id: str
    snapshot_ts: datetime
    source_version: str
    status: str
    row_count: int
    accepted_count: int
    rejected_count: int
    checksum: str | None
    error_message: str | None
    rejections: list[RejectionDetailResponse]


class SourceFreshnessStatusResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    source_id: str
    metro_id: str
    required: bool
    passed: bool
    freshness_code: str
    freshness_reason: str
    refresh_cadence: str
    max_age_hours: int
    latest_snapshot_id: str | None
    latest_snapshot_ts: datetime | None
    latest_snapshot_status: str | None
    age_hours: float | None


class MetroFreshnessReportResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    metro_id: str
    evaluated_at: datetime
    passed: bool
    statuses: list[SourceFreshnessStatusResponse]


class SourceHealthSnapshotResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    source_id: str
    metro_id: str
    latest_snapshot_id: str | None
    latest_snapshot_ts: datetime | None
    latest_snapshot_status: str | None
    row_count: int
    accepted_count: int
    rejected_count: int
    error_message: str | None
