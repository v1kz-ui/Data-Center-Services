from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from app.schemas.ingestion import MetroFreshnessReportResponse
from app.schemas.orchestration import (
    BatchPlanResponse,
    OperatorActionRecordResponse,
    PlannedRunResponse,
)
from app.schemas.scoring import ParcelScoringDetailResponse


class AuditSourceSnapshotRecordResponse(BaseModel):
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
    source_version: str | None
    checksum: str | None
    row_count: int
    accepted_count: int
    rejected_count: int
    error_message: str | None


class AuditSourceEvidenceRecordResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    source_id: str
    source_snapshot_id: str | None
    record_key: str
    attribute_name: str
    attribute_value: str
    lineage_key: str
    county_fips: str | None
    parcel_id: str | None
    created_at: datetime


class AuditParcelContextResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    parcel_id: str
    county_fips: str
    metro_id: str
    apn: str | None
    acreage: Decimal
    geometry_wkt: str
    rep_point_wkt: str | None
    geometry_method: str | None
    parcel_source_snapshot_id: str | None
    parcel_lineage_key: str
    zoning_code: str | None
    land_use_code: str | None
    zoning_source_snapshot_id: str | None
    zoning_lineage_key: str | None


class AuditParcelEvidencePackageResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    parcel_context: AuditParcelContextResponse
    parcel_detail: ParcelScoringDetailResponse
    source_evidence: list[AuditSourceEvidenceRecordResponse]


class AuditPackageResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    package_version: str
    export_scope: str
    exported_at: datetime
    exported_by: str
    run: PlannedRunResponse
    batch: BatchPlanResponse
    freshness: MetroFreshnessReportResponse
    source_snapshots: list[AuditSourceSnapshotRecordResponse]
    operator_actions: list[OperatorActionRecordResponse]
    parcel_evidence: AuditParcelEvidencePackageResponse | None
