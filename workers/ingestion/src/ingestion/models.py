from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class ParcelSourceRecord:
    parcel_id: str
    county_fips: str
    acreage: str | float | int
    geometry_wkt: str
    lineage_key: str
    apn: str | None = None
    rep_point_wkt: str | None = None


@dataclass(slots=True)
class ZoningSourceRecord:
    parcel_id: str
    county_fips: str
    zoning_code: str
    lineage_key: str
    land_use_code: str | None = None


@dataclass(slots=True)
class EvidenceSourceRecord:
    record_key: str
    attribute_name: str
    attribute_value: str
    lineage_key: str
    county_fips: str | None = None
    parcel_id: str | None = None


@dataclass(slots=True)
class RejectionDetail:
    row_number: int
    rejection_code: str
    rejection_message: str
    external_key: str | None
    raw_payload: str | None = None


@dataclass(slots=True)
class SourceLoadReport:
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
    rejections: list[RejectionDetail] = field(default_factory=list)


@dataclass(slots=True)
class SourceFreshnessStatus:
    source_id: str
    metro_id: str
    required: bool
    passed: bool
    freshness_code: str
    freshness_reason: str
    refresh_cadence: str
    max_age_hours: int
    latest_snapshot_id: str | None = None
    latest_snapshot_ts: datetime | None = None
    latest_snapshot_status: str | None = None
    age_hours: float | None = None


@dataclass(slots=True)
class MetroFreshnessReport:
    metro_id: str
    evaluated_at: datetime
    passed: bool
    statuses: list[SourceFreshnessStatus]


@dataclass(slots=True)
class SourceHealthSnapshot:
    source_id: str
    metro_id: str
    latest_snapshot_id: str | None
    latest_snapshot_ts: datetime | None
    latest_snapshot_status: str | None
    row_count: int
    accepted_count: int
    rejected_count: int
    error_message: str | None


RecordPayload = dict[str, Any]
