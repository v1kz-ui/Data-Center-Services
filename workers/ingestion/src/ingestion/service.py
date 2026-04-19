from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from shapely import wkt
from shapely.errors import ShapelyError
from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from app.db.models.catalogs import SourceCatalog
from app.db.models.enums import SourceSnapshotStatus
from app.db.models.ingestion import SourceSnapshot
from app.db.models.source_data import RawZoning, SourceEvidence, SourceRecordRejection
from app.db.models.territory import CountyCatalog, ParcelRepPoint, RawParcel
from ingestion.adapters import SourceAdapter
from ingestion.models import (
    MetroFreshnessReport,
    RecordPayload,
    RejectionDetail,
    SourceFreshnessStatus,
    SourceHealthSnapshot,
    SourceLoadReport,
)

CADENCE_WINDOWS = {
    "daily": timedelta(hours=36),
    "weekly": timedelta(days=8),
    "monthly": timedelta(days=32),
}


class SourceConfigurationError(LookupError):
    """Raised when a source or metro is not configured for ingestion."""


class UnsupportedSourceError(ValueError):
    """Raised when a source has no registered ingestion path."""


def describe_service() -> dict[str, str]:
    return {
        "service": "ingestion",
        "purpose": (
            "Load source data, validate quality, quarantine bad rows, "
            "and compute freshness."
        ),
    }


def load_from_adapter(
    session: Session,
    adapter: SourceAdapter,
    metro_id: str,
    loaded_at: datetime | None = None,
) -> SourceLoadReport:
    normalized_metro_id = _canonicalize_metro_id(metro_id)
    loaded_at = _coerce_timestamp(loaded_at)
    records = [dict(record) for record in adapter.fetch_records(normalized_metro_id)]

    if adapter.source_id == "PARCEL":
        return ingest_parcel_records(
            session=session,
            metro_id=normalized_metro_id,
            source_version=adapter.source_version,
            records=records,
            loaded_at=loaded_at,
        )
    if adapter.source_id == "ZONING":
        return ingest_zoning_records(
            session=session,
            metro_id=normalized_metro_id,
            source_version=adapter.source_version,
            records=records,
            loaded_at=loaded_at,
        )
    return ingest_evidence_records(
        session=session,
        source_id=adapter.source_id,
        metro_id=normalized_metro_id,
        source_version=adapter.source_version,
        records=records,
        loaded_at=loaded_at,
    )


def ingest_parcel_records(
    session: Session,
    metro_id: str,
    source_version: str,
    records: Sequence[RecordPayload],
    loaded_at: datetime | None = None,
    connector_key: str | None = None,
) -> SourceLoadReport:
    return _ingest_records(
        session=session,
        source_id="PARCEL",
        metro_id=metro_id,
        source_version=source_version,
        records=records,
        loaded_at=loaded_at,
        connector_key=connector_key,
        processor=_process_parcel_records,
    )


def ingest_zoning_records(
    session: Session,
    metro_id: str,
    source_version: str,
    records: Sequence[RecordPayload],
    loaded_at: datetime | None = None,
    connector_key: str | None = None,
) -> SourceLoadReport:
    return _ingest_records(
        session=session,
        source_id="ZONING",
        metro_id=metro_id,
        source_version=source_version,
        records=records,
        loaded_at=loaded_at,
        connector_key=connector_key,
        processor=_process_zoning_records,
    )


def ingest_evidence_records(
    session: Session,
    source_id: str,
    metro_id: str,
    source_version: str,
    records: Sequence[RecordPayload],
    loaded_at: datetime | None = None,
    connector_key: str | None = None,
) -> SourceLoadReport:
    normalized_source_id = source_id.strip().upper()
    if normalized_source_id in {"PARCEL", "ZONING"}:
        raise UnsupportedSourceError(
            "Use the parcel or zoning ingestion path for canonical records."
        )

    return _ingest_records(
        session=session,
        source_id=normalized_source_id,
        metro_id=metro_id,
        source_version=source_version,
        records=records,
        loaded_at=loaded_at,
        connector_key=connector_key,
        processor=_process_evidence_records,
    )


def evaluate_freshness(
    session: Session,
    metro_id: str,
    evaluated_at: datetime | None = None,
) -> MetroFreshnessReport:
    normalized_metro_id = _canonicalize_metro_id(metro_id)
    evaluated_at = _coerce_timestamp(evaluated_at)
    statuses: list[SourceFreshnessStatus] = []

    for source in _get_active_sources_for_metro(session, normalized_metro_id):
        cadence_window = _get_cadence_window(source.refresh_cadence)
        latest_snapshot = _get_latest_snapshot(session, source.source_id, normalized_metro_id)
        required = source.block_refresh

        if latest_snapshot is None:
            statuses.append(
                SourceFreshnessStatus(
                    source_id=source.source_id,
                    metro_id=normalized_metro_id,
                    required=required,
                    passed=not required,
                    freshness_code="MISSING_SOURCE",
                    freshness_reason="No snapshot has been loaded for the required metro.",
                    refresh_cadence=source.refresh_cadence,
                    max_age_hours=int(cadence_window.total_seconds() // 3600),
                )
            )
            continue

        latest_snapshot_ts = _coerce_timestamp(latest_snapshot.snapshot_ts)
        age = evaluated_at - latest_snapshot_ts
        age_hours = round(age.total_seconds() / 3600, 2)
        status_value = latest_snapshot.status.value
        max_age_hours = int(cadence_window.total_seconds() // 3600)

        if latest_snapshot.status is not SourceSnapshotStatus.SUCCESS:
            statuses.append(
                SourceFreshnessStatus(
                    source_id=source.source_id,
                    metro_id=normalized_metro_id,
                    required=required,
                    passed=not required,
                    freshness_code="SOURCE_LOAD_ERROR",
                    freshness_reason="The latest snapshot is not in a successful state.",
                    refresh_cadence=source.refresh_cadence,
                    max_age_hours=max_age_hours,
                    latest_snapshot_id=str(latest_snapshot.snapshot_id),
                    latest_snapshot_ts=latest_snapshot_ts,
                    latest_snapshot_status=status_value,
                    age_hours=age_hours,
                )
            )
            continue

        if age > cadence_window:
            statuses.append(
                SourceFreshnessStatus(
                    source_id=source.source_id,
                    metro_id=normalized_metro_id,
                    required=required,
                    passed=not required,
                    freshness_code="STALE_SOURCE",
                    freshness_reason=(
                        "The latest successful snapshot is older than the allowed cadence."
                    ),
                    refresh_cadence=source.refresh_cadence,
                    max_age_hours=max_age_hours,
                    latest_snapshot_id=str(latest_snapshot.snapshot_id),
                    latest_snapshot_ts=latest_snapshot_ts,
                    latest_snapshot_status=status_value,
                    age_hours=age_hours,
                )
            )
            continue

        statuses.append(
            SourceFreshnessStatus(
                source_id=source.source_id,
                metro_id=normalized_metro_id,
                required=required,
                passed=True,
                freshness_code="FRESH",
                freshness_reason="The latest successful snapshot is within the allowed cadence.",
                refresh_cadence=source.refresh_cadence,
                max_age_hours=max_age_hours,
                latest_snapshot_id=str(latest_snapshot.snapshot_id),
                latest_snapshot_ts=latest_snapshot_ts,
                latest_snapshot_status=status_value,
                age_hours=age_hours,
            )
        )

    return MetroFreshnessReport(
        metro_id=normalized_metro_id,
        evaluated_at=evaluated_at,
        passed=all(status.passed for status in statuses),
        statuses=statuses,
    )


def summarize_source_health(
    session: Session,
    metro_id: str | None = None,
) -> list[SourceHealthSnapshot]:
    source_pairs: list[tuple[SourceCatalog, str]] = []

    if metro_id is None:
        sources = session.scalars(
            select(SourceCatalog).where(SourceCatalog.is_active.is_(True)).order_by(SourceCatalog.source_id)
        ).all()
        for source in sources:
            for covered_metro in _split_metro_coverage(source.metro_coverage):
                source_pairs.append((source, covered_metro))
    else:
        normalized_metro_id = _canonicalize_metro_id(metro_id)
        for source in _get_active_sources_for_metro(session, normalized_metro_id):
            source_pairs.append((source, normalized_metro_id))

    summaries: list[SourceHealthSnapshot] = []
    for source, covered_metro in source_pairs:
        latest_snapshot = _get_latest_snapshot(session, source.source_id, covered_metro)
        if latest_snapshot is None:
            summaries.append(
                SourceHealthSnapshot(
                    source_id=source.source_id,
                    metro_id=covered_metro,
                    latest_snapshot_id=None,
                    latest_snapshot_ts=None,
                    latest_snapshot_status=None,
                    row_count=0,
                    accepted_count=0,
                    rejected_count=0,
                    error_message="No snapshot loaded.",
                )
            )
            continue

        rejected_count = _count_rejections(session, latest_snapshot.snapshot_id)
        row_count = latest_snapshot.row_count
        summaries.append(
            SourceHealthSnapshot(
                source_id=source.source_id,
                metro_id=covered_metro,
                latest_snapshot_id=str(latest_snapshot.snapshot_id),
                latest_snapshot_ts=_coerce_timestamp(latest_snapshot.snapshot_ts),
                latest_snapshot_status=latest_snapshot.status.value,
                row_count=row_count,
                accepted_count=max(row_count - rejected_count, 0),
                rejected_count=rejected_count,
                error_message=latest_snapshot.error_message,
            )
        )

    return sorted(summaries, key=lambda summary: (summary.metro_id, summary.source_id))


def _ingest_records(
    session: Session,
    source_id: str,
    metro_id: str,
    source_version: str,
    records: Sequence[RecordPayload],
    loaded_at: datetime | None,
    connector_key: str | None,
    processor: Any,
) -> SourceLoadReport:
    normalized_source_id = source_id.strip().upper()
    normalized_metro_id = _canonicalize_metro_id(metro_id)
    loaded_at = _coerce_timestamp(loaded_at)
    source = _get_source(session, normalized_source_id)
    _assert_source_covers_metro(source, normalized_metro_id)

    checksum = _build_checksum(records)
    row_count = len(records)

    try:
        snapshot = SourceSnapshot(
            source_id=normalized_source_id,
            metro_id=normalized_metro_id,
            connector_key=_safe_string(connector_key),
            snapshot_ts=loaded_at,
            source_version=source_version,
            row_count=row_count,
            checksum=checksum,
            status=SourceSnapshotStatus.SUCCESS,
        )
        session.add(snapshot)
        session.flush()

        rejections = processor(
            session=session,
            metro_id=normalized_metro_id,
            snapshot=snapshot,
            records=list(records),
        )
        _persist_rejections(session, snapshot.snapshot_id, rejections)

        if rejections:
            snapshot.status = SourceSnapshotStatus.QUARANTINED
            snapshot.error_message = f"{len(rejections)} row(s) were quarantined during ingest."

        session.commit()
        return _to_load_report(snapshot, rejections)
    except Exception as exc:
        session.rollback()

        failed_snapshot = SourceSnapshot(
            source_id=normalized_source_id,
            metro_id=normalized_metro_id,
            connector_key=_safe_string(connector_key),
            snapshot_ts=loaded_at,
            source_version=source_version,
            row_count=row_count,
            checksum=checksum,
            status=SourceSnapshotStatus.FAILED,
            error_message=str(exc),
        )
        session.add(failed_snapshot)
        session.commit()
        return _to_load_report(failed_snapshot, [])


def _process_parcel_records(
    session: Session,
    metro_id: str,
    snapshot: SourceSnapshot,
    records: list[RecordPayload],
) -> list[RejectionDetail]:
    counties = _load_county_lookup(session, metro_id)
    seen_parcel_ids: set[str] = set()
    normalized_records: list[dict[str, Any]] = []
    rejections: list[RejectionDetail] = []

    for row_number, record in enumerate(records, start=1):
        parcel_id = _safe_string(record.get("parcel_id"))
        if parcel_id and parcel_id in seen_parcel_ids:
            rejections.append(
                _build_rejection(
                    row_number,
                    "DUPLICATE_RECORD",
                    "Duplicate parcel_id found within the same source payload.",
                    parcel_id,
                    record,
                )
            )
            continue

        try:
            county_fips = _require_string(record, "county_fips")
            if county_fips not in counties:
                raise ValueError("county_fips is not mapped to the requested metro.")

            geometry_wkt = _require_string(record, "geometry_wkt")
            geometry = _load_geometry(geometry_wkt)
            rep_point_wkt = _normalize_rep_point(record.get("rep_point_wkt"), geometry)

            normalized_records.append(
                {
                    "parcel_id": _require_string(record, "parcel_id"),
                    "county_fips": county_fips,
                    "apn": _safe_string(record.get("apn")),
                    "acreage": _coerce_decimal(record.get("acreage")),
                    "geometry_wkt": geometry.wkt,
                    "rep_point_wkt": rep_point_wkt,
                    "lineage_key": _require_string(record, "lineage_key"),
                }
            )
            seen_parcel_ids.add(parcel_id or normalized_records[-1]["parcel_id"])
        except (KeyError, TypeError, ValueError) as exc:
            rejections.append(
                _build_rejection(
                    row_number,
                    "VALIDATION_FAILURE",
                    str(exc),
                    parcel_id,
                    record,
                )
            )

    parcel_ids = [row["parcel_id"] for row in normalized_records]
    existing_parcels = {
        parcel.parcel_id: parcel
        for parcel in session.scalars(
            select(RawParcel).where(RawParcel.parcel_id.in_(parcel_ids))
        ).all()
    }
    existing_rep_points = {
        rep_point.parcel_id: rep_point
        for rep_point in session.scalars(
            select(ParcelRepPoint).where(ParcelRepPoint.parcel_id.in_(parcel_ids))
        ).all()
    }

    for row in normalized_records:
        parcel = existing_parcels.get(row["parcel_id"])
        if parcel is None:
            parcel = RawParcel(
                parcel_id=row["parcel_id"],
                county_fips=row["county_fips"],
                metro_id=metro_id,
                apn=row["apn"],
                acreage=row["acreage"],
                geometry_wkt=row["geometry_wkt"],
                source_snapshot_id=snapshot.snapshot_id,
                lineage_key=row["lineage_key"],
                is_active=True,
            )
            session.add(parcel)
            existing_parcels[parcel.parcel_id] = parcel
        else:
            parcel.county_fips = row["county_fips"]
            parcel.metro_id = metro_id
            parcel.apn = row["apn"]
            parcel.acreage = row["acreage"]
            parcel.geometry_wkt = row["geometry_wkt"]
            parcel.source_snapshot_id = snapshot.snapshot_id
            parcel.lineage_key = row["lineage_key"]
            parcel.is_active = True

        rep_point = existing_rep_points.get(row["parcel_id"])
        if rep_point is None:
            rep_point = ParcelRepPoint(
                parcel_id=row["parcel_id"],
                rep_point_wkt=row["rep_point_wkt"],
                geometry_method="representative_point",
                source_snapshot_id=snapshot.snapshot_id,
            )
            session.add(rep_point)
            existing_rep_points[row["parcel_id"]] = rep_point
        else:
            rep_point.rep_point_wkt = row["rep_point_wkt"]
            rep_point.geometry_method = "representative_point"
            rep_point.source_snapshot_id = snapshot.snapshot_id

    return rejections


def _process_zoning_records(
    session: Session,
    metro_id: str,
    snapshot: SourceSnapshot,
    records: list[RecordPayload],
) -> list[RejectionDetail]:
    counties = _load_county_lookup(session, metro_id)
    existing_parcels = {
        parcel.parcel_id: parcel
        for parcel in session.scalars(select(RawParcel).where(RawParcel.metro_id == metro_id)).all()
    }
    seen_keys: set[str] = set()
    normalized_records: list[dict[str, str | None]] = []
    rejections: list[RejectionDetail] = []

    for row_number, record in enumerate(records, start=1):
        parcel_id = _safe_string(record.get("parcel_id"))
        if parcel_id and parcel_id in seen_keys:
            rejections.append(
                _build_rejection(
                    row_number,
                    "DUPLICATE_RECORD",
                    "Duplicate parcel zoning record found within the same source payload.",
                    parcel_id,
                    record,
                )
            )
            continue

        try:
            county_fips = _require_string(record, "county_fips")
            if county_fips not in counties:
                raise ValueError("county_fips is not mapped to the requested metro.")

            parcel_id = _require_string(record, "parcel_id")
            parcel = existing_parcels.get(parcel_id)
            if parcel is None:
                raise ValueError("parcel_id does not exist in canonical parcel storage.")
            if parcel.county_fips != county_fips:
                raise ValueError("parcel county_fips does not match the zoning payload.")

            normalized_records.append(
                {
                    "parcel_id": parcel_id,
                    "county_fips": county_fips,
                    "zoning_code": _require_string(record, "zoning_code"),
                    "land_use_code": _safe_string(record.get("land_use_code")),
                    "lineage_key": _require_string(record, "lineage_key"),
                }
            )
            seen_keys.add(parcel_id)
        except (KeyError, TypeError, ValueError) as exc:
            rejections.append(
                _build_rejection(
                    row_number,
                    "VALIDATION_FAILURE",
                    str(exc),
                    parcel_id,
                    record,
                )
            )

    for row in normalized_records:
        session.execute(
            update(RawZoning)
            .where(RawZoning.parcel_id == row["parcel_id"], RawZoning.is_active.is_(True))
            .values(is_active=False)
        )
        session.add(
            RawZoning(
                parcel_id=row["parcel_id"],
                county_fips=row["county_fips"],
                metro_id=metro_id,
                zoning_code=row["zoning_code"],
                land_use_code=row["land_use_code"],
                source_snapshot_id=snapshot.snapshot_id,
                lineage_key=row["lineage_key"],
                is_active=True,
            )
        )

    return rejections


def _process_evidence_records(
    session: Session,
    metro_id: str,
    snapshot: SourceSnapshot,
    records: list[RecordPayload],
) -> list[RejectionDetail]:
    counties = _load_county_lookup(session, metro_id)
    existing_parcels = {
        parcel.parcel_id: parcel
        for parcel in session.scalars(select(RawParcel).where(RawParcel.metro_id == metro_id)).all()
    }
    seen_keys: set[tuple[str, str]] = set()
    normalized_records: list[dict[str, str | None]] = []
    rejections: list[RejectionDetail] = []

    for row_number, record in enumerate(records, start=1):
        record_key = _safe_string(record.get("record_key"))
        attribute_name = _safe_string(record.get("attribute_name"))
        dedupe_key = (record_key or "", attribute_name or "")
        if record_key and attribute_name and dedupe_key in seen_keys:
            rejections.append(
                _build_rejection(
                    row_number,
                    "DUPLICATE_RECORD",
                    "Duplicate evidence record_key and attribute_name found in the payload.",
                    record_key,
                    record,
                )
            )
            continue

        try:
            county_fips = _safe_string(record.get("county_fips"))
            parcel_id = _safe_string(record.get("parcel_id"))
            if county_fips and county_fips not in counties:
                raise ValueError("county_fips is not mapped to the requested metro.")
            if parcel_id:
                parcel = existing_parcels.get(parcel_id)
                if parcel is None:
                    raise ValueError("parcel_id does not exist in canonical parcel storage.")
                if county_fips and parcel.county_fips != county_fips:
                    raise ValueError("parcel county_fips does not match the evidence payload.")

            normalized_records.append(
                {
                    "record_key": _require_string(record, "record_key"),
                    "attribute_name": _require_string(record, "attribute_name"),
                    "attribute_value": _require_string(record, "attribute_value"),
                    "lineage_key": _require_string(record, "lineage_key"),
                    "county_fips": county_fips,
                    "parcel_id": parcel_id,
                }
            )
            seen_keys.add(
                (
                    _require_string(record, "record_key"),
                    _require_string(record, "attribute_name"),
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            rejections.append(
                _build_rejection(
                    row_number,
                    "VALIDATION_FAILURE",
                    str(exc),
                    record_key,
                    record,
                )
            )

    for row in normalized_records:
        session.execute(
            update(SourceEvidence)
            .where(
                SourceEvidence.source_id == snapshot.source_id,
                SourceEvidence.metro_id == metro_id,
                SourceEvidence.record_key == row["record_key"],
                SourceEvidence.attribute_name == row["attribute_name"],
                SourceEvidence.is_active.is_(True),
            )
            .values(is_active=False)
        )
        session.add(
            SourceEvidence(
                source_id=snapshot.source_id,
                metro_id=metro_id,
                county_fips=row["county_fips"],
                parcel_id=row["parcel_id"],
                source_snapshot_id=snapshot.snapshot_id,
                record_key=row["record_key"],
                attribute_name=row["attribute_name"],
                attribute_value=row["attribute_value"],
                lineage_key=row["lineage_key"],
                is_active=True,
            )
        )

    return rejections


def _get_source(session: Session, source_id: str) -> SourceCatalog:
    source = session.scalar(select(SourceCatalog).where(SourceCatalog.source_id == source_id))
    if source is None or not source.is_active:
        raise SourceConfigurationError(f"Source `{source_id}` is not active in the source catalog.")
    return source


def _get_active_sources_for_metro(session: Session, metro_id: str) -> list[SourceCatalog]:
    sources = session.scalars(
        select(SourceCatalog)
        .where(SourceCatalog.is_active.is_(True))
        .order_by(SourceCatalog.source_id)
    ).all()
    return [
        source
        for source in sources
        if metro_id in _split_metro_coverage(source.metro_coverage)
    ]


def _get_latest_snapshot(
    session: Session,
    source_id: str,
    metro_id: str,
    *,
    connector_key: str | None = None,
) -> SourceSnapshot | None:
    statement = select(SourceSnapshot).where(
        SourceSnapshot.source_id == source_id,
        SourceSnapshot.metro_id == metro_id,
    )
    if connector_key is None:
        statement = statement.order_by(SourceSnapshot.snapshot_ts.desc()).limit(1)
    else:
        statement = statement.where(SourceSnapshot.connector_key == connector_key).order_by(
            SourceSnapshot.snapshot_ts.desc()
        ).limit(1)
    return session.scalar(statement)


def _assert_source_covers_metro(source: SourceCatalog, metro_id: str) -> None:
    if metro_id not in _split_metro_coverage(source.metro_coverage):
        raise SourceConfigurationError(
            f"Source `{source.source_id}` is not configured for metro `{metro_id}`."
        )


def _split_metro_coverage(metro_coverage: str | None) -> list[str]:
    if not metro_coverage:
        return []
    return [metro.strip().upper() for metro in metro_coverage.split(",") if metro.strip()]


def _load_county_lookup(session: Session, metro_id: str) -> set[str]:
    counties = session.scalars(
        select(CountyCatalog.county_fips).where(CountyCatalog.metro_id == metro_id)
    ).all()
    if not counties:
        raise SourceConfigurationError(f"Metro `{metro_id}` has no county mapping in the catalog.")
    return set(counties)


def _persist_rejections(
    session: Session,
    snapshot_id: Any,
    rejections: Sequence[RejectionDetail],
) -> None:
    for rejection in rejections:
        session.add(
            SourceRecordRejection(
                snapshot_id=snapshot_id,
                row_number=rejection.row_number,
                external_key=rejection.external_key,
                rejection_code=rejection.rejection_code,
                rejection_message=rejection.rejection_message,
                raw_payload=rejection.raw_payload,
            )
        )


def _build_rejection(
    row_number: int,
    rejection_code: str,
    rejection_message: str,
    external_key: str | None,
    payload: RecordPayload,
) -> RejectionDetail:
    return RejectionDetail(
        row_number=row_number,
        rejection_code=rejection_code,
        rejection_message=rejection_message,
        external_key=external_key,
        raw_payload=json.dumps(payload, sort_keys=True, default=str),
    )


def _to_load_report(
    snapshot: SourceSnapshot,
    rejections: Sequence[RejectionDetail],
) -> SourceLoadReport:
    row_count = snapshot.row_count
    rejected_count = len(rejections)
    accepted_count = (
        0
        if snapshot.status is SourceSnapshotStatus.FAILED
        else max(row_count - rejected_count, 0)
    )
    return SourceLoadReport(
        snapshot_id=str(snapshot.snapshot_id),
        source_id=snapshot.source_id,
        metro_id=snapshot.metro_id,
        snapshot_ts=_coerce_timestamp(snapshot.snapshot_ts),
        source_version=snapshot.source_version,
        status=snapshot.status.value,
        row_count=row_count,
        accepted_count=accepted_count,
        rejected_count=rejected_count,
        checksum=snapshot.checksum,
        error_message=snapshot.error_message,
        rejections=list(rejections),
    )


def _count_rejections(session: Session, snapshot_id: Any) -> int:
    count = session.scalar(
        select(func.count())
        .select_from(SourceRecordRejection)
        .where(SourceRecordRejection.snapshot_id == snapshot_id)
    )
    return int(count or 0)


def _build_checksum(records: Sequence[RecordPayload]) -> str:
    payload = json.dumps(list(records), sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _load_geometry(geometry_wkt: str):
    try:
        geometry = wkt.loads(geometry_wkt)
    except (TypeError, ShapelyError) as exc:
        raise ValueError("geometry_wkt is not valid WKT.") from exc

    if geometry.is_empty:
        raise ValueError("geometry_wkt cannot be empty.")
    return geometry


def _normalize_rep_point(rep_point_wkt: Any, geometry: Any) -> str:
    if rep_point_wkt:
        rep_point = _load_geometry(str(rep_point_wkt))
        if rep_point.geom_type != "Point":
            raise ValueError("rep_point_wkt must resolve to a point geometry.")
        return rep_point.wkt
    return geometry.representative_point().wkt


def _coerce_decimal(value: Any) -> Decimal:
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, TypeError) as exc:
        raise ValueError("acreage must be numeric.") from exc

    if decimal_value < 0:
        raise ValueError("acreage must be greater than or equal to zero.")
    return decimal_value


def _require_string(record: RecordPayload, key: str) -> str:
    value = _safe_string(record.get(key))
    if value is None:
        raise KeyError(f"{key} is required.")
    return value


def _safe_string(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _canonicalize_metro_id(metro_id: str) -> str:
    normalized = metro_id.strip().upper()
    if not normalized:
        raise SourceConfigurationError("metro_id is required.")
    return normalized


def _coerce_timestamp(timestamp: datetime | None) -> datetime:
    normalized = timestamp or datetime.now(UTC)
    if normalized.tzinfo is None:
        return normalized.replace(tzinfo=UTC)
    return normalized.astimezone(UTC)


def _get_cadence_window(refresh_cadence: str) -> timedelta:
    return CADENCE_WINDOWS.get(refresh_cadence.lower(), timedelta(days=2))
