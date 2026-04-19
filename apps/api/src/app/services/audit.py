from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models.ingestion import SourceSnapshot
from app.db.models.source_data import RawZoning, SourceEvidence
from app.db.models.territory import ParcelRepPoint, RawParcel
from ingestion.models import MetroFreshnessReport
from ingestion.service import evaluate_freshness, summarize_source_health
from orchestrator.models import OperatorActionRecord, PlannedBatch, PlannedRun
from orchestrator.service import get_batch, get_run, list_operator_actions
from scoring.models import ParcelScoringDetail
from scoring.service import get_parcel_scoring_detail


@dataclass(slots=True)
class AuditSourceSnapshotRecord:
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


@dataclass(slots=True)
class AuditSourceEvidenceRecord:
    source_id: str
    source_snapshot_id: str | None
    record_key: str
    attribute_name: str
    attribute_value: str
    lineage_key: str
    county_fips: str | None
    parcel_id: str | None
    created_at: datetime


@dataclass(slots=True)
class AuditParcelContext:
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


@dataclass(slots=True)
class AuditParcelEvidencePackage:
    parcel_context: AuditParcelContext
    parcel_detail: ParcelScoringDetail
    source_evidence: list[AuditSourceEvidenceRecord]


@dataclass(slots=True)
class AuditPackage:
    package_version: str
    export_scope: str
    exported_at: datetime
    exported_by: str
    run: PlannedRun
    batch: PlannedBatch
    freshness: MetroFreshnessReport
    source_snapshots: list[AuditSourceSnapshotRecord]
    operator_actions: list[OperatorActionRecord]
    parcel_evidence: AuditParcelEvidencePackage | None


def build_run_audit_package(
    session: Session,
    run_id: str | UUID,
    *,
    exported_by: str,
    parcel_id: str | None = None,
) -> AuditPackage:
    run = get_run(session, run_id)
    batch = get_batch(session, run.batch_id)
    freshness = evaluate_freshness(session, run.metro_id)
    source_snapshots = _build_source_snapshot_records(session, freshness)
    operator_actions = list_operator_actions(session, batch_id=run.batch_id, limit=50)
    parcel_evidence = None

    if parcel_id is not None:
        parcel_evidence = _build_parcel_evidence_package(
            session,
            run.run_id,
            run.metro_id,
            parcel_id,
        )

    return AuditPackage(
        package_version="phase7-audit-v1",
        export_scope="run_with_parcel" if parcel_evidence is not None else "run",
        exported_at=datetime.now(UTC),
        exported_by=exported_by,
        run=run,
        batch=batch,
        freshness=freshness,
        source_snapshots=source_snapshots,
        operator_actions=operator_actions,
        parcel_evidence=parcel_evidence,
    )


def _build_source_snapshot_records(
    session: Session,
    freshness: MetroFreshnessReport,
) -> list[AuditSourceSnapshotRecord]:
    source_health = {
        snapshot.source_id: snapshot
        for snapshot in summarize_source_health(session, metro_id=freshness.metro_id)
    }
    records: list[AuditSourceSnapshotRecord] = []

    for status in freshness.statuses:
        snapshot = None
        if status.latest_snapshot_id is not None:
            snapshot = session.get(SourceSnapshot, UUID(status.latest_snapshot_id))

        health_snapshot = source_health.get(status.source_id)
        records.append(
            AuditSourceSnapshotRecord(
                source_id=status.source_id,
                metro_id=status.metro_id,
                required=status.required,
                passed=status.passed,
                freshness_code=status.freshness_code,
                freshness_reason=status.freshness_reason,
                refresh_cadence=status.refresh_cadence,
                max_age_hours=status.max_age_hours,
                latest_snapshot_id=status.latest_snapshot_id,
                latest_snapshot_ts=status.latest_snapshot_ts,
                latest_snapshot_status=status.latest_snapshot_status,
                age_hours=status.age_hours,
                source_version=snapshot.source_version if snapshot is not None else None,
                checksum=snapshot.checksum if snapshot is not None else None,
                row_count=health_snapshot.row_count if health_snapshot is not None else 0,
                accepted_count=health_snapshot.accepted_count if health_snapshot is not None else 0,
                rejected_count=health_snapshot.rejected_count if health_snapshot is not None else 0,
                error_message=(
                    health_snapshot.error_message if health_snapshot is not None else None
                ),
            )
        )

    return records


def _build_parcel_evidence_package(
    session: Session,
    run_id: str,
    metro_id: str,
    parcel_id: str,
) -> AuditParcelEvidencePackage:
    parcel_detail = get_parcel_scoring_detail(session, run_id, parcel_id)
    parcel = session.get(RawParcel, parcel_id)
    rep_point = session.get(ParcelRepPoint, parcel_id)
    zoning = session.scalar(
        select(RawZoning).where(
            RawZoning.parcel_id == parcel_id,
            RawZoning.is_active.is_(True),
        )
    )

    if parcel is None:
        raise LookupError(f"Parcel `{parcel_id}` was not found in canonical parcel storage.")

    evidence_rows = session.scalars(
        select(SourceEvidence)
        .where(
            SourceEvidence.metro_id == metro_id,
            SourceEvidence.parcel_id == parcel_id,
            SourceEvidence.is_active.is_(True),
        )
        .order_by(
            SourceEvidence.source_id,
            SourceEvidence.attribute_name,
            SourceEvidence.record_key,
        )
    ).all()

    return AuditParcelEvidencePackage(
        parcel_context=AuditParcelContext(
            parcel_id=parcel.parcel_id,
            county_fips=parcel.county_fips,
            metro_id=parcel.metro_id,
            apn=parcel.apn,
            acreage=parcel.acreage,
            geometry_wkt=parcel.geometry_wkt,
            rep_point_wkt=rep_point.rep_point_wkt if rep_point is not None else None,
            geometry_method=rep_point.geometry_method if rep_point is not None else None,
            parcel_source_snapshot_id=(
                str(parcel.source_snapshot_id) if parcel.source_snapshot_id is not None else None
            ),
            parcel_lineage_key=parcel.lineage_key,
            zoning_code=zoning.zoning_code if zoning is not None else None,
            land_use_code=zoning.land_use_code if zoning is not None else None,
            zoning_source_snapshot_id=(
                str(zoning.source_snapshot_id) if zoning is not None else None
            ),
            zoning_lineage_key=zoning.lineage_key if zoning is not None else None,
        ),
        parcel_detail=parcel_detail,
        source_evidence=[
            AuditSourceEvidenceRecord(
                source_id=evidence.source_id,
                source_snapshot_id=(
                    str(evidence.source_snapshot_id)
                    if evidence.source_snapshot_id is not None
                    else None
                ),
                record_key=evidence.record_key,
                attribute_name=evidence.attribute_name,
                attribute_value=evidence.attribute_value,
                lineage_key=evidence.lineage_key,
                county_fips=evidence.county_fips,
                parcel_id=evidence.parcel_id,
                created_at=evidence.created_at,
            )
            for evidence in evidence_rows
        ],
    )
