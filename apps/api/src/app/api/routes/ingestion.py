from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.core.security import require_operator_access
from app.db.session import get_db
from app.schemas.ingestion import (
    AdminSourceLoadRequest,
    MetroFreshnessReportResponse,
    SourceHealthSnapshotResponse,
    SourceLoadReportResponse,
)
from ingestion.service import (
    SourceConfigurationError,
    UnsupportedSourceError,
    evaluate_freshness,
    ingest_evidence_records,
    ingest_parcel_records,
    ingest_zoning_records,
    summarize_source_health,
)

router = APIRouter(dependencies=[Depends(require_operator_access)])
DbSession = Annotated[Session, Depends(get_db)]


@router.post(
    "/admin/sources/{source_id}/metros/{metro_id}/loads",
    response_model=SourceLoadReportResponse,
    status_code=status.HTTP_201_CREATED,
)
def load_source_records(
    source_id: str,
    metro_id: str,
    request: AdminSourceLoadRequest,
    db: DbSession,
) -> SourceLoadReportResponse:
    normalized_source_id = source_id.strip().upper()

    try:
        if normalized_source_id == "PARCEL":
            report = ingest_parcel_records(
                session=db,
                metro_id=metro_id,
                source_version=request.source_version,
                records=request.records,
                loaded_at=request.snapshot_ts,
            )
        elif normalized_source_id == "ZONING":
            report = ingest_zoning_records(
                session=db,
                metro_id=metro_id,
                source_version=request.source_version,
                records=request.records,
                loaded_at=request.snapshot_ts,
            )
        else:
            report = ingest_evidence_records(
                session=db,
                source_id=normalized_source_id,
                metro_id=metro_id,
                source_version=request.source_version,
                records=request.records,
                loaded_at=request.snapshot_ts,
            )
    except (SourceConfigurationError, UnsupportedSourceError) as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc

    return SourceLoadReportResponse.model_validate(report)


@router.get(
    "/admin/sources/freshness/{metro_id}",
    response_model=MetroFreshnessReportResponse,
)
def get_source_freshness(
    metro_id: str,
    db: DbSession,
) -> MetroFreshnessReportResponse:
    report = evaluate_freshness(db, metro_id)
    return MetroFreshnessReportResponse.model_validate(report)


@router.get(
    "/admin/sources/health",
    response_model=list[SourceHealthSnapshotResponse],
)
def get_source_health(
    db: DbSession,
    metro_id: Annotated[str | None, Query()] = None,
) -> list[SourceHealthSnapshotResponse]:
    snapshots = summarize_source_health(db, metro_id=metro_id)
    return [SourceHealthSnapshotResponse.model_validate(snapshot) for snapshot in snapshots]
