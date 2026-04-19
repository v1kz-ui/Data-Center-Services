from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.core.security import require_operator_access
from app.core.settings import Settings, get_settings
from app.db.session import get_db
from app.schemas.monitoring import MonitoringOverviewResponse
from app.services.monitoring import MonitoringThresholdPolicy, build_monitoring_overview

router = APIRouter(dependencies=[Depends(require_operator_access)])
DbSession = Annotated[Session, Depends(get_db)]
AppSettings = Annotated[Settings, Depends(get_settings)]


@router.get("/admin/monitoring/overview", response_model=MonitoringOverviewResponse)
def get_monitoring_overview(
    db: DbSession,
    settings: AppSettings,
    metro_id: Annotated[str | None, Query()] = None,
    recent_failed_limit: Annotated[int, Query(ge=1, le=20)] = 5,
) -> MonitoringOverviewResponse:
    overview = build_monitoring_overview(
        db,
        metro_id=metro_id,
        recent_failed_limit=recent_failed_limit,
        threshold_policy=MonitoringThresholdPolicy(
            failed_run_threshold=settings.monitoring_failed_run_threshold,
            failed_snapshot_threshold=settings.monitoring_failed_snapshot_threshold,
            quarantined_snapshot_threshold=settings.monitoring_quarantined_snapshot_threshold,
            freshness_failure_threshold=settings.monitoring_freshness_failure_threshold,
            latest_batch_failed_threshold=settings.monitoring_latest_batch_failed_threshold,
        ),
    )
    return MonitoringOverviewResponse.model_validate(overview)
