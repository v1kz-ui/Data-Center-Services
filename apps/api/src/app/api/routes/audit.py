from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.core.security import CurrentPrincipal, require_admin_access
from app.db.session import get_db
from app.schemas.audit import AuditPackageResponse
from app.services.audit import build_run_audit_package
from orchestrator.service import BatchNotFoundError, RunNotFoundError
from scoring.service import ScoringParcelNotFoundError

router = APIRouter(dependencies=[Depends(require_admin_access)])
DbSession = Annotated[Session, Depends(get_db)]


@router.get(
    "/admin/audit/packages/runs/{run_id}",
    response_model=AuditPackageResponse,
)
def export_run_audit_package(
    run_id: UUID,
    db: DbSession,
    principal: CurrentPrincipal,
    parcel_id: Annotated[str | None, Query()] = None,
) -> AuditPackageResponse:
    try:
        package = build_run_audit_package(
            db,
            str(run_id),
            exported_by=principal.display_name or principal.subject,
            parcel_id=parcel_id,
        )
    except (BatchNotFoundError, RunNotFoundError, ScoringParcelNotFoundError, LookupError) as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return AuditPackageResponse.model_validate(package)
