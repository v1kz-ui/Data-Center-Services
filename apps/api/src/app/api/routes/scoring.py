from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.core.security import require_operator_access
from app.db.session import get_db
from app.schemas.scoring import (
    ParcelScoringDetailResponse,
    ScoringRunRequest,
    ScoringSummaryResponse,
)
from scoring.models import EvidenceQualityWeights, ScoringPolicy
from scoring.service import (
    ScoringInvariantError,
    ScoringParcelNotFoundError,
    ScoringProfileValidationError,
    ScoringReplayBlockedError,
    ScoringRunNotFoundError,
    get_parcel_scoring_detail,
    get_scoring_summary,
    score_run,
)

router = APIRouter(dependencies=[Depends(require_operator_access)])
DbSession = Annotated[Session, Depends(get_db)]


@router.get(
    "/admin/runs/{run_id}/scoring",
    response_model=ScoringSummaryResponse,
)
def get_run_scoring_status(
    run_id: UUID,
    db: DbSession,
) -> ScoringSummaryResponse:
    try:
        summary = get_scoring_summary(db, run_id)
    except ScoringRunNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return ScoringSummaryResponse.model_validate(summary)


@router.get(
    "/admin/runs/{run_id}/scoring/parcels/{parcel_id}",
    response_model=ParcelScoringDetailResponse,
)
def get_run_parcel_scoring_detail(
    run_id: UUID,
    parcel_id: str,
    db: DbSession,
) -> ParcelScoringDetailResponse:
    try:
        detail = get_parcel_scoring_detail(db, run_id, parcel_id)
    except (ScoringParcelNotFoundError, ScoringRunNotFoundError) as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return ParcelScoringDetailResponse.model_validate(detail)


@router.post(
    "/admin/runs/{run_id}/scoring",
    response_model=ScoringSummaryResponse,
)
def execute_run_scoring(
    run_id: UUID,
    request: ScoringRunRequest,
    db: DbSession,
) -> ScoringSummaryResponse:
    policy = ScoringPolicy(
        profile_name=request.profile_name,
        restart_failed_run=request.restart_failed_run,
        allow_completed_run_rerun=request.allow_completed_run_rerun,
        evidence_quality_weights=EvidenceQualityWeights(
            measured=request.evidence_quality_weights.measured,
            manual=request.evidence_quality_weights.manual,
            proxy=request.evidence_quality_weights.proxy,
            heuristic=request.evidence_quality_weights.heuristic,
            missing=request.evidence_quality_weights.missing,
        ),
    )

    try:
        summary = score_run(db, run_id, policy)
    except ScoringRunNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except (
        ScoringInvariantError,
        ScoringProfileValidationError,
        ScoringReplayBlockedError,
        ValueError,
    ) as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=str(exc),
        ) from exc

    return ScoringSummaryResponse.model_validate(summary)
