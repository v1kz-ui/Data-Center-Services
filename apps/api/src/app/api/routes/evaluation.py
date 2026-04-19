from decimal import Decimal
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.core.security import require_operator_access
from app.db.session import get_db
from app.schemas.evaluation import (
    EvaluationRunRequest,
    EvaluationRunScopeResponse,
    EvaluationSummaryResponse,
)
from evaluation.models import EvaluationPolicy, EvidenceExclusionRule
from evaluation.service import (
    EvaluationReplayBlockedError,
    EvaluationRunNotFoundError,
    describe_run_scope,
    evaluate_run,
    get_evaluation_summary,
)

router = APIRouter(dependencies=[Depends(require_operator_access)])
DbSession = Annotated[Session, Depends(get_db)]


@router.get(
    "/admin/runs/{run_id}/evaluation/scope",
    response_model=EvaluationRunScopeResponse,
)
def get_run_evaluation_scope(
    run_id: UUID,
    db: DbSession,
) -> EvaluationRunScopeResponse:
    try:
        scope = describe_run_scope(db, run_id)
    except EvaluationRunNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return EvaluationRunScopeResponse.model_validate(scope)


@router.get(
    "/admin/runs/{run_id}/evaluation",
    response_model=EvaluationSummaryResponse,
)
def get_run_evaluation_status(
    run_id: UUID,
    db: DbSession,
) -> EvaluationSummaryResponse:
    try:
        summary = get_evaluation_summary(db, run_id)
    except EvaluationRunNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return EvaluationSummaryResponse.model_validate(summary)


@router.post(
    "/admin/runs/{run_id}/evaluation",
    response_model=EvaluationSummaryResponse,
)
def execute_run_evaluation(
    run_id: UUID,
    request: EvaluationRunRequest,
    db: DbSession,
) -> EvaluationSummaryResponse:
    policy = EvaluationPolicy(
        rule_version=request.rule_version,
        allowed_band_wkt=request.allowed_band_wkt,
        minimum_acreage=Decimal(str(request.minimum_acreage)),
        blocked_zoning_codes=tuple(request.blocked_zoning_codes),
        blocked_land_use_codes=tuple(request.blocked_land_use_codes),
        evidence_exclusion_rules=tuple(
            EvidenceExclusionRule(
                source_id=rule.source_id,
                attribute_name=rule.attribute_name,
                blocked_values=tuple(rule.blocked_values),
                exclusion_code=rule.exclusion_code,
                exclusion_reason=rule.exclusion_reason,
            )
            for rule in request.evidence_exclusion_rules
        ),
        restart_failed_run=request.restart_failed_run,
    )

    try:
        summary = evaluate_run(db, run_id, policy)
    except EvaluationRunNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except (EvaluationReplayBlockedError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=str(exc),
        ) from exc

    return EvaluationSummaryResponse.model_validate(summary)
