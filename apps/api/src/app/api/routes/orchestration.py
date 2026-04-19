from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.core.security import CurrentPrincipal, require_operator_access
from app.db.session import get_db
from app.schemas.orchestration import (
    ActivationCheckResponse,
    BatchPlanRequest,
    BatchPlanResponse,
    BatchRerunResponse,
    OperatorActionRecordResponse,
    OperatorActionRequest,
    PlannedRunResponse,
    RunRetryResponse,
)
from orchestrator.service import (
    BatchNotFoundError,
    BatchRerunNotAllowedError,
    RunCancelNotAllowedError,
    RunNotFoundError,
    RunRetryNotAllowedError,
    build_batch_plan,
    cancel_run,
    create_batch,
    get_activation_check,
    get_batch,
    get_run,
    list_batches,
    list_operator_actions,
    reconcile_batch,
    rerun_batch,
    retry_run,
)

router = APIRouter(dependencies=[Depends(require_operator_access)])
DbSession = Annotated[Session, Depends(get_db)]


@router.post("/orchestration/plan", response_model=BatchPlanResponse)
def orchestration_plan(request: BatchPlanRequest) -> BatchPlanResponse:
    planned_batch = build_batch_plan(request.metro_ids)
    return BatchPlanResponse.model_validate(planned_batch)


@router.post(
    "/orchestration/batches",
    response_model=BatchPlanResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_orchestration_batch(
    request: BatchPlanRequest,
    db: DbSession,
) -> BatchPlanResponse:
    try:
        batch = create_batch(db, request.metro_ids)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc

    return BatchPlanResponse.model_validate(batch)


@router.get("/orchestration/batches", response_model=list[BatchPlanResponse])
def list_orchestration_batches(db: DbSession) -> list[BatchPlanResponse]:
    return [BatchPlanResponse.model_validate(batch) for batch in list_batches(db)]


@router.get("/orchestration/batches/{batch_id}", response_model=BatchPlanResponse)
def get_orchestration_batch(
    batch_id: UUID,
    db: DbSession,
) -> BatchPlanResponse:
    try:
        batch = get_batch(db, str(batch_id))
    except BatchNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return BatchPlanResponse.model_validate(batch)


@router.get(
    "/orchestration/batches/{batch_id}/activation-check",
    response_model=ActivationCheckResponse,
)
def get_orchestration_activation_check(
    batch_id: UUID,
    db: DbSession,
) -> ActivationCheckResponse:
    try:
        report = get_activation_check(db, str(batch_id))
    except BatchNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return ActivationCheckResponse.model_validate(report)


@router.post("/orchestration/batches/{batch_id}/reconcile", response_model=BatchPlanResponse)
def reconcile_orchestration_batch(
    batch_id: UUID,
    db: DbSession,
) -> BatchPlanResponse:
    try:
        batch = reconcile_batch(db, str(batch_id))
    except BatchNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return BatchPlanResponse.model_validate(batch)


@router.get("/orchestration/runs/{run_id}", response_model=PlannedRunResponse)
def get_orchestration_run(
    run_id: UUID,
    db: DbSession,
) -> PlannedRunResponse:
    try:
        run = get_run(db, str(run_id))
    except RunNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return PlannedRunResponse.model_validate(run)


@router.post("/orchestration/runs/{run_id}/retry", response_model=RunRetryResponse)
def retry_orchestration_run(
    run_id: UUID,
    db: DbSession,
    principal: CurrentPrincipal,
    action: Annotated[OperatorActionRequest | None, Body()] = None,
) -> RunRetryResponse:
    operator_action = action or OperatorActionRequest()
    try:
        run = retry_run(
            db,
            str(run_id),
            actor_name=(
                operator_action.actor_name
                or principal.display_name
                or principal.subject
            ),
            action_reason=operator_action.action_reason,
        )
        batch = get_batch(db, run.batch_id)
    except RunNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except RunRetryNotAllowedError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc
    except BatchNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return RunRetryResponse(
        run=PlannedRunResponse.model_validate(run),
        batch=BatchPlanResponse.model_validate(batch),
    )


@router.post("/orchestration/runs/{run_id}/cancel", response_model=RunRetryResponse)
def cancel_orchestration_run(
    run_id: UUID,
    db: DbSession,
    principal: CurrentPrincipal,
    action: Annotated[OperatorActionRequest | None, Body()] = None,
) -> RunRetryResponse:
    operator_action = action or OperatorActionRequest()
    try:
        run = cancel_run(
            db,
            str(run_id),
            actor_name=(
                operator_action.actor_name
                or principal.display_name
                or principal.subject
            ),
            action_reason=operator_action.action_reason,
        )
        batch = get_batch(db, run.batch_id)
    except RunNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except RunCancelNotAllowedError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc
    except BatchNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return RunRetryResponse(
        run=PlannedRunResponse.model_validate(run),
        batch=BatchPlanResponse.model_validate(batch),
    )


@router.post(
    "/orchestration/batches/{batch_id}/rerun",
    response_model=BatchRerunResponse,
    status_code=status.HTTP_201_CREATED,
)
def rerun_orchestration_batch(
    batch_id: UUID,
    db: DbSession,
    principal: CurrentPrincipal,
    action: Annotated[OperatorActionRequest | None, Body()] = None,
) -> BatchRerunResponse:
    operator_action = action or OperatorActionRequest()
    try:
        replacement_batch = rerun_batch(
            db,
            str(batch_id),
            actor_name=(
                operator_action.actor_name
                or principal.display_name
                or principal.subject
            ),
            action_reason=operator_action.action_reason,
        )
    except BatchNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except BatchRerunNotAllowedError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc

    return BatchRerunResponse(
        source_batch_id=str(batch_id),
        replacement_batch=BatchPlanResponse.model_validate(replacement_batch),
    )


@router.get(
    "/orchestration/actions",
    response_model=list[OperatorActionRecordResponse],
)
def list_orchestration_actions(
    db: DbSession,
    batch_id: UUID | None = None,
    run_id: UUID | None = None,
    limit: int = Query(default=20, ge=1, le=100),
) -> list[OperatorActionRecordResponse]:
    actions = list_operator_actions(
        db,
        batch_id=batch_id,
        run_id=run_id,
        limit=limit,
    )
    return [OperatorActionRecordResponse.model_validate(action) for action in actions]
