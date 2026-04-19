import json
from collections import Counter
from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from app.db.models.batching import ScoreBatch, ScoreRun
from app.db.models.enums import ParcelEvaluationStatus, ScoreBatchStatus, ScoreRunStatus
from app.db.models.evaluation import ParcelEvaluation
from app.db.models.operations import OperatorActionEvent
from app.db.models.scoring import ScoreBonusDetail, ScoreFactorDetail
from orchestrator.models import (
    ActivationCheck,
    ActivationCheckIssue,
    BatchRunCounts,
    OperatorActionRecord,
    PlannedBatch,
    PlannedRun,
)

_FACTOR_COUNT = 10
_BONUS_COUNT = 5


class BatchNotFoundError(LookupError):
    """Raised when a requested orchestration batch does not exist."""


class RunNotFoundError(LookupError):
    """Raised when a requested orchestration run does not exist."""


class RunRetryNotAllowedError(RuntimeError):
    """Raised when an operator retry request is not allowed."""


class RunCancelNotAllowedError(RuntimeError):
    """Raised when an operator cancel request is not allowed."""


class BatchRerunNotAllowedError(RuntimeError):
    """Raised when a batch cannot be rerun."""


def describe_service() -> dict[str, str]:
    return {
        "service": "orchestrator",
        "purpose": "Manage score batches, runs, and activation flow.",
    }


def normalize_metro_ids(metro_ids: list[str]) -> list[str]:
    unique_metros = list(dict.fromkeys(_canonicalize_metro_id(metro_id) for metro_id in metro_ids))
    return [metro_id for metro_id in unique_metros if metro_id]


def build_batch_plan(metro_ids: list[str]) -> PlannedBatch:
    unique_metros = normalize_metro_ids(metro_ids)
    if not unique_metros:
        raise ValueError("At least one metro_id is required to build a batch plan.")

    batch_id = str(uuid4())
    runs = [
        PlannedRun(
            run_id=str(uuid4()),
            batch_id=batch_id,
            metro_id=metro_id,
            profile_name=None,
            status="running",
            failure_reason=None,
            started_at=None,
            completed_at=None,
        )
        for metro_id in unique_metros
    ]

    return PlannedBatch(
        batch_id=batch_id,
        status="building",
        expected_metros=len(unique_metros),
        completed_metros=0,
        activated_at=None,
        activation_ready=False,
        run_counts=BatchRunCounts(running=len(unique_metros), failed=0, completed=0),
        runs=runs,
    )


def create_batch(session: Session, metro_ids: list[str]) -> PlannedBatch:
    planned_batch = build_batch_plan(metro_ids)
    now = datetime.now(UTC)

    batch = ScoreBatch(
        status=ScoreBatchStatus.BUILDING,
        expected_metros=planned_batch.expected_metros,
        completed_metros=planned_batch.completed_metros,
    )
    session.add(batch)
    session.flush()

    runs = [
        ScoreRun(
            batch_id=batch.batch_id,
            metro_id=run.metro_id,
            status=ScoreRunStatus.RUNNING,
            started_at=now,
        )
        for run in planned_batch.runs
    ]
    session.add_all(runs)
    session.commit()

    return _to_planned_batch(batch, runs)


def list_batches(session: Session) -> list[PlannedBatch]:
    batches = session.scalars(
        select(ScoreBatch)
        .options(selectinload(ScoreBatch.runs))
        .order_by(ScoreBatch.created_at.desc(), ScoreBatch.batch_id.desc())
    ).all()
    return [
        _to_planned_batch(batch, sorted(batch.runs, key=lambda run: run.metro_id))
        for batch in batches
    ]


def get_batch(session: Session, batch_id: str | UUID) -> PlannedBatch:
    batch = _get_batch_model(session, batch_id)

    runs = sorted(batch.runs, key=lambda run: run.metro_id)
    return _to_planned_batch(batch, runs)


def reconcile_batch(session: Session, batch_id: str | UUID) -> PlannedBatch:
    batch = _get_batch_model(session, batch_id)
    runs = sorted(batch.runs, key=lambda run: run.metro_id)
    _reconcile_batch_model(batch, runs)
    session.commit()
    return _to_planned_batch(batch, runs)


def reconcile_batch_for_run(session: Session, run_id: str | UUID) -> PlannedBatch:
    run = _get_run_model(session, run_id)
    batch = session.scalar(
        select(ScoreBatch)
        .options(selectinload(ScoreBatch.runs))
        .where(ScoreBatch.batch_id == run.batch_id)
    )
    if batch is None:
        raise BatchNotFoundError(f"Batch `{run.batch_id}` was not found.")

    runs = sorted(batch.runs, key=lambda item: item.metro_id)
    _reconcile_batch_model(batch, runs)
    session.flush()
    return _to_planned_batch(batch, runs)


def get_run(session: Session, run_id: str | UUID) -> PlannedRun:
    run = _get_run_model(session, run_id)
    return _to_planned_run(run)


def retry_run(
    session: Session,
    run_id: str | UUID,
    *,
    actor_name: str = "operator",
    action_reason: str | None = None,
) -> PlannedRun:
    run = _get_run_model(session, run_id)
    batch = _get_batch_model(session, run.batch_id)

    if run.status is not ScoreRunStatus.FAILED:
        raise RunRetryNotAllowedError(
            f"Run `{run.run_id}` is not failed and cannot be retried."
        )
    if batch.status is ScoreBatchStatus.ACTIVE:
        raise RunRetryNotAllowedError(
            f"Run `{run.run_id}` belongs to the active batch and cannot be retried in place."
        )

    previous_failure_reason = run.failure_reason
    run.status = ScoreRunStatus.RUNNING
    run.failure_reason = None
    run.profile_name = None
    run.started_at = datetime.now(UTC)
    run.completed_at = None

    runs = sorted(batch.runs, key=lambda item: item.metro_id)
    _reconcile_batch_model(batch, runs)
    _record_action_event(
        session,
        action_type="retry_run",
        target_type="run",
        target_id=str(run.run_id),
        batch_id=batch.batch_id,
        run_id=run.run_id,
        actor_name=actor_name,
        action_reason=action_reason,
        action_payload={"previous_failure_reason": previous_failure_reason},
    )
    session.commit()
    return _to_planned_run(run)


def cancel_run(
    session: Session,
    run_id: str | UUID,
    *,
    actor_name: str = "operator",
    action_reason: str | None = None,
) -> PlannedRun:
    run = _get_run_model(session, run_id)
    batch = _get_batch_model(session, run.batch_id)

    if run.status is not ScoreRunStatus.RUNNING:
        raise RunCancelNotAllowedError(
            f"Run `{run.run_id}` is not running and cannot be cancelled."
        )
    if batch.status is ScoreBatchStatus.ACTIVE:
        raise RunCancelNotAllowedError(
            f"Run `{run.run_id}` belongs to the active batch and cannot be cancelled."
        )

    run.status = ScoreRunStatus.FAILED
    run.failure_reason = "MANUAL_CANCELLED"
    run.completed_at = datetime.now(UTC)

    runs = sorted(batch.runs, key=lambda item: item.metro_id)
    _reconcile_batch_model(batch, runs)
    _record_action_event(
        session,
        action_type="cancel_run",
        target_type="run",
        target_id=str(run.run_id),
        batch_id=batch.batch_id,
        run_id=run.run_id,
        actor_name=actor_name,
        action_reason=action_reason or "Operator cancelled the run.",
        action_payload={"failure_reason": run.failure_reason},
    )
    session.commit()
    return _to_planned_run(run)


def rerun_batch(
    session: Session,
    batch_id: str | UUID,
    *,
    actor_name: str = "operator",
    action_reason: str | None = None,
) -> PlannedBatch:
    batch = _get_batch_model(session, batch_id)
    runs = sorted(batch.runs, key=lambda run: run.metro_id)

    if batch.status is ScoreBatchStatus.BUILDING:
        raise BatchRerunNotAllowedError(
            f"Batch `{batch.batch_id}` is still building and cannot be rerun."
        )

    replacement_batch = create_batch(session, [run.metro_id for run in runs])
    _record_action_event(
        session,
        action_type="rerun_batch",
        target_type="batch",
        target_id=str(batch.batch_id),
        batch_id=batch.batch_id,
        run_id=None,
        actor_name=actor_name,
        action_reason=action_reason,
        action_payload={"replacement_batch_id": replacement_batch.batch_id},
    )
    session.commit()
    return replacement_batch


def list_operator_actions(
    session: Session,
    *,
    batch_id: str | UUID | None = None,
    run_id: str | UUID | None = None,
    limit: int = 20,
) -> list[OperatorActionRecord]:
    statement = select(OperatorActionEvent).order_by(
        OperatorActionEvent.created_at.desc(),
        OperatorActionEvent.action_event_id.desc(),
    )
    if batch_id is not None:
        typed_batch_id = batch_id if isinstance(batch_id, UUID) else UUID(str(batch_id))
        statement = statement.where(OperatorActionEvent.batch_id == typed_batch_id)
    if run_id is not None:
        typed_run_id = run_id if isinstance(run_id, UUID) else UUID(str(run_id))
        statement = statement.where(OperatorActionEvent.run_id == typed_run_id)

    events = session.scalars(statement.limit(limit)).all()
    return [_to_operator_action_record(event) for event in events]


def get_activation_check(session: Session, batch_id: str | UUID) -> ActivationCheck:
    batch = _get_batch_model(session, batch_id)
    runs = sorted(batch.runs, key=lambda run: run.metro_id)
    run_counts = _build_run_counts(runs)
    issues: list[ActivationCheckIssue] = []
    checked_at = datetime.now(UTC)

    if batch.status is not ScoreBatchStatus.COMPLETED:
        issues.append(
            ActivationCheckIssue(
                code="BATCH_STATUS_NOT_COMPLETED",
                detail="Batch must be completed before activation is allowed.",
                run_id=None,
                metro_id=None,
            )
        )

    if len(runs) != batch.expected_metros:
        issues.append(
            ActivationCheckIssue(
                code="RUN_COUNT_MISMATCH",
                detail=(
                    "Batch expects "
                    f"{batch.expected_metros} metro runs but has {len(runs)} "
                    "persisted runs."
                ),
                run_id=None,
                metro_id=None,
            )
        )

    if batch.completed_metros != batch.expected_metros:
        issues.append(
            ActivationCheckIssue(
                code="COMPLETION_COUNT_MISMATCH",
                detail=(
                    "Batch completed_metros is "
                    f"{batch.completed_metros} but expected "
                    f"{batch.expected_metros}."
                ),
                run_id=None,
                metro_id=None,
            )
        )

    for run in runs:
        if run.status is not ScoreRunStatus.COMPLETED:
            issues.append(
                ActivationCheckIssue(
                    code="RUN_NOT_COMPLETED",
                    detail="All metro runs must be completed before activation.",
                    run_id=str(run.run_id),
                    metro_id=run.metro_id,
                )
            )
        if run.failure_reason:
            issues.append(
                ActivationCheckIssue(
                    code="RUN_HAS_FAILURE_REASON",
                    detail=f"Run still carries failure reason `{run.failure_reason}`.",
                    run_id=str(run.run_id),
                    metro_id=run.metro_id,
                )
            )

        pending_count = _count_pending_evaluations(session, run.run_id)
        if pending_count:
            issues.append(
                ActivationCheckIssue(
                    code="PENDING_PARCELS_REMAIN",
                    detail=f"Run still has {pending_count} pending parcel evaluations.",
                    run_id=str(run.run_id),
                    metro_id=run.metro_id,
                )
            )

        scored_count = _count_scored_evaluations(session, run.run_id)
        factor_count = _count_factor_rows(session, run.run_id)
        bonus_count = _count_bonus_rows(session, run.run_id)
        expected_factor_count = scored_count * _FACTOR_COUNT
        expected_bonus_count = scored_count * _BONUS_COUNT

        if factor_count != expected_factor_count:
            issues.append(
                ActivationCheckIssue(
                    code="FACTOR_CARDINALITY_MISMATCH",
                    detail=(
                        f"Run has {factor_count} factor rows but expected {expected_factor_count} "
                        f"for {scored_count} scored parcels."
                    ),
                    run_id=str(run.run_id),
                    metro_id=run.metro_id,
                )
            )

        if bonus_count != expected_bonus_count:
            issues.append(
                ActivationCheckIssue(
                    code="BONUS_CARDINALITY_MISMATCH",
                    detail=(
                        f"Run has {bonus_count} bonus rows but expected {expected_bonus_count} "
                        f"for {scored_count} scored parcels."
                    ),
                    run_id=str(run.run_id),
                    metro_id=run.metro_id,
                )
            )

    return ActivationCheck(
        batch_id=str(batch.batch_id),
        status=batch.status.value,
        expected_metros=batch.expected_metros,
        completed_metros=batch.completed_metros,
        checked_at=checked_at,
        activation_ready=not issues,
        issue_count=len(issues),
        run_counts=run_counts,
        issues=issues,
    )


def _canonicalize_metro_id(metro_id: str) -> str:
    return metro_id.strip().upper()


def _get_batch_model(session: Session, batch_id: str | UUID) -> ScoreBatch:
    typed_batch_id = batch_id if isinstance(batch_id, UUID) else UUID(str(batch_id))
    statement = (
        select(ScoreBatch)
        .options(selectinload(ScoreBatch.runs))
        .where(ScoreBatch.batch_id == typed_batch_id)
    )
    batch = session.scalar(statement)
    if batch is None:
        raise BatchNotFoundError(f"Batch `{typed_batch_id}` was not found.")
    return batch


def _get_run_model(session: Session, run_id: str | UUID) -> ScoreRun:
    typed_run_id = run_id if isinstance(run_id, UUID) else UUID(str(run_id))
    run = session.get(ScoreRun, typed_run_id)
    if run is None:
        raise RunNotFoundError(f"Run `{typed_run_id}` was not found.")
    return run


def _reconcile_batch_model(batch: ScoreBatch, runs: list[ScoreRun]) -> None:
    run_counts = _build_run_counts(runs)
    completed_metros = run_counts.completed
    failed_runs = run_counts.failed

    batch.completed_metros = completed_metros

    if batch.status is ScoreBatchStatus.ACTIVE:
        return

    if failed_runs:
        batch.status = ScoreBatchStatus.FAILED
        batch.activated_at = None
        return

    if runs and completed_metros == batch.expected_metros == len(runs):
        batch.status = ScoreBatchStatus.COMPLETED
        batch.activated_at = None
        return

    batch.status = ScoreBatchStatus.BUILDING
    batch.activated_at = None


def _to_planned_batch(batch: ScoreBatch, runs: list[ScoreRun]) -> PlannedBatch:
    run_counts = _build_run_counts(runs)
    return PlannedBatch(
        batch_id=str(batch.batch_id),
        status=batch.status.value,
        expected_metros=batch.expected_metros,
        completed_metros=batch.completed_metros,
        activated_at=batch.activated_at,
        activation_ready=(
            batch.expected_metros > 0
            and run_counts.failed == 0
            and run_counts.completed == batch.expected_metros
        ),
        run_counts=run_counts,
        runs=[_to_planned_run(run) for run in runs],
    )


def _to_planned_run(run: ScoreRun) -> PlannedRun:
    return PlannedRun(
        run_id=str(run.run_id),
        batch_id=str(run.batch_id),
        metro_id=run.metro_id,
        profile_name=run.profile_name,
        status=run.status.value,
        failure_reason=run.failure_reason,
        started_at=run.started_at,
        completed_at=run.completed_at,
    )


def _record_action_event(
    session: Session,
    *,
    action_type: str,
    target_type: str,
    target_id: str,
    batch_id: UUID | None,
    run_id: UUID | None,
    actor_name: str,
    action_reason: str | None,
    action_payload: dict[str, object] | None,
) -> None:
    session.add(
        OperatorActionEvent(
            action_type=action_type,
            target_type=target_type,
            target_id=target_id,
            batch_id=batch_id,
            run_id=run_id,
            actor_name=actor_name,
            action_reason=action_reason,
            action_payload=(
                json.dumps(action_payload, sort_keys=True) if action_payload is not None else None
            ),
        )
    )


def _to_operator_action_record(event: OperatorActionEvent) -> OperatorActionRecord:
    return OperatorActionRecord(
        action_event_id=str(event.action_event_id),
        action_type=event.action_type,
        target_type=event.target_type,
        target_id=event.target_id,
        batch_id=str(event.batch_id) if event.batch_id is not None else None,
        run_id=str(event.run_id) if event.run_id is not None else None,
        actor_name=event.actor_name,
        action_reason=event.action_reason,
        action_payload=json.loads(event.action_payload) if event.action_payload else None,
        created_at=event.created_at,
    )


def _build_run_counts(runs: list[ScoreRun]) -> BatchRunCounts:
    status_counts = Counter(run.status.value for run in runs)
    return BatchRunCounts(
        running=status_counts.get(ScoreRunStatus.RUNNING.value, 0),
        failed=status_counts.get(ScoreRunStatus.FAILED.value, 0),
        completed=status_counts.get(ScoreRunStatus.COMPLETED.value, 0),
    )


def _count_pending_evaluations(session: Session, run_id: UUID) -> int:
    count = session.scalar(
        select(func.count())
        .select_from(ParcelEvaluation)
        .where(
            ParcelEvaluation.run_id == run_id,
            ParcelEvaluation.status.in_(
                (
                    ParcelEvaluationStatus.PENDING_EXCLUSION_CHECK,
                    ParcelEvaluationStatus.PENDING_SCORING,
                )
            ),
        )
    )
    return int(count or 0)


def _count_scored_evaluations(session: Session, run_id: UUID) -> int:
    count = session.scalar(
        select(func.count())
        .select_from(ParcelEvaluation)
        .where(
            ParcelEvaluation.run_id == run_id,
            ParcelEvaluation.status == ParcelEvaluationStatus.SCORED,
        )
    )
    return int(count or 0)


def _count_factor_rows(session: Session, run_id: UUID) -> int:
    count = session.scalar(
        select(func.count())
        .select_from(ScoreFactorDetail)
        .where(ScoreFactorDetail.run_id == run_id)
    )
    return int(count or 0)


def _count_bonus_rows(session: Session, run_id: UUID) -> int:
    count = session.scalar(
        select(func.count())
        .select_from(ScoreBonusDetail)
        .where(ScoreBonusDetail.run_id == run_id)
    )
    return int(count or 0)
