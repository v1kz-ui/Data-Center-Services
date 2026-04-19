import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy.orm import Session

from app.db.models.batching import ScoreBatch, ScoreRun
from app.db.models.catalogs import BonusCatalog, FactorCatalog
from app.db.models.enums import ParcelEvaluationStatus, ScoreBatchStatus, ScoreRunStatus
from app.db.models.evaluation import ParcelEvaluation
from app.db.models.operations import OperatorActionEvent
from app.db.models.scoring import ScoreBonusDetail, ScoreFactorDetail
from app.db.models.territory import CountyCatalog, MetroCatalog, RawParcel
from orchestrator.service import (
    BatchRerunNotAllowedError,
    RunCancelNotAllowedError,
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


def test_build_batch_plan_deduplicates_metros() -> None:
    plan = build_batch_plan(["dfw", "AUS", " DFW "])
    assert plan.status == "building"
    assert plan.expected_metros == 2
    assert plan.run_counts.running == 2
    assert plan.activation_ready is False
    assert len(plan.runs) == 2
    assert [run.metro_id for run in plan.runs] == ["DFW", "AUS"]


def test_create_batch_persists_runs(db_session: Session) -> None:
    created_batch = create_batch(db_session, ["dfw", "aus", "DFW"])

    assert created_batch.expected_metros == 2
    assert created_batch.run_counts.running == 2
    assert [run.metro_id for run in created_batch.runs] == ["DFW", "AUS"]
    assert all(run.status == "running" for run in created_batch.runs)
    assert all(run.started_at is not None for run in created_batch.runs)


def test_get_batch_returns_persisted_record(db_session: Session) -> None:
    created_batch = create_batch(db_session, ["PHX", "LAS"])

    loaded_batch = get_batch(db_session, created_batch.batch_id)

    assert loaded_batch.batch_id == created_batch.batch_id
    assert loaded_batch.expected_metros == 2
    assert sorted(run.metro_id for run in loaded_batch.runs) == ["LAS", "PHX"]


def test_reconcile_batch_marks_completed_when_all_runs_complete(db_session: Session) -> None:
    created_batch = create_batch(db_session, ["PHX", "LAS"])

    runs = (
        db_session.query(ScoreRun)
        .filter(ScoreRun.batch_id == UUID(created_batch.batch_id))
        .all()
    )
    completed_at = datetime.now(UTC)
    for run in runs:
        run.status = ScoreRunStatus.COMPLETED
        run.completed_at = completed_at
    db_session.commit()

    reconciled_batch = reconcile_batch(db_session, created_batch.batch_id)

    assert reconciled_batch.status == ScoreBatchStatus.COMPLETED.value
    assert reconciled_batch.completed_metros == 2
    assert reconciled_batch.activation_ready is True
    assert reconciled_batch.run_counts.completed == 2


def test_reconcile_batch_marks_failed_when_any_run_fails(db_session: Session) -> None:
    created_batch = create_batch(db_session, ["DFW", "AUS"])

    runs = (
        db_session.query(ScoreRun)
        .filter(ScoreRun.batch_id == UUID(created_batch.batch_id))
        .order_by(ScoreRun.metro_id)
        .all()
    )
    runs[0].status = ScoreRunStatus.COMPLETED
    runs[0].completed_at = datetime.now(UTC)
    runs[1].status = ScoreRunStatus.FAILED
    runs[1].failure_reason = "MISSING_SOURCE"
    runs[1].completed_at = datetime.now(UTC)
    db_session.commit()

    reconciled_batch = reconcile_batch(db_session, created_batch.batch_id)

    assert reconciled_batch.status == ScoreBatchStatus.FAILED.value
    assert reconciled_batch.completed_metros == 1
    assert reconciled_batch.activation_ready is False
    assert reconciled_batch.run_counts.failed == 1


def test_list_batches_returns_most_recent_first(db_session: Session) -> None:
    first_batch = create_batch(db_session, ["DFW"])
    stored_first_batch = db_session.get(ScoreBatch, UUID(first_batch.batch_id))
    assert stored_first_batch is not None
    stored_first_batch.created_at = datetime(2026, 4, 1, tzinfo=UTC)
    db_session.commit()

    second_batch = create_batch(db_session, ["AUS"])

    batches = list_batches(db_session)

    assert [batch.batch_id for batch in batches[:2]] == [
        second_batch.batch_id,
        first_batch.batch_id,
    ]


def test_get_run_returns_operator_status_fields(db_session: Session) -> None:
    created_batch = create_batch(db_session, ["DFW"])
    run_id = created_batch.runs[0].run_id

    run = get_run(db_session, run_id)

    assert run.batch_id == created_batch.batch_id
    assert run.metro_id == "DFW"
    assert run.status == ScoreRunStatus.RUNNING.value
    assert run.started_at is not None


def test_retry_run_resets_failed_run_and_parent_batch(db_session: Session) -> None:
    created_batch = create_batch(db_session, ["DFW"])
    run_model = db_session.get(ScoreRun, UUID(created_batch.runs[0].run_id))

    assert run_model is not None
    run_model.status = ScoreRunStatus.FAILED
    run_model.failure_reason = "MISSING_SOURCE"
    run_model.completed_at = datetime(2026, 4, 10, tzinfo=UTC)
    db_session.commit()
    reconcile_batch(db_session, created_batch.batch_id)

    retried_run = retry_run(
        db_session,
        created_batch.runs[0].run_id,
        actor_name="ops.lead",
        action_reason="Retry after source refresh.",
    )
    batch = get_batch(db_session, created_batch.batch_id)
    action_event = db_session.query(OperatorActionEvent).one()

    assert retried_run.status == ScoreRunStatus.RUNNING.value
    assert retried_run.failure_reason is None
    assert retried_run.completed_at is None
    assert batch.status == ScoreBatchStatus.BUILDING.value
    assert batch.run_counts.running == 1
    assert action_event.action_type == "retry_run"
    assert action_event.actor_name == "ops.lead"
    assert action_event.action_reason == "Retry after source refresh."
    assert json.loads(action_event.action_payload or "{}") == {
        "previous_failure_reason": "MISSING_SOURCE"
    }


def test_cancel_run_marks_failed_and_records_operator_action(db_session: Session) -> None:
    created_batch = create_batch(db_session, ["DFW"])
    run_id = created_batch.runs[0].run_id

    cancelled_run = cancel_run(
        db_session,
        run_id,
        actor_name="ops.lead",
        action_reason="Cancelled due to upstream outage.",
    )
    batch = get_batch(db_session, created_batch.batch_id)
    actions = list_operator_actions(db_session, run_id=run_id)

    assert cancelled_run.status == ScoreRunStatus.FAILED.value
    assert cancelled_run.failure_reason == "MANUAL_CANCELLED"
    assert cancelled_run.completed_at is not None
    assert batch.status == ScoreBatchStatus.FAILED.value
    assert actions[0].action_type == "cancel_run"
    assert actions[0].actor_name == "ops.lead"
    assert actions[0].action_reason == "Cancelled due to upstream outage."
    assert actions[0].action_payload == {"failure_reason": "MANUAL_CANCELLED"}


def test_cancel_run_rejects_active_batch(db_session: Session) -> None:
    created_batch = create_batch(db_session, ["DFW"])
    batch_model = db_session.get(ScoreBatch, UUID(created_batch.batch_id))

    assert batch_model is not None
    batch_model.status = ScoreBatchStatus.ACTIVE
    db_session.commit()

    try:
        cancel_run(db_session, created_batch.runs[0].run_id)
    except RunCancelNotAllowedError as exc:
        assert "active batch" in str(exc)
    else:
        raise AssertionError("Expected RunCancelNotAllowedError for an active batch.")


def test_rerun_batch_creates_replacement_batch_and_records_operator_action(
    db_session: Session,
) -> None:
    created_batch = create_batch(db_session, ["DFW", "AUS"])
    runs = (
        db_session.query(ScoreRun)
        .filter(ScoreRun.batch_id == UUID(created_batch.batch_id))
        .order_by(ScoreRun.metro_id)
        .all()
    )
    for run in runs:
        run.status = ScoreRunStatus.COMPLETED
        run.completed_at = datetime.now(UTC)
    db_session.commit()
    reconcile_batch(db_session, created_batch.batch_id)

    replacement_batch = rerun_batch(
        db_session,
        created_batch.batch_id,
        actor_name="ops.lead",
        action_reason="Rebuild after downstream verification drift.",
    )
    actions = list_operator_actions(db_session, batch_id=created_batch.batch_id)

    assert replacement_batch.batch_id != created_batch.batch_id
    assert replacement_batch.status == ScoreBatchStatus.BUILDING.value
    assert replacement_batch.expected_metros == 2
    assert [run.metro_id for run in replacement_batch.runs] == ["AUS", "DFW"]
    assert actions[0].action_type == "rerun_batch"
    assert actions[0].action_payload == {"replacement_batch_id": replacement_batch.batch_id}


def test_rerun_batch_rejects_building_batch(db_session: Session) -> None:
    created_batch = create_batch(db_session, ["DFW"])

    try:
        rerun_batch(db_session, created_batch.batch_id)
    except BatchRerunNotAllowedError as exc:
        assert "still building" in str(exc)
    else:
        raise AssertionError("Expected BatchRerunNotAllowedError for a building batch.")


def test_list_operator_actions_filters_by_batch_and_run(db_session: Session) -> None:
    first_batch = create_batch(db_session, ["DFW", "AUS"])
    second_batch = create_batch(db_session, ["PHX"])

    first_run = db_session.get(ScoreRun, UUID(first_batch.runs[0].run_id))
    second_run = db_session.get(ScoreRun, UUID(second_batch.runs[0].run_id))

    assert first_run is not None
    assert second_run is not None

    first_run.status = ScoreRunStatus.FAILED
    first_run.failure_reason = "MISSING_SOURCE"
    first_run.completed_at = datetime.now(UTC)
    second_run.status = ScoreRunStatus.FAILED
    second_run.failure_reason = "SOURCE_TIMEOUT"
    second_run.completed_at = datetime.now(UTC)
    db_session.commit()

    reconcile_batch(db_session, first_batch.batch_id)
    reconcile_batch(db_session, second_batch.batch_id)

    retry_run(db_session, first_run.run_id, actor_name="ops.one")
    retry_run(db_session, second_run.run_id, actor_name="ops.two")

    first_batch_actions = list_operator_actions(db_session, batch_id=first_batch.batch_id)
    first_run_actions = list_operator_actions(db_session, run_id=first_run.run_id)
    limited_actions = list_operator_actions(db_session, limit=1)

    assert len(first_batch_actions) == 1
    assert first_batch_actions[0].batch_id == first_batch.batch_id
    assert len(first_run_actions) == 1
    assert first_run_actions[0].run_id == str(first_run.run_id)
    assert len(limited_actions) == 1


def test_get_activation_check_passes_for_completed_batch(db_session: Session) -> None:
    created_batch = create_batch(db_session, ["DFW"])
    run_id = UUID(created_batch.runs[0].run_id)
    _seed_completed_scored_run(db_session, run_id=run_id, parcel_id="P-ACTIVE-PASS")
    run_model = db_session.get(ScoreRun, run_id)

    assert run_model is not None
    run_model.status = ScoreRunStatus.COMPLETED
    run_model.completed_at = datetime.now(UTC)
    db_session.commit()
    reconcile_batch(db_session, created_batch.batch_id)

    report = get_activation_check(db_session, created_batch.batch_id)

    assert report.activation_ready is True
    assert report.issue_count == 0
    assert report.run_counts.completed == 1


def test_get_activation_check_fails_when_pending_or_cardinality_is_invalid(
    db_session: Session,
) -> None:
    created_batch = create_batch(db_session, ["DFW"])
    run_id = UUID(created_batch.runs[0].run_id)
    _seed_completed_scored_run(
        db_session,
        run_id=run_id,
        parcel_id="P-ACTIVE-FAIL",
        factor_count=9,
        pending_count=1,
    )
    run_model = db_session.get(ScoreRun, run_id)

    assert run_model is not None
    run_model.status = ScoreRunStatus.COMPLETED
    run_model.completed_at = datetime.now(UTC)
    db_session.commit()
    reconcile_batch(db_session, created_batch.batch_id)

    report = get_activation_check(db_session, created_batch.batch_id)
    issue_codes = {issue.code for issue in report.issues}

    assert report.activation_ready is False
    assert "PENDING_PARCELS_REMAIN" in issue_codes
    assert "FACTOR_CARDINALITY_MISMATCH" in issue_codes


def _seed_completed_scored_run(
    session: Session,
    *,
    run_id: UUID,
    parcel_id: str,
    factor_count: int = 10,
    bonus_count: int = 5,
    pending_count: int = 0,
) -> None:
    if session.get(MetroCatalog, "DFW") is None:
        session.add(MetroCatalog(metro_id="DFW", display_name="Dallas-Fort Worth", state_code="TX"))
    if session.get(CountyCatalog, "48085") is None:
        session.add(
            CountyCatalog(
                county_fips="48085",
                metro_id="DFW",
                display_name="Collin",
                state_code="TX",
            )
        )
    if session.get(RawParcel, parcel_id) is None:
        session.add(
            RawParcel(
                parcel_id=parcel_id,
                county_fips="48085",
                metro_id="DFW",
                acreage=Decimal("25.00"),
                geometry_wkt="POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                lineage_key=f"parcel:{parcel_id}",
                is_active=True,
            )
        )
    for index in range(1, 11):
        if session.get(FactorCatalog, f"F{index:02d}") is None:
            session.add(
                FactorCatalog(
                    factor_id=f"F{index:02d}",
                    display_name=f"Factor {index:02d}",
                    description=f"Seed factor {index:02d}",
                    ordinal=index,
                    is_active=True,
                )
            )
    for index in range(1, 6):
        if session.get(BonusCatalog, f"B{index:02d}") is None:
            session.add(
                BonusCatalog(
                    bonus_id=f"B{index:02d}",
                    display_name=f"Bonus {index:02d}",
                    description=f"Seed bonus {index:02d}",
                    max_points=5,
                    is_active=True,
                )
            )
    session.flush()

    for index in range(1, pending_count + 1):
        pending_parcel_id = f"{parcel_id}-PENDING-{index}"
        if session.get(RawParcel, pending_parcel_id) is None:
            session.add(
                RawParcel(
                    parcel_id=pending_parcel_id,
                    county_fips="48085",
                    metro_id="DFW",
                    acreage=Decimal("25.00"),
                    geometry_wkt="POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                    lineage_key=f"parcel:{pending_parcel_id}",
                    is_active=True,
                )
            )
            session.flush()
        session.add(
            ParcelEvaluation(
                run_id=run_id,
                parcel_id=pending_parcel_id,
                status=ParcelEvaluationStatus.PENDING_SCORING,
                status_reason="Still pending scoring.",
            )
        )

    session.add(
        ParcelEvaluation(
            run_id=run_id,
            parcel_id=parcel_id,
            status=ParcelEvaluationStatus.SCORED,
            status_reason="Scored for activation check.",
            viability_score=Decimal("75.00"),
            confidence_score=Decimal("90.00"),
        )
    )

    for index in range(1, factor_count + 1):
        session.add(
            ScoreFactorDetail(
                run_id=run_id,
                parcel_id=parcel_id,
                factor_id=f"F{index:02d}",
                points_awarded=Decimal("1.00"),
                rationale="Seed factor detail.",
            )
        )

    for index in range(1, bonus_count + 1):
        session.add(
            ScoreBonusDetail(
                run_id=run_id,
                parcel_id=parcel_id,
                bonus_id=f"B{index:02d}",
                applied=True,
                points_awarded=Decimal("1.00"),
                rationale="Seed bonus detail.",
            )
        )

    session.commit()
