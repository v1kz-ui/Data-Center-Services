from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

from app.db.models.batching import ScoreBatch, ScoreRun
from app.db.models.catalogs import BonusCatalog, FactorCatalog
from app.db.models.enums import ParcelEvaluationStatus, ScoreBatchStatus, ScoreRunStatus
from app.db.models.evaluation import ParcelEvaluation
from app.db.models.scoring import ScoreBonusDetail, ScoreFactorDetail
from app.db.models.territory import CountyCatalog, MetroCatalog, RawParcel


def test_create_batch_endpoint_persists_and_returns_runs(client: TestClient) -> None:
    response = client.post(
        "/orchestration/batches",
        json={"metro_ids": ["dfw", "austin", "DFW"]},
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["status"] == "building"
    assert payload["expected_metros"] == 2
    assert payload["run_counts"] == {"running": 2, "failed": 0, "completed": 0}
    assert payload["activation_ready"] is False
    assert [run["metro_id"] for run in payload["runs"]] == ["DFW", "AUSTIN"]
    assert all(run["started_at"] is not None for run in payload["runs"])


def test_get_batch_endpoint_returns_existing_batch(client: TestClient) -> None:
    created_response = client.post(
        "/orchestration/batches",
        json={"metro_ids": ["PHX", "LAS"]},
    )
    batch_id = created_response.json()["batch_id"]

    get_response = client.get(f"/orchestration/batches/{batch_id}")

    assert get_response.status_code == 200
    payload = get_response.json()
    assert payload["batch_id"] == batch_id
    assert payload["expected_metros"] == 2


def test_list_batches_endpoint_returns_batches_in_reverse_chronological_order(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    first_batch_id = client.post("/orchestration/batches", json={"metro_ids": ["DFW"]}).json()[
        "batch_id"
    ]
    session = session_factory()
    try:
        first_batch = session.get(ScoreBatch, UUID(first_batch_id))
        assert first_batch is not None
        first_batch.created_at = first_batch.created_at.replace(year=2026, month=4, day=1)
        session.commit()
    finally:
        session.close()

    second_batch_id = client.post("/orchestration/batches", json={"metro_ids": ["AUS"]}).json()[
        "batch_id"
    ]

    response = client.get("/orchestration/batches")

    assert response.status_code == 200
    payload = response.json()
    assert [item["batch_id"] for item in payload[:2]] == [second_batch_id, first_batch_id]


def test_reconcile_batch_endpoint_returns_failed_aggregate(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    batch_payload = client.post("/orchestration/batches", json={"metro_ids": ["DFW", "AUS"]}).json()

    session = session_factory()
    try:
        runs = (
            session.query(ScoreRun)
            .filter(ScoreRun.batch_id == UUID(batch_payload["batch_id"]))
            .order_by(ScoreRun.metro_id)
            .all()
        )
        runs[0].status = ScoreRunStatus.COMPLETED
        runs[1].status = ScoreRunStatus.FAILED
        runs[1].failure_reason = "MISSING_SOURCE"
        session.commit()
    finally:
        session.close()

    response = client.post(f"/orchestration/batches/{batch_payload['batch_id']}/reconcile")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "failed"
    assert payload["completed_metros"] == 1
    assert payload["run_counts"]["failed"] == 1


def test_get_run_endpoint_returns_run_operator_fields(
    client: TestClient,
) -> None:
    batch_payload = client.post("/orchestration/batches", json={"metro_ids": ["DFW"]}).json()
    run_id = batch_payload["runs"][0]["run_id"]

    response = client.get(f"/orchestration/runs/{run_id}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["run_id"] == run_id
    assert payload["batch_id"] == batch_payload["batch_id"]
    assert payload["metro_id"] == "DFW"
    assert payload["status"] == "running"
    assert payload["started_at"] is not None


def test_retry_run_endpoint_resets_failed_run_to_running(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    batch_payload = client.post("/orchestration/batches", json={"metro_ids": ["DFW"]}).json()
    run_id = batch_payload["runs"][0]["run_id"]

    session = session_factory()
    try:
        run = session.get(ScoreRun, UUID(run_id))
        assert run is not None
        run.status = ScoreRunStatus.FAILED
        run.failure_reason = "MISSING_SOURCE"
        run.completed_at = datetime(2026, 4, 12, tzinfo=UTC)
        session.commit()
    finally:
        session.close()

    client.post(f"/orchestration/batches/{batch_payload['batch_id']}/reconcile")
    response = client.post(
        f"/orchestration/runs/{run_id}/retry",
        json={
            "actor_name": "ops.api",
            "action_reason": "Retry after upstream data repair.",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["run"]["status"] == "running"
    assert payload["run"]["failure_reason"] is None
    assert payload["batch"]["status"] == "building"

    history_response = client.get(f"/orchestration/actions?run_id={run_id}")

    assert history_response.status_code == 200
    history_payload = history_response.json()
    assert history_payload[0]["action_type"] == "retry_run"
    assert history_payload[0]["actor_name"] == "ops.api"
    assert history_payload[0]["action_reason"] == "Retry after upstream data repair."
    assert history_payload[0]["action_payload"] == {
        "previous_failure_reason": "MISSING_SOURCE"
    }


def test_cancel_run_endpoint_marks_run_failed_and_records_action(
    client: TestClient,
) -> None:
    batch_payload = client.post("/orchestration/batches", json={"metro_ids": ["DFW"]}).json()
    run_id = batch_payload["runs"][0]["run_id"]

    response = client.post(
        f"/orchestration/runs/{run_id}/cancel",
        json={
            "action_reason": "Cancelled pending source remediation.",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["run"]["status"] == "failed"
    assert payload["run"]["failure_reason"] == "MANUAL_CANCELLED"
    assert payload["batch"]["status"] == "failed"

    history_response = client.get(f"/orchestration/actions?batch_id={batch_payload['batch_id']}")

    assert history_response.status_code == 200
    history_payload = history_response.json()
    assert history_payload[0]["action_type"] == "cancel_run"
    assert history_payload[0]["actor_name"] == "Test Admin"
    assert history_payload[0]["action_payload"] == {"failure_reason": "MANUAL_CANCELLED"}


def test_cancel_run_endpoint_rejects_active_batch(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    batch_payload = client.post("/orchestration/batches", json={"metro_ids": ["DFW"]}).json()
    run_id = batch_payload["runs"][0]["run_id"]

    session = session_factory()
    try:
        batch = session.get(ScoreBatch, UUID(batch_payload["batch_id"]))
        assert batch is not None
        batch.status = ScoreBatchStatus.ACTIVE
        session.commit()
    finally:
        session.close()

    response = client.post(f"/orchestration/runs/{run_id}/cancel")

    assert response.status_code == 409
    assert "active batch" in response.json()["detail"]


def test_rerun_batch_endpoint_creates_replacement_batch_and_audit_event(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    batch_payload = client.post("/orchestration/batches", json={"metro_ids": ["DFW", "AUS"]}).json()

    session = session_factory()
    try:
        runs = (
            session.query(ScoreRun)
            .filter(ScoreRun.batch_id == UUID(batch_payload["batch_id"]))
            .all()
        )
        for run in runs:
            run.status = ScoreRunStatus.COMPLETED
            run.completed_at = datetime.now(UTC)
        session.commit()
    finally:
        session.close()

    client.post(f"/orchestration/batches/{batch_payload['batch_id']}/reconcile")
    response = client.post(
        f"/orchestration/batches/{batch_payload['batch_id']}/rerun",
        json={
            "actor_name": "ops.api",
            "action_reason": "Requeue complete metro pack.",
        },
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["source_batch_id"] == batch_payload["batch_id"]
    assert payload["replacement_batch"]["batch_id"] != batch_payload["batch_id"]
    assert payload["replacement_batch"]["status"] == "building"
    assert payload["replacement_batch"]["expected_metros"] == 2

    history_response = client.get(f"/orchestration/actions?batch_id={batch_payload['batch_id']}")

    assert history_response.status_code == 200
    history_payload = history_response.json()
    assert history_payload[0]["action_type"] == "rerun_batch"
    assert history_payload[0]["action_payload"] == {
        "replacement_batch_id": payload["replacement_batch"]["batch_id"]
    }


def test_list_operator_actions_endpoint_filters_results(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    first_batch = client.post("/orchestration/batches", json={"metro_ids": ["DFW"]}).json()
    second_batch = client.post("/orchestration/batches", json={"metro_ids": ["PHX"]}).json()

    session = session_factory()
    try:
        first_run = session.get(ScoreRun, UUID(first_batch["runs"][0]["run_id"]))
        second_run = session.get(ScoreRun, UUID(second_batch["runs"][0]["run_id"]))
        assert first_run is not None
        assert second_run is not None
        first_run.status = ScoreRunStatus.FAILED
        first_run.failure_reason = "MISSING_SOURCE"
        first_run.completed_at = datetime.now(UTC)
        second_run.status = ScoreRunStatus.FAILED
        second_run.failure_reason = "SOURCE_TIMEOUT"
        second_run.completed_at = datetime.now(UTC)
        session.commit()
    finally:
        session.close()

    client.post(f"/orchestration/batches/{first_batch['batch_id']}/reconcile")
    client.post(f"/orchestration/batches/{second_batch['batch_id']}/reconcile")
    client.post(f"/orchestration/runs/{first_batch['runs'][0]['run_id']}/retry")
    client.post(f"/orchestration/runs/{second_batch['runs'][0]['run_id']}/retry")

    batch_response = client.get(f"/orchestration/actions?batch_id={first_batch['batch_id']}")
    run_response = client.get(f"/orchestration/actions?run_id={first_batch['runs'][0]['run_id']}")
    limited_response = client.get("/orchestration/actions?limit=1")

    assert batch_response.status_code == 200
    assert len(batch_response.json()) == 1
    assert batch_response.json()[0]["batch_id"] == first_batch["batch_id"]

    assert run_response.status_code == 200
    assert len(run_response.json()) == 1
    assert run_response.json()[0]["run_id"] == first_batch["runs"][0]["run_id"]

    assert limited_response.status_code == 200
    assert len(limited_response.json()) == 1


def test_activation_check_endpoint_returns_validation_issues(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    batch_payload = client.post("/orchestration/batches", json={"metro_ids": ["DFW"]}).json()
    run_id = UUID(batch_payload["runs"][0]["run_id"])

    session = session_factory()
    try:
        _seed_activation_check_context(
            session,
            run_id=run_id,
            parcel_id="P-API-ACTIVATION",
            factor_count=9,
            pending_count=1,
        )
        run = session.get(ScoreRun, run_id)
        assert run is not None
        run.status = ScoreRunStatus.COMPLETED
        run.completed_at = datetime.now(UTC)
        session.commit()
    finally:
        session.close()

    client.post(f"/orchestration/batches/{batch_payload['batch_id']}/reconcile")
    response = client.get(f"/orchestration/batches/{batch_payload['batch_id']}/activation-check")

    assert response.status_code == 200
    payload = response.json()
    issue_codes = {item["code"] for item in payload["issues"]}
    assert payload["activation_ready"] is False
    assert "PENDING_PARCELS_REMAIN" in issue_codes
    assert "FACTOR_CARDINALITY_MISMATCH" in issue_codes


def _seed_activation_check_context(
    session: Session,
    *,
    run_id: UUID,
    parcel_id: str,
    factor_count: int,
    pending_count: int,
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

    if pending_count:
        pending_parcel_id = f"{parcel_id}-PENDING-1"
        if session.get(RawParcel, pending_parcel_id) is None:
            session.add(
                RawParcel(
                    parcel_id=pending_parcel_id,
                    county_fips="48085",
                    metro_id="DFW",
                    acreage=Decimal("20.00"),
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
            viability_score=Decimal("70.00"),
            confidence_score=Decimal("85.00"),
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

    for index in range(1, 6):
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
