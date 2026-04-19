from __future__ import annotations

from uuid import uuid4

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

from tests.unit.test_scoring_service import _seed_scoring_context


def test_run_audit_package_endpoint_returns_run_freshness_and_parcel_evidence(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    session = session_factory()
    try:
        run_id = _seed_scoring_context(session)
    finally:
        session.close()

    scoring_response = client.post(f"/admin/runs/{run_id}/scoring", json={})
    batch_id = scoring_response.json()["batch_id"]
    client.post(
        f"/orchestration/batches/{batch_id}/rerun",
        json={"action_reason": "Audit package validation rerun."},
    )

    response = client.get(
        f"/admin/audit/packages/runs/{run_id}",
        params={"parcel_id": "P-SCORE-A"},
    )

    assert response.status_code == 200
    payload = response.json()
    snapshot_sources = {item["source_id"] for item in payload["source_snapshots"]}
    evidence_attributes = {
        item["attribute_name"] for item in payload["parcel_evidence"]["source_evidence"]
    }

    assert payload["package_version"] == "phase7-audit-v1"
    assert payload["export_scope"] == "run_with_parcel"
    assert payload["exported_by"] == "Test Admin"
    assert payload["run"]["run_id"] == run_id
    assert payload["batch"]["batch_id"] == batch_id
    assert payload["freshness"]["passed"] is True
    assert snapshot_sources == {"PARCEL", "SCORING"}
    assert payload["operator_actions"][0]["action_type"] == "rerun_batch"
    assert payload["parcel_evidence"]["parcel_context"]["parcel_id"] == "P-SCORE-A"
    assert payload["parcel_evidence"]["parcel_detail"]["parcel_id"] == "P-SCORE-A"
    assert payload["parcel_evidence"]["parcel_detail"]["viability_score"] == "60.00"
    assert "f01_measured" in evidence_attributes


def test_run_audit_package_endpoint_supports_run_scope_without_parcel(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    session = session_factory()
    try:
        run_id = _seed_scoring_context(session)
    finally:
        session.close()

    response = client.get(f"/admin/audit/packages/runs/{run_id}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["export_scope"] == "run"
    assert payload["run"]["run_id"] == run_id
    assert payload["parcel_evidence"] is None


def test_run_audit_package_endpoint_rejects_reader_role(
    client: TestClient,
    reader_headers: dict[str, str],
) -> None:
    response = client.get(
        f"/admin/audit/packages/runs/{uuid4()}",
        headers=reader_headers,
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "This operation requires one of the following roles: admin."


def test_run_audit_package_endpoint_returns_not_found_for_unknown_parcel(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    session = session_factory()
    try:
        run_id = _seed_scoring_context(session)
    finally:
        session.close()

    response = client.get(
        f"/admin/audit/packages/runs/{run_id}",
        params={"parcel_id": "P-UNKNOWN"},
    )

    assert response.status_code == 404
    assert "was not found for run" in response.json()["detail"]
