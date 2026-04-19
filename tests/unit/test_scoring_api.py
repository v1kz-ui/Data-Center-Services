from __future__ import annotations

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

from tests.unit.test_scoring_service import _seed_scoring_context


def test_execute_scoring_endpoint_returns_summary(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    session = session_factory()
    try:
        run_id = _seed_scoring_context(session)
    finally:
        session.close()

    response = client.post(f"/admin/runs/{run_id}/scoring", json={})
    payload = response.json()

    assert response.status_code == 200
    assert payload["run_status"] == "completed"
    assert payload["profile_name"] == "default_v1"
    assert payload["scored_count"] == 2
    assert payload["factor_detail_count"] == 20
    assert payload["bonus_detail_count"] == 10


def test_get_scoring_summary_endpoint_returns_post_run_counts(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    session = session_factory()
    try:
        run_id = _seed_scoring_context(session)
    finally:
        session.close()

    client.post(f"/admin/runs/{run_id}/scoring", json={})
    response = client.get(f"/admin/runs/{run_id}/scoring")
    payload = response.json()

    assert response.status_code == 200
    assert payload["run_id"] == run_id
    assert payload["pending_scoring_count"] == 0
    assert {item["status"] for item in payload["status_counts"]} == {"excluded", "scored"}


def test_get_scoring_detail_endpoint_returns_explanation(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    session = session_factory()
    try:
        run_id = _seed_scoring_context(session)
    finally:
        session.close()

    client.post(f"/admin/runs/{run_id}/scoring", json={})
    response = client.get(f"/admin/runs/{run_id}/scoring/parcels/P-SCORE-A")
    payload = response.json()

    assert response.status_code == 200
    assert payload["parcel_id"] == "P-SCORE-A"
    assert payload["status"] == "scored"
    assert payload["viability_score"] == "60.00"
    assert payload["bonus_details"][0]["bonus_id"] == "B01"
    assert payload["evidence_quality_counts"] == {"measured": 10, "proxy": 1}


def test_execute_scoring_endpoint_rejects_invalid_profile_name(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    session = session_factory()
    try:
        run_id = _seed_scoring_context(session)
    finally:
        session.close()

    response = client.post(
        f"/admin/runs/{run_id}/scoring",
        json={"profile_name": "missing_profile"},
    )
    payload = response.json()

    assert response.status_code == 422
    assert "missing_profile" in payload["detail"]
