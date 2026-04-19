from __future__ import annotations

import logging

from fastapi.testclient import TestClient

from app.main import create_app


def test_orchestration_plan_requires_authentication() -> None:
    client = TestClient(create_app())

    response = client.post("/orchestration/plan", json={"metro_ids": ["DFW"]})

    assert response.status_code == 401
    assert response.json()["detail"] == "Authentication headers are required."


def test_operator_role_can_access_orchestration_plan(
    operator_headers: dict[str, str],
) -> None:
    client = TestClient(create_app())

    response = client.post(
        "/orchestration/plan",
        headers=operator_headers,
        json={"metro_ids": ["DFW"]},
    )

    assert response.status_code == 200
    assert response.json()["expected_metros"] == 1


def test_reader_role_cannot_access_operator_route(
    reader_headers: dict[str, str],
) -> None:
    client = TestClient(create_app())

    response = client.post(
        "/orchestration/plan",
        headers=reader_headers,
        json={"metro_ids": ["DFW"]},
    )

    assert response.status_code == 403
    assert "admin, operator" in response.json()["detail"]


def test_admin_role_can_access_foundation_tables(
    admin_headers: dict[str, str],
) -> None:
    client = TestClient(create_app())

    response = client.get("/foundation/tables", headers=admin_headers)

    assert response.status_code == 200
    assert "score_batch" in response.json()["tables"]


def test_denied_attempt_is_logged_for_admin_only_endpoint(
    reader_headers: dict[str, str],
    caplog,
) -> None:
    caplog.set_level(logging.WARNING, logger="app.core.security")
    client = TestClient(create_app())

    response = client.get("/foundation/tables", headers=reader_headers)

    assert response.status_code == 403
    assert "RBAC access denied" in caplog.text
