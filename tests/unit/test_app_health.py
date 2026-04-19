from fastapi import status
from fastapi.testclient import TestClient

from app.api.routes import system as system_routes
from app.main import app


def test_dashboard_requires_authenticated_viewer_headers() -> None:
    client = TestClient(app)
    response = client.get("/")
    assert response.status_code == status.HTTP_401_UNAUTHORIZED

    response = client.get("/dashboard/summary")
    assert response.status_code == status.HTTP_401_UNAUTHORIZED


def test_dashboard_landing_page_returns_private_client_shell(
    reader_headers: dict[str, str],
) -> None:
    client = TestClient(app)
    response = client.get("/", headers=reader_headers)
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "Texas Private Client Siting Portal" in response.text
    assert "Private Texas Client Intelligence Portal" in response.text
    assert "Texas Opportunity Catalogue" in response.text
    assert "Plano North Utility Belt" in response.text
    assert "Katy West Power Exchange" in response.text
    assert 'href="/dashboard/summary"' in response.text
    assert 'href="/health"' in response.text


def test_dashboard_summary_returns_texas_opportunity_catalog(
    reader_headers: dict[str, str],
) -> None:
    client = TestClient(app)
    response = client.get("/dashboard/summary", headers=reader_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["app_name"] == "dense-data-center-locator"
    assert payload["display_name"] == "Texas Private Client Siting Portal"
    assert payload["market"] == "Texas"
    assert payload["data_mode"] == "seeded_catalog"
    assert payload["opportunity_count"] == 50
    assert payload["corridor_count"] == 12
    assert len(payload["featured_opportunities"]) == 6
    assert len(payload["opportunities"]) == 50
    assert any(item["metro"] == "Dallas-Fort Worth" for item in payload["opportunities"])
    assert any(
        item["university_anchor"] == "University of Texas at Austin"
        for item in payload["opportunities"]
    )
    assert payload["data_coverage"]["available"] is True
    assert payload["data_coverage"]["total_sources"] == 51


def test_dashboard_summary_includes_live_monitoring_snapshot(monkeypatch) -> None:
    fake_monitoring = {
        "available": True,
        "error": None,
        "evaluated_at": "2026-04-18T12:00:00+00:00",
        "alert_count": 3,
        "threshold_trigger_count": 2,
        "failed_run_count": 1,
        "source_issue_count": 4,
        "run_counts": {"running": 2, "failed": 1, "completed": 6},
        "batch_counts": {"building": 1, "failed": 0, "completed": 3, "active": 1},
        "latest_batch": {
            "batch_id": "batch-live-001",
            "status": "active",
            "expected_metros": 8,
            "completed_metros": 5,
            "activation_ready": False,
            "activated_at": None,
        },
        "recent_failed_runs": [],
        "alerts": [],
        "thresholds": [],
        "source_health": {
            "total": 10,
            "healthy": 6,
            "failed": 2,
            "quarantined": 2,
        },
        "freshness": None,
    }

    monkeypatch.setattr(
        system_routes,
        "_read_monitoring_snapshot",
        lambda settings: fake_monitoring,
    )

    client = TestClient(app)
    response = client.get(
        "/dashboard/summary",
        headers={
            "X-DDCL-Subject": "test-reader",
            "X-DDCL-Name": "Test Reader",
            "X-DDCL-Roles": "reader",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["monitoring"]["available"] is True
    assert payload["monitoring"]["alert_count"] == 3
    assert payload["monitoring"]["latest_batch"]["batch_id"] == "batch-live-001"
    assert payload["monitoring"]["run_counts"]["running"] == 2


def test_dashboard_summary_falls_back_when_inventory_is_unavailable(
    monkeypatch,
    reader_headers: dict[str, str],
) -> None:
    monkeypatch.setattr(
        system_routes,
        "_read_source_inventory_snapshot",
        lambda settings: {
            "available": False,
            "error": "inventory unavailable for test",
            "version": None,
            "captured_at": None,
            "total_sources": 0,
            "free_sources": 0,
            "config_flag_count": 0,
            "phase_totals": [],
        },
    )

    client = TestClient(app)
    response = client.get("/dashboard/summary", headers=reader_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["data_coverage"]["available"] is False
    assert payload["data_coverage"]["error"] == "inventory unavailable for test"
    assert payload["data_coverage"]["phase_totals"] == []


def test_health_endpoint_returns_ok() -> None:
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.headers["X-Request-ID"]
    assert response.headers["X-Trace-ID"]


def test_version_endpoint_returns_version() -> None:
    client = TestClient(app)
    response = client.get("/version")
    assert response.status_code == 200
    assert response.json()["version"] == "0.1.0"


def test_health_endpoint_echoes_trace_headers() -> None:
    client = TestClient(app)
    response = client.get(
        "/health",
        headers={
            "X-Request-ID": "req-health-001",
            "X-Trace-ID": "trace-health-001",
        },
    )

    assert response.status_code == 200
    assert response.headers["X-Request-ID"] == "req-health-001"
    assert response.headers["X-Trace-ID"] == "trace-health-001"


def test_foundation_tables_endpoint_returns_foundational_tables(
    admin_headers: dict[str, str],
) -> None:
    client = TestClient(app)
    response = client.get("/foundation/tables", headers=admin_headers)
    assert response.status_code == 200
    assert "score_batch" in response.json()["tables"]


def test_orchestration_plan_endpoint_returns_batch_plan(
    admin_headers: dict[str, str],
) -> None:
    client = TestClient(app)
    response = client.post(
        "/orchestration/plan",
        headers=admin_headers,
        json={"metro_ids": ["DFW", "AUS"]},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "building"
    assert payload["expected_metros"] == 2
