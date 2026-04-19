from fastapi.testclient import TestClient

from app.api.routes import system as system_routes
from app.db.models import MANAGED_TABLES
from app.main import app


def test_dashboard_landing_page_returns_visual_shell() -> None:
    client = TestClient(app)
    response = client.get("/")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "Operations Command Center" in response.text
    assert "Live Monitoring Pulse" in response.text
    assert "Eight-Phase Delivery Grid" in response.text
    assert "Managed Table Cloud" in response.text
    assert 'href="/docs"' in response.text
    assert 'href="/health"' in response.text
    assert 'href="/dashboard/summary"' in response.text


def test_dashboard_summary_returns_platform_inventory() -> None:
    client = TestClient(app)
    response = client.get("/dashboard/summary")
    assert response.status_code == 200
    payload = response.json()
    assert payload["app_name"] == "dense-data-center-locator"
    assert payload["managed_table_count"] == len(MANAGED_TABLES)
    assert payload["phase_count"] == 8
    assert payload["public_route_count"] == 4
    assert "monitoring" in payload
    assert any(domain["slug"] == "orchestration" for domain in payload["domains"])
    assert any(tripwire["label"] == "Failed runs" for tripwire in payload["tripwires"])


def test_dashboard_summary_includes_live_monitoring_snapshot(
    monkeypatch,
) -> None:
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
    response = client.get("/dashboard/summary")
    assert response.status_code == 200
    payload = response.json()
    assert payload["monitoring"]["available"] is True
    assert payload["monitoring"]["alert_count"] == 3
    assert payload["monitoring"]["latest_batch"]["batch_id"] == "batch-live-001"
    assert payload["monitoring"]["run_counts"]["running"] == 2


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
