import re

from fastapi import status
from fastapi.testclient import TestClient

from app.api.routes import system as system_routes
from app.main import app


def test_dashboard_requires_authenticated_viewer_headers() -> None:
    client = TestClient(app)
    response = client.get("/")
    assert response.status_code == status.HTTP_401_UNAUTHORIZED

    response = client.get("/dashboard/contenders")
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
    assert "watchlist" in response.text
    assert "Open contender board" in response.text
    assert "Open live JSON" in response.text
    assert 'href="/dashboard/contenders"' in response.text
    assert 'href="/dashboard/summary"' in response.text
    assert 'href="/health"' in response.text


def test_dashboard_contenders_page_returns_top_50_board(
    reader_headers: dict[str, str],
) -> None:
    client = TestClient(app)
    response = client.get("/dashboard/contenders", headers=reader_headers)
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "Texas Top " in response.text
    assert "Major Metro Contender Board" in response.text
    assert "Open contender board" in response.text
    assert "Main portal" in response.text
    assert "Live JSON" in response.text
    assert "Approval" in response.text
    assert 'href="/dashboard/contenders/' in response.text
    assert 'data-href="/dashboard/contenders/' in response.text
    assert 'href="/"' in response.text
    assert 'href="/dashboard/summary"' in response.text


def test_dashboard_contender_detail_page_returns_full_explanation(
    reader_headers: dict[str, str],
) -> None:
    client = TestClient(app)
    board_response = client.get("/dashboard/contenders", headers=reader_headers)
    assert board_response.status_code == 200
    match = re.search(r'href="(/dashboard/contenders/[^"]+)"', board_response.text)
    assert match is not None

    response = client.get(match.group(1), headers=reader_headers)
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "Comprehensive explanation" in response.text
    assert "Decision Lens" in response.text
    assert "Scoring Anatomy" in response.text
    assert "Approval Path" in response.text
    assert "Recommended Diligence Path" in response.text
    assert "Nearby Board Comparisons" in response.text
    assert "Review decision lens" in response.text
    assert "quick-nav" in response.text
    assert "Top 136 client dossier" in response.text
    assert "Executive Briefing Memo" in response.text
    assert "Risk And Mitigation Read" in response.text
    assert "Client Call Questions" in response.text
    assert "Pursuit Workplan" in response.text


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
    assert payload["data_mode"] in {"seeded_catalog", "live_candidate_scoring"}
    assert payload["opportunity_count"] >= 50
    assert len(payload["featured_opportunities"]) == 6
    assert len(payload["opportunities"]) == payload["opportunity_count"]
    assert payload["data_coverage"]["available"] is True
    assert payload["data_coverage"]["total_sources"] == 51
    assert any(item["metro"] for item in payload["opportunities"])
    assert any(item["university_anchor"] for item in payload["opportunities"])
    assert all("social_score" in item for item in payload["opportunities"])
    assert all("political_score" in item for item in payload["opportunities"])
    assert all("approval_score" in item for item in payload["opportunities"])
    assert all(item["approval_stage"] for item in payload["opportunities"])
    assert all(isinstance(item["approval_headwinds"], list) for item in payload["opportunities"])
    assert payload["corridor_count"] >= 1
    if payload["data_mode"] == "seeded_catalog":
        assert payload["opportunity_count"] == 50
        assert any(
            item["site_name"] == "Plano North Utility Belt"
            for item in payload["opportunities"]
        )
    else:
        assert 60 <= payload["opportunity_count"] <= 150
        assert payload["hero_title"].startswith(
            f"{payload['opportunity_count']} live-ranked Texas opportunities"
        )
        assert any(item["confidence_score"] is not None for item in payload["opportunities"])
        counts: dict[str, int] = {}
        for item in payload["opportunities"]:
            counts[item["metro"]] = counts.get(item["metro"], 0) + 1
        assert counts.get("Dallas-Fort Worth", 0) >= 30
        assert counts.get("Houston", 0) >= 25
        assert counts.get("San Antonio", 0) >= 25
        for metro_name in ("Austin", "Brazos Valley", "El Paso", "Rio Grande Valley"):
            assert counts.get(metro_name, 0) >= 12
        assert counts.get("Austin", 0) <= 20
        assert counts.get("Houston", 0) <= 25
        assert counts.get("San Antonio", 0) <= 25


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
