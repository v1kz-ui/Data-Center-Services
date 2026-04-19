from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker


def test_source_inventory_endpoint_exposes_authoritative_phase_scope(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _ = session_factory

    response = client.get(
        "/admin/source-inventory",
        params={"phase": 1, "category": "city_zoning"},
    )
    payload = response.json()

    assert response.status_code == 200
    assert payload["total_sources"] == 51
    assert payload["filtered_source_count"] == 2
    assert [item["if_code"] for item in payload["sources"]] == ["IF-044", "IF-045"]


def test_source_inventory_coverage_endpoint_reports_connector_progress(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _ = session_factory

    response = client.get(
        "/admin/source-inventory/coverage",
        params={"phase": 1},
    )
    payload = response.json()

    assert response.status_code == 200
    assert payload["total_sources"] == 34
    assert payload["covered_sources"] == 2
    assert payload["covered_if_codes"] == ["IF-044", "IF-045"]
    assert "dfw_parcel_pilot" in payload["unmapped_connector_keys"]
    dallas_zoning_item = next(item for item in payload["items"] if item["if_code"] == "IF-044")
    fort_worth_zoning_item = next(item for item in payload["items"] if item["if_code"] == "IF-045")
    assert dallas_zoning_item["implemented"] is True
    assert dallas_zoning_item["connector_keys"] == ["dfw_dallas_arcgis_zoning_live"]
    assert fort_worth_zoning_item["implemented"] is True
    assert fort_worth_zoning_item["connector_keys"] == ["dfw_fort_worth_arcgis_zoning_live"]
