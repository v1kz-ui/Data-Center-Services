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
    assert payload["covered_sources"] == 34
    assert payload["live_ready_sources"] == 16
    assert payload["live_ready_percent"] == 47.06
    assert len(payload["covered_if_codes"]) == 34
    assert "dfw_parcel_pilot" in payload["unmapped_connector_keys"]
    acs_item = next(item for item in payload["items"] if item["if_code"] == "IF-010")
    dallas_zoning_item = next(item for item in payload["items"] if item["if_code"] == "IF-044")
    fort_worth_zoning_item = next(item for item in payload["items"] if item["if_code"] == "IF-045")
    hifld_substations_item = next(item for item in payload["items"] if item["if_code"] == "IF-001")
    hifld_gas_pipelines_item = next(item for item in payload["items"] if item["if_code"] == "IF-003")
    nfhl_item = next(item for item in payload["items"] if item["if_code"] == "IF-006")
    nri_item = next(item for item in payload["items"] if item["if_code"] == "IF-007")
    tceq_item = next(item for item in payload["items"] if item["if_code"] == "IF-019")
    twdb_item = next(item for item in payload["items"] if item["if_code"] == "IF-020")
    census_tracts_item = next(item for item in payload["items"] if item["if_code"] == "IF-011")
    superfund_item = next(item for item in payload["items"] if item["if_code"] == "IF-023")
    critical_habitat_item = next(item for item in payload["items"] if item["if_code"] == "IF-024")
    peeringdb_item = next(item for item in payload["items"] if item["if_code"] == "IF-009")
    assert dallas_zoning_item["implemented"] is True
    assert dallas_zoning_item["live_ready"] is False
    assert dallas_zoning_item["connector_keys"] == ["dfw_dallas_arcgis_zoning_live"]
    assert fort_worth_zoning_item["implemented"] is True
    assert fort_worth_zoning_item["live_ready"] is False
    assert fort_worth_zoning_item["connector_keys"] == ["dfw_fort_worth_arcgis_zoning_live"]
    assert hifld_substations_item["live_ready"] is True
    assert hifld_substations_item["enabled_connector_keys"] == ["tx_hifld_substations_live"]
    assert hifld_gas_pipelines_item["live_ready"] is True
    assert hifld_gas_pipelines_item["enabled_connector_keys"] == ["tx_hifld_gas_pipelines_live"]
    assert nfhl_item["live_ready"] is True
    assert nfhl_item["enabled_connector_keys"] == ["tx_fema_nfhl_flood_zones_live"]
    assert nri_item["live_ready"] is True
    assert nri_item["enabled_connector_keys"] == ["tx_fema_nri_counties_live"]
    assert tceq_item["live_ready"] is True
    assert tceq_item["enabled_connector_keys"] == ["tx_tceq_water_rights_live"]
    assert twdb_item["live_ready"] is True
    assert twdb_item["enabled_connector_keys"] == ["tx_twdb_groundwater_live"]
    assert acs_item["live_ready"] is True
    assert acs_item["enabled_connector_keys"] == ["tx_census_acs_live"]
    assert census_tracts_item["live_ready"] is True
    assert census_tracts_item["enabled_connector_keys"] == ["tx_census_tracts_live"]
    assert superfund_item["live_ready"] is True
    assert superfund_item["enabled_connector_keys"] == ["tx_epa_superfund_sites_live"]
    assert critical_habitat_item["live_ready"] is True
    assert critical_habitat_item["enabled_connector_keys"] == ["tx_usfws_critical_habitat_live"]
    assert peeringdb_item["live_ready"] is True
    assert peeringdb_item["enabled_connector_keys"] == ["tx_peeringdb_facilities_live"]
