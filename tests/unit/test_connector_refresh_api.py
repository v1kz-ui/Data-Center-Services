from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

from app.core.settings import Settings, get_settings
from app.db.models.catalogs import SourceCatalog
from app.db.models.territory import CountyCatalog, MetroCatalog


def test_connector_definition_endpoint_exposes_pilot_connectors(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _seed_reference_catalogs(session_factory)

    response = client.get("/admin/connectors/definitions")
    payload = response.json()

    assert response.status_code == 200
    assert {item["connector_key"] for item in payload} == {
        "dfw_dallas_arcgis_parcels_live",
        "dfw_dallas_arcgis_zoning_live",
        "dfw_fort_worth_arcgis_zoning_live",
        "dfw_parcel_pilot",
        "dfw_zoning_pilot",
    }
    live_parcel_definition = next(
        item for item in payload if item["connector_key"] == "dfw_dallas_arcgis_parcels_live"
    )
    fort_worth_zoning_definition = next(
        item for item in payload if item["connector_key"] == "dfw_fort_worth_arcgis_zoning_live"
    )
    assert live_parcel_definition["enabled"] is False
    assert live_parcel_definition["priority"] == 10
    assert fort_worth_zoning_definition["inventory_if_codes"] == ["IF-045"]
    assert fort_worth_zoning_definition["preprocess_strategy"] == "zoning_overlay_to_parcels"


def test_due_connector_refresh_endpoint_loads_fixture_data_and_supports_parcel_search(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _seed_reference_catalogs(session_factory)

    refresh_response = client.post("/admin/connectors/refresh-due")
    refresh_payload = refresh_response.json()
    search_response = client.get(
        "/parcels/search",
        params={"metro_id": "DFW", "zoning_code": "LI"},
    )
    search_payload = search_response.json()

    assert refresh_response.status_code == 200
    assert refresh_payload["total_due"] == 2
    assert refresh_payload["completed"] == 2
    assert {item["source_id"] for item in refresh_payload["reports"]} == {"PARCEL", "ZONING"}
    assert search_response.status_code == 200
    assert search_payload["total_count"] == 1
    assert search_payload["items"][0]["parcel_id"] == "DFW-PARCEL-1001"
    assert search_payload["items"][0]["zoning_code"] == "LI"


def test_manual_connector_refresh_updates_refresh_plan(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _seed_reference_catalogs(session_factory)

    refresh_response = client.post("/admin/connectors/PARCEL/metros/DFW/refresh")
    refresh_payload = refresh_response.json()
    plan_response = client.get("/admin/connectors/refresh-plan")
    plan_payload = plan_response.json()
    parcel_item = next(
        item for item in plan_payload if item["connector_key"] == "dfw_parcel_pilot"
    )

    assert refresh_response.status_code == 200
    assert refresh_payload["accepted_count"] == 2
    assert parcel_item["due"] is False
    assert parcel_item["due_reason"] == "within_cadence"


def test_connector_refresh_by_key_targets_explicit_same_scope_connector(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _seed_reference_catalogs(session_factory)
    config_path = _make_workspace_temp_dir() / "same_scope_api_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "dfw_parcel_primary",
                        "source_id": "PARCEL",
                        "metro_id": "DFW",
                        "interface_name": "parcel_fixture_primary_v1",
                        "adapter_type": "fixture",
                        "enabled": True,
                        "priority": 10,
                        "fetch_policy": {
                            "checkpoint_field": "updated_at",
                        },
                        "field_map": {
                            "parcel_id": "parcel_id",
                            "county_fips": "county_fips",
                            "acreage": "acreage",
                            "geometry_wkt": "geometry_wkt",
                            "lineage_key": "lineage_key",
                            "apn": "apn",
                        },
                        "fixture_records": [
                            {
                                "parcel_id": "DFW-PRIMARY-1001",
                                "county_fips": "48085",
                                "acreage": "10.0",
                                "geometry_wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                                "lineage_key": "parcel:DFW-PRIMARY-1001",
                                "apn": "PRIMARY1001",
                                "updated_at": "2026-04-18T09:00:00Z",
                            }
                        ],
                    },
                    {
                        "connector_key": "dfw_parcel_secondary",
                        "source_id": "PARCEL",
                        "metro_id": "DFW",
                        "interface_name": "parcel_fixture_secondary_v1",
                        "adapter_type": "fixture",
                        "enabled": True,
                        "priority": 20,
                        "fetch_policy": {
                            "checkpoint_field": "updated_at",
                        },
                        "field_map": {
                            "parcel_id": "parcel_id",
                            "county_fips": "county_fips",
                            "acreage": "acreage",
                            "geometry_wkt": "geometry_wkt",
                            "lineage_key": "lineage_key",
                            "apn": "apn",
                        },
                        "fixture_records": [
                            {
                                "parcel_id": "DFW-SECONDARY-2001",
                                "county_fips": "48085",
                                "acreage": "14.0",
                                "geometry_wkt": "POLYGON ((2 0, 2 1, 3 1, 3 0, 2 0))",
                                "lineage_key": "parcel:DFW-SECONDARY-2001",
                                "apn": "SECONDARY2001",
                                "updated_at": "2026-04-18T09:30:00Z",
                            }
                        ],
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    client.app.dependency_overrides[get_settings] = lambda: Settings(
        source_connector_config_path=str(config_path)
    )

    try:
        refresh_response = client.post("/admin/connectors/dfw_parcel_secondary/refresh")
        refresh_payload = refresh_response.json()
        plan_response = client.get("/admin/connectors/refresh-plan")
        plan_payload = plan_response.json()
    finally:
        client.app.dependency_overrides.pop(get_settings, None)

    primary_item = next(
        item for item in plan_payload if item["connector_key"] == "dfw_parcel_primary"
    )
    secondary_item = next(
        item for item in plan_payload if item["connector_key"] == "dfw_parcel_secondary"
    )

    assert refresh_response.status_code == 200
    assert refresh_payload["connector_key"] == "dfw_parcel_secondary"
    assert refresh_payload["accepted_count"] == 1
    assert primary_item["due"] is True
    assert primary_item["due_reason"] == "missing_snapshot"
    assert secondary_item["due"] is False
    assert secondary_item["due_reason"] == "within_cadence"


def _seed_reference_catalogs(session_factory: sessionmaker[Session]) -> None:
    session = session_factory()
    try:
        session.add(MetroCatalog(metro_id="DFW", display_name="Dallas-Fort Worth", state_code="TX"))
        session.add(
            CountyCatalog(
                county_fips="48085",
                metro_id="DFW",
                display_name="Collin",
                state_code="TX",
            )
        )
        session.add_all(
            [
                SourceCatalog(
                    source_id="PARCEL",
                    display_name="Approved Parcel Feed",
                    owner_name="Data Governance",
                    refresh_cadence="daily",
                    block_refresh=True,
                    metro_coverage="DFW",
                    target_table_name="raw_parcels",
                    is_active=True,
                ),
                SourceCatalog(
                    source_id="ZONING",
                    display_name="Approved Zoning Feed",
                    owner_name="Data Governance",
                    refresh_cadence="weekly",
                    block_refresh=True,
                    metro_coverage="DFW",
                    target_table_name="raw_zoning",
                    is_active=True,
                ),
            ]
        )
        session.commit()
    finally:
        session.close()


def _make_workspace_temp_dir() -> Path:
    temp_dir = Path("temp") / f"connector-refresh-api-tests-{uuid4().hex}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir
