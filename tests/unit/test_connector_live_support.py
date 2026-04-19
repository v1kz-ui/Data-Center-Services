from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

import ingestion.connectors as connector_module
from app.db.models.catalogs import SourceCatalog
from app.db.models.source_data import RawZoning
from app.db.models.territory import CountyCatalog, MetroCatalog
from ingestion.connectors import fetch_connector_records, load_connector_registry
from ingestion.refresh import refresh_source_connector
from ingestion.service import ingest_parcel_records


class _FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        return False

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")


def test_fetch_connector_records_paginates_arcgis_feature_service(
    monkeypatch,
) -> None:
    config_path = _make_workspace_temp_dir() / "connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "arcgis_test_parcels",
                        "source_id": "PARCEL",
                        "metro_id": "DFW",
                        "interface_name": "arcgis_test_v1",
                        "adapter_type": "arcgis_feature_service",
                        "enabled": True,
                        "request": {
                            "endpoint_url": "https://example.test/arcgis/rest/services/parcels/FeatureServer/0",
                            "query_params": {
                                "where": "1=1",
                                "outFields": "OBJECTID,AREA_FEET,GIS_ACCT,COUNTY",
                                "returnGeometry": "true",
                                "f": "geojson"
                            },
                            "pagination": {
                                "strategy": "arcgis_offset",
                                "page_size": 2
                            }
                        },
                        "field_rules": [
                            {
                                "target": "county_fips",
                                "source": "COUNTY",
                                "transform": "map_value",
                                "options": {
                                    "map": {
                                        "Dallas": "48113"
                                    }
                                }
                            },
                            {
                                "target": "apn",
                                "source": "GIS_ACCT",
                                "transform": "strip"
                            },
                            {
                                "target": "parcel_id",
                                "transform": "template",
                                "template": "DFW:{county_fips}:{apn}"
                            },
                            {
                                "target": "acreage",
                                "source": "AREA_FEET",
                                "transform": "square_feet_to_acres"
                            },
                            {
                                "target": "geometry_wkt",
                                "source": "__geometry__",
                                "transform": "geojson_to_wkt"
                            },
                            {
                                "target": "lineage_key",
                                "transform": "template",
                                "template": "parcel:{parcel_id}"
                            }
                        ]
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    registry = load_connector_registry(str(config_path))
    definition = registry.get_definition_by_connector_key("arcgis_test_parcels")

    requested_urls: list[str] = []
    payloads = [
        {
            "features": [
                {
                    "properties": {
                        "OBJECTID": 1,
                        "COUNTY": "Dallas",
                        "GIS_ACCT": "A100",
                        "AREA_FEET": 43560,
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]
                        ],
                    },
                },
                {
                    "properties": {
                        "OBJECTID": 2,
                        "COUNTY": "Dallas",
                        "GIS_ACCT": "A101",
                        "AREA_FEET": 87120,
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [[2, 0], [2, 1], [3, 1], [3, 0], [2, 0]]
                        ],
                    },
                },
            ]
        },
        {
            "features": [
                {
                    "properties": {
                        "OBJECTID": 3,
                        "COUNTY": "Dallas",
                        "GIS_ACCT": "A102",
                        "AREA_FEET": 21780,
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [[4, 0], [4, 1], [5, 1], [5, 0], [4, 0]]
                        ],
                    },
                }
            ]
        },
    ]

    def fake_urlopen(request, timeout=30.0):  # noqa: ANN001
        requested_urls.append(request.full_url)
        return _FakeResponse(payloads[len(requested_urls) - 1])

    monkeypatch.setattr(connector_module, "urlopen", fake_urlopen)

    result = fetch_connector_records(definition)

    assert len(requested_urls) == 2
    assert "resultOffset=0" in requested_urls[0]
    assert "resultRecordCount=2" in requested_urls[0]
    assert "resultOffset=2" in requested_urls[1]
    assert result.attempt_count == 1
    assert [record["parcel_id"] for record in result.records] == [
        "DFW:48113:A100",
        "DFW:48113:A101",
        "DFW:48113:A102",
    ]
    assert result.records[0]["acreage"] == "1"
    assert result.records[1]["acreage"] == "2"
    assert result.records[2]["lineage_key"] == "parcel:DFW:48113:A102"
    assert result.records[0]["geometry_wkt"].startswith("POLYGON")


def test_refresh_source_connector_expands_zoning_overlay_records_to_parcels(
    db_session: Session,
) -> None:
    _seed_reference_catalogs(db_session)
    ingest_parcel_records(
        session=db_session,
        metro_id="DFW",
        source_version="seed-parcels-v1",
        loaded_at=datetime(2026, 4, 18, 12, 0, tzinfo=UTC),
        records=[
            {
                "parcel_id": "DFW:48113:D100",
                "county_fips": "48113",
                "apn": "D100",
                "acreage": "1.0",
                "geometry_wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                "lineage_key": "parcel:DFW:48113:D100",
            },
            {
                "parcel_id": "DFW:48113:D101",
                "county_fips": "48113",
                "apn": "D101",
                "acreage": "1.0",
                "geometry_wkt": "POLYGON ((2 0, 2 1, 3 1, 3 0, 2 0))",
                "lineage_key": "parcel:DFW:48113:D101",
            },
            {
                "parcel_id": "DFW:48085:C200",
                "county_fips": "48085",
                "apn": "C200",
                "acreage": "1.0",
                "geometry_wkt": "POLYGON ((4 0, 4 1, 5 1, 5 0, 4 0))",
                "lineage_key": "parcel:DFW:48085:C200",
            },
        ],
    )

    config_path = _make_workspace_temp_dir() / "overlay_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "dfw_zoning_overlay_test",
                        "source_id": "ZONING",
                        "metro_id": "DFW",
                        "interface_name": "overlay_fixture_v1",
                        "adapter_type": "fixture",
                        "enabled": True,
                        "preprocess_strategy": "zoning_overlay_to_parcels",
                        "field_map": {
                            "county_fips": "county_fips",
                            "zoning_code": "zoning_code",
                            "land_use_code": "land_use_code",
                            "geometry_wkt": "geometry_wkt",
                            "lineage_key": "lineage_key"
                        },
                        "fixture_records": [
                            {
                                "county_fips": "48113",
                                "zoning_code": "LI",
                                "land_use_code": "LIGHT INDUSTRIAL",
                                "geometry_wkt": "POLYGON ((-1 -1, -1 2, 6 2, 6 -1, -1 -1))",
                                "lineage_key": "zoning-overlay:li"
                            }
                        ]
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    registry = load_connector_registry(str(config_path))

    report = refresh_source_connector(
        db_session,
        registry,
        source_id="ZONING",
        metro_id="DFW",
        actor_name="Test Operator",
        requested_at=datetime(2026, 4, 18, 13, 0, tzinfo=UTC),
    )
    active_zoning = db_session.scalars(
        select(RawZoning)
        .where(RawZoning.metro_id == "DFW", RawZoning.is_active.is_(True))
        .order_by(RawZoning.parcel_id)
    ).all()

    assert report.status == "success"
    assert report.row_count == 2
    assert report.accepted_count == 2
    assert [row.parcel_id for row in active_zoning] == [
        "DFW:48113:D100",
        "DFW:48113:D101",
    ]
    assert all(row.county_fips == "48113" for row in active_zoning)
    assert all(row.zoning_code == "LI" for row in active_zoning)
    assert all(row.lineage_key.startswith("zoning-overlay:li:") for row in active_zoning)


def _seed_reference_catalogs(session: Session) -> None:
    session.add(MetroCatalog(metro_id="DFW", display_name="Dallas-Fort Worth", state_code="TX"))
    session.add_all(
        [
            CountyCatalog(
                county_fips="48085",
                metro_id="DFW",
                display_name="Collin",
                state_code="TX",
            ),
            CountyCatalog(
                county_fips="48113",
                metro_id="DFW",
                display_name="Dallas",
                state_code="TX",
            ),
        ]
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


def _make_workspace_temp_dir() -> Path:
    temp_dir = Path("temp") / f"connector-live-tests-{uuid4().hex}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir
