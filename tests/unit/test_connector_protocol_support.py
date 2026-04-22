from __future__ import annotations

import json
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4
from zipfile import ZipFile

from openpyxl import Workbook
from pyproj import CRS, Transformer
import shapefile
from shapely.geometry import Polygon
from shapely import wkt as shapely_wkt
from sqlalchemy import select
from sqlalchemy.orm import Session

import ingestion.connectors as connector_module
from app.db.models.catalogs import SourceCatalog
from app.db.models.ingestion import SourceSnapshot
from app.db.models.market import ListingSourceCatalog, MarketListing
from app.db.models.source_data import SourceEvidence
from app.db.models.territory import CountyCatalog, MetroCatalog, RawParcel
from ingestion.connectors import fetch_connector_records, load_connector_registry
from ingestion.refresh import refresh_source_connector


class _FakeResponse:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        return False

    def read(self) -> bytes:
        return self._payload


def test_fetch_connector_records_supports_post_json_and_query_auth(monkeypatch) -> None:
    config_path = _make_workspace_temp_dir() / "post_json_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "post_json_test",
                        "source_id": "UTILITY",
                        "metro_id": "DFW",
                        "interface_name": "post_json_test_v1",
                        "adapter_type": "http_json",
                        "enabled": True,
                        "request": {
                            "endpoint_url": "https://example.test/api/resources",
                            "method": "POST",
                            "auth_env_var": "POST_JSON_TEST_TOKEN",
                            "auth_query_param_name": "api_key",
                            "json_body": {"query": "substations"},
                        },
                        "field_rules": [
                            {
                                "target": "record_key",
                                "source": "id",
                                "transform": "strip",
                            },
                            {
                                "target": "attribute_name",
                                "default": "name",
                            },
                            {
                                "target": "attribute_value",
                                "source": "name",
                                "transform": "strip",
                            },
                            {
                                "target": "lineage_key",
                                "template": "utility:{id}:name",
                                "transform": "template",
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    registry = load_connector_registry(str(config_path))
    definition = registry.get_definition_by_connector_key("post_json_test")
    captured: dict[str, object] = {}

    def fake_urlopen(request, timeout=30.0):  # noqa: ANN001
        captured["method"] = request.get_method()
        captured["url"] = request.full_url
        captured["data"] = request.data.decode("utf-8")
        return _FakeResponse(json.dumps([{"id": "rec-001", "name": "Alpha"}]).encode("utf-8"))

    monkeypatch.setenv("POST_JSON_TEST_TOKEN", "secret-token")
    monkeypatch.setattr(connector_module, "urlopen", fake_urlopen)

    result = fetch_connector_records(definition)

    assert captured["method"] == "POST"
    assert "api_key=secret-token" in str(captured["url"])
    assert json.loads(str(captured["data"])) == {"query": "substations"}
    assert result.records == [
        {
            "record_key": "rec-001",
            "attribute_name": "name",
            "attribute_value": "Alpha",
            "lineage_key": "utility:rec-001:name",
        }
    ]


def test_fetch_connector_records_supports_direct_field_keys_with_dots() -> None:
    config_path = _make_workspace_temp_dir() / "direct_key_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "direct_key_test",
                        "source_id": "PARCEL",
                        "metro_id": "DFW",
                        "interface_name": "direct_key_test_v1",
                        "adapter_type": "fixture",
                        "enabled": True,
                        "field_rules": [
                            {
                                "target": "parcel_id",
                                "default": "DENTON-1",
                            },
                            {
                                "target": "county_fips",
                                "default": "48121",
                            },
                            {
                                "target": "acreage",
                                "source": "Shape.STArea()",
                                "transform": "strip",
                            },
                            {
                                "target": "geometry_wkt",
                                "default": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                            },
                            {
                                "target": "lineage_key",
                                "default": "parcel:DENTON-1",
                            },
                        ],
                        "fixture_records": [
                            {
                                "Shape.STArea()": "17996.91736025651",
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    registry = load_connector_registry(str(config_path))
    definition = registry.get_definition_by_connector_key("direct_key_test")

    result = fetch_connector_records(definition)

    assert result.records == [
        {
            "parcel_id": "DENTON-1",
            "county_fips": "48121",
            "acreage": "17996.91736025651",
            "geometry_wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
            "lineage_key": "parcel:DENTON-1",
        }
    ]


def test_fetch_connector_records_supports_acreage_text_or_square_feet_transform() -> None:
    config_path = _make_workspace_temp_dir() / "acreage_transform_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "acreage_transform_test",
                        "source_id": "PARCEL",
                        "metro_id": "DFW",
                        "interface_name": "acreage_transform_test_v1",
                        "adapter_type": "fixture",
                        "enabled": True,
                        "field_rules": [
                            {
                                "target": "acreage",
                                "source": [
                                    "RecAcs",
                                    "__geometry_native_area__",
                                ],
                                "transform": "acreage_text_or_square_feet",
                            },
                        ],
                        "fixture_records": [
                            {
                                "RecAcs": "1.075 Acre",
                                "__geometry_native_area__": 43560.0,
                            },
                            {
                                "RecAcs": "No Stated Area Acre US",
                                "__geometry_native_area__": 43560.0,
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    registry = load_connector_registry(str(config_path))
    definition = registry.get_definition_by_connector_key("acreage_transform_test")

    result = fetch_connector_records(definition)

    assert result.records == [
        {
            "acreage": "1.075",
        },
        {
            "acreage": "1",
        }
    ]


def test_fetch_connector_records_supports_acres_or_square_feet_by_source_transform() -> None:
    config_path = _make_workspace_temp_dir() / "mixed_unit_acreage_transform_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "mixed_unit_acreage_transform_test",
                        "source_id": "PARCEL",
                        "metro_id": "CRP",
                        "interface_name": "mixed_unit_acreage_transform_test_v1",
                        "adapter_type": "fixture",
                        "enabled": True,
                        "field_rules": [
                            {
                                "target": "acreage",
                                "source": [
                                    "legal_acreage",
                                    "__geometry_native_area__",
                                ],
                                "transform": "acres_or_square_feet_by_source",
                            },
                        ],
                        "fixture_records": [
                            {
                                "legal_acreage": 34,
                                "__geometry_native_area__": 1486118.681640625,
                            },
                            {
                                "legal_acreage": None,
                                "__geometry_native_area__": 43560.0,
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    registry = load_connector_registry(str(config_path))
    definition = registry.get_definition_by_connector_key(
        "mixed_unit_acreage_transform_test"
    )

    result = fetch_connector_records(definition)

    assert result.records == [
        {
            "acreage": "34",
        },
        {
            "acreage": "1",
        },
    ]


def test_fetch_connector_records_supports_cad_mixed_area_to_acres_transform() -> None:
    config_path = _make_workspace_temp_dir() / "cad_mixed_area_transform_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "cad_mixed_area_transform_test",
                        "source_id": "PARCEL",
                        "metro_id": "HOU",
                        "interface_name": "cad_mixed_area_transform_test_v1",
                        "adapter_type": "fixture",
                        "enabled": True,
                        "field_rules": [
                            {
                                "target": "acreage",
                                "source": [
                                    "Acreage",
                                    "StatedArea",
                                    "__geometry_native_area__",
                                ],
                                "transform": "cad_mixed_area_to_acres",
                            },
                        ],
                        "fixture_records": [
                            {
                                "HCAD_NUM": "9700000000278",
                                "LOWPARCELI": "9700000000278",
                                "Acreage": "10.2100 AC",
                                "StatedArea": "10.3",
                                "__geometry_native_area__": 448668.0,
                            },
                            {
                                "HCAD_NUM": "9700000000230",
                                "LOWPARCELI": "9700000000230",
                                "Acreage": "",
                                "StatedArea": "5267880",
                                "__geometry_native_area__": 5267880.0,
                            },
                            {
                                "HCAD_NUM": "0650910030115",
                                "LOWPARCELI": "0650910030115",
                                "Acreage": "0650910030012",
                                "StatedArea": "",
                                "__geometry_native_area__": 43560.0,
                            },
                            {
                                "HCAD_NUM": "1457070010065",
                                "LOWPARCELI": "1457070010065",
                                "Acreage": "",
                                "StatedArea": "1457070010063",
                                "__geometry_native_area__": 100000.0,
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    registry = load_connector_registry(str(config_path))
    definition = registry.get_definition_by_connector_key(
        "cad_mixed_area_transform_test"
    )

    result = fetch_connector_records(definition)

    assert result.records == [
        {"acreage": "10.21"},
        {"acreage": "120.933884"},
        {"acreage": "1"},
        {"acreage": "2.295684"},
    ]


def test_fetch_connector_records_parses_zip_csv_download(monkeypatch) -> None:
    config_path = _make_workspace_temp_dir() / "zip_csv_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "zip_csv_test",
                        "source_id": "MARKET",
                        "metro_id": "TX",
                        "interface_name": "zip_csv_test_v1",
                        "adapter_type": "http_zip_csv",
                        "enabled": True,
                        "request": {
                            "endpoint_url": "https://example.test/data.zip",
                            "csv_delimiter": ",",
                        },
                        "field_rules": [
                            {
                                "target": "record_key",
                                "source": "site_id",
                                "transform": "strip",
                            },
                            {
                                "target": "attribute_name",
                                "default": "city",
                            },
                            {
                                "target": "attribute_value",
                                "source": "city",
                                "transform": "strip",
                            },
                            {
                                "target": "lineage_key",
                                "template": "market:{site_id}:city",
                                "transform": "template",
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    registry = load_connector_registry(str(config_path))
    definition = registry.get_definition_by_connector_key("zip_csv_test")
    csv_buffer = BytesIO()
    with ZipFile(csv_buffer, "w") as archive:
        archive.writestr("sites.csv", "site_id,city\nS-100,Dallas\nS-200,Houston\n")

    monkeypatch.setattr(
        connector_module,
        "urlopen",
        lambda request, timeout=30.0: _FakeResponse(csv_buffer.getvalue()),
    )

    result = fetch_connector_records(definition)

    assert [item["record_key"] for item in result.records] == ["S-100", "S-200"]
    assert result.records[1]["attribute_value"] == "Houston"


def test_fetch_connector_records_parses_xlsx_download(monkeypatch) -> None:
    config_path = _make_workspace_temp_dir() / "xlsx_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "xlsx_test",
                        "source_id": "MARKET",
                        "metro_id": "TX",
                        "interface_name": "xlsx_test_v1",
                        "adapter_type": "http_xlsx",
                        "enabled": True,
                        "request": {
                            "endpoint_url": "https://example.test/data.xlsx",
                            "xlsx_sheet_name": "High Cost Areas",
                        },
                        "row_filters": [
                            {
                                "source": "State FIPS",
                                "operator": "equals",
                                "value": "48",
                            }
                        ],
                        "field_rules": [
                            {
                                "target": "record_key",
                                "source": "Census Block Group",
                                "transform": "strip",
                            },
                            {
                                "target": "attribute_name",
                                "default": "state_name",
                            },
                            {
                                "target": "attribute_value",
                                "source": "State Name",
                                "transform": "strip",
                            },
                            {
                                "target": "lineage_key",
                                "template": "market:{Census Block Group}:state_name",
                                "transform": "template",
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    registry = load_connector_registry(str(config_path))
    definition = registry.get_definition_by_connector_key("xlsx_test")
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "High Cost Areas"
    worksheet.append(["State FIPS", "State Name", "Census Block Group"])
    worksheet.append(["48", "Texas", "481130001001"])
    worksheet.append(["06", "California", "060010001001"])
    workbook_buffer = BytesIO()
    workbook.save(workbook_buffer)

    monkeypatch.setattr(
        connector_module,
        "urlopen",
        lambda request, timeout=30.0: _FakeResponse(workbook_buffer.getvalue()),
    )

    result = fetch_connector_records(definition)

    assert result.records == [
        {
            "record_key": "481130001001",
            "attribute_name": "state_name",
            "attribute_value": "Texas",
            "lineage_key": "market:481130001001:state_name",
        }
    ]


def test_fetch_connector_records_parses_ercot_dam_spp_html_with_dated_fallback(
    monkeypatch,
) -> None:
    config_path = _make_workspace_temp_dir() / "ercot_dam_spp_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "ercot_dam_spp_test",
                        "source_id": "IF-013",
                        "metro_id": "TX",
                        "interface_name": "if-013-ercot-dam-spp-html-v1",
                        "adapter_type": "ercot_dam_spp_html",
                        "enabled": True,
                        "request": {
                            "endpoint_url": "https://ercot.test/dam_spp",
                            "text_encoding": "utf-8",
                        },
                        "preprocess_options": {
                            "fallback_days": 1,
                        },
                        "field_rules": [
                            {
                                "target": "record_key",
                                "transform": "template",
                                "template": "ercot:{Oper Day}:{Hour Ending}",
                            },
                            {
                                "target": "attribute_name",
                                "default": "hb_houston",
                            },
                            {
                                "target": "attribute_value",
                                "source": "HB_HOUSTON",
                                "transform": "strip",
                            },
                            {
                                "target": "lineage_key",
                                "transform": "template",
                                "template": "ercot:{Oper Day}:{Hour Ending}:hb_houston",
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    registry = load_connector_registry(str(config_path))
    definition = registry.get_definition_by_connector_key("ercot_dam_spp_test")
    requested_urls: list[str] = []
    current_page = """
    <html>
      <body>
        <input type="hidden" id="currentDate" value="04/20/2026" />
        <p class="dataUnavailable">Check back later.</p>
      </body>
    </html>
    """
    dated_page = """
    <html>
      <body>
        <table class="tableStyle">
          <tr>
            <th>Oper Day</th>
            <th>Hour Ending</th>
            <th>HB_HOUSTON</th>
          </tr>
          <tr>
            <td>04/19/2026</td>
            <td>01</td>
            <td>25.42</td>
          </tr>
        </table>
      </body>
    </html>
    """

    def fake_urlopen(request, timeout=30.0):  # noqa: ANN001
        url = request.full_url
        requested_urls.append(url)
        if url == "https://ercot.test/dam_spp":
            return _FakeResponse(current_page.encode("utf-8"))
        if url == "https://ercot.test/20260419_dam_spp":
            return _FakeResponse(dated_page.encode("utf-8"))
        raise AssertionError(f"Unexpected URL fetched: {url}")

    monkeypatch.setattr(connector_module, "urlopen", fake_urlopen)

    result = fetch_connector_records(definition)

    assert requested_urls == [
        "https://ercot.test/dam_spp",
        "https://ercot.test/20260419_dam_spp",
    ]
    assert result.records == [
        {
            "record_key": "ercot:04/19/2026:01",
            "attribute_name": "hb_houston",
            "attribute_value": "25.42",
            "lineage_key": "ercot:04/19/2026:01:hb_houston",
        }
    ]


def test_fetch_connector_records_builds_usgs_designmaps_grid(monkeypatch) -> None:
    temp_dir = _make_workspace_temp_dir()
    boundary_path = temp_dir / "designmaps_boundary.geojson"
    boundary_path.write_text(
        json.dumps(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [0.0, 0.0],
                            [1.0, 0.0],
                            [1.0, 1.0],
                            [0.0, 1.0],
                            [0.0, 0.0],
                        ]
                    ],
                },
                "properties": {},
            }
        ),
        encoding="utf-8",
    )

    config_path = temp_dir / "usgs_designmaps_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "usgs_designmaps_test",
                        "source_id": "IF-022",
                        "metro_id": "TX",
                        "interface_name": "if-022-usgs-designmaps-grid-v1",
                        "adapter_type": "usgs_designmaps_grid",
                        "enabled": True,
                        "request": {
                            "endpoint_url": "https://example.test/designmaps",
                            "query_params": {
                                "riskCategory": "III",
                                "siteClass": "D",
                            },
                        },
                        "preprocess_options": {
                            "boundary_geojson_path": str(boundary_path),
                            "grid_step_degrees": 1.0,
                            "parallel_requests": 1,
                        },
                        "field_rules": [
                            {
                                "target": "record_key",
                                "source": "point_id",
                                "transform": "strip",
                            },
                            {
                                "target": "attribute_name",
                                "default": "pgam",
                            },
                            {
                                "target": "attribute_value",
                                "source": "pgam",
                                "transform": "stringify",
                            },
                            {
                                "target": "lineage_key",
                                "transform": "template",
                                "template": "designmaps:{point_id}:pgam",
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    registry = load_connector_registry(str(config_path))
    definition = registry.get_definition_by_connector_key("usgs_designmaps_test")

    def fake_urlopen(request, timeout=30.0):  # noqa: ANN001
        parsed = urlsplit(request.full_url)
        query = parse_qs(parsed.query)
        assert query["latitude"] == ["0.500000"]
        assert query["longitude"] == ["0.500000"]
        assert query["riskCategory"] == ["III"]
        assert query["siteClass"] == ["D"]
        payload = {
            "request": {
                "referenceDocument": "ASCE7-22",
            },
            "response": {
                "data": {
                    "pgam": 0.062,
                }
            },
        }
        return _FakeResponse(json.dumps(payload).encode("utf-8"))

    monkeypatch.setattr(connector_module, "urlopen", fake_urlopen)

    result = fetch_connector_records(definition)

    assert result.records == [
        {
            "record_key": "0.500000,0.500000",
            "attribute_name": "pgam",
            "attribute_value": "0.062",
            "lineage_key": "designmaps:0.500000,0.500000:pgam",
        }
    ]


def test_fetch_connector_records_supports_trueprodigy_public_parcels(monkeypatch) -> None:
    config_path = _make_workspace_temp_dir() / "trueprodigy_public_parcels.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "trueprodigy_public_parcels_test",
                        "source_id": "IF-041",
                        "metro_id": "MFE",
                        "interface_name": "if-041-trueprodigy-public-parcels-v1",
                        "adapter_type": "trueprodigy_public_parcels",
                        "enabled": True,
                        "request": {
                            "endpoint_url": "https://example.test",
                            "timeout_seconds": 30.0,
                        },
                        "fetch_policy": {
                            "max_attempts": 2,
                            "backoff_seconds": 0.0,
                        },
                        "preprocess_options": {
                            "office": "Hidalgo",
                            "token_endpoint_url": "https://example.test/trueprodigy/cadpublic/auth/token",
                            "tile_size_degrees": 0.05,
                            "pid_batch_size": 2,
                            "parallel_requests": 2,
                        },
                        "static_fields": {
                            "county_fips": "48215",
                        },
                        "field_rules": [
                            {
                                "target": "apn",
                                "source": ["geoID", "pID"],
                                "transform": "strip",
                            },
                            {
                                "target": "parcel_id",
                                "transform": "template",
                                "template": "MFE:{county_fips}:{apn}",
                            },
                            {
                                "target": "acreage",
                                "source": "tp_area",
                                "transform": "strip",
                            },
                            {
                                "target": "geometry_wkt",
                                "source": "__geometry__",
                                "transform": "geojson_to_wkt",
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    registry = load_connector_registry(str(config_path))
    definition = registry.get_definition_by_connector_key("trueprodigy_public_parcels_test")

    county_boundary_payload = {
        "results": [
            {
                "row_to_json": {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {
                                "type": "Polygon",
                                "coordinates": [
                                    [
                                        [0.0, 0.0],
                                        [0.1, 0.0],
                                        [0.1, 0.1],
                                        [0.0, 0.1],
                                        [0.0, 0.0],
                                    ]
                                ],
                            },
                            "properties": {"county": "Hidalgo"},
                        }
                    ],
                }
            }
        ]
    }

    def _tile_feature_collection(*features: dict[str, object]) -> dict[str, object]:
        return {
            "results": [
                [
                    {
                        "type": "FeatureCollection",
                        "features": list(features),
                    }
                ]
            ]
        }

    def fake_urlopen(request, timeout=30.0):  # noqa: ANN001
        parsed = urlsplit(request.full_url)

        if parsed.path == "/trueprodigy/cadpublic/auth/token":
            assert request.headers["User-agent"].startswith("DenseDataCenterLocator/")
            assert request.headers["Accept"] == "application/json, text/plain, */*"
            return _FakeResponse(
                json.dumps({"user": {"token": "public-token"}}).encode("utf-8")
            )

        if parsed.path == "/gama/countylines/geojson":
            assert request.headers["Authorization"] == "public-token"
            assert request.headers["User-agent"].startswith("DenseDataCenterLocator/")
            assert request.headers["Accept"] == "application/json, text/plain, */*"
            return _FakeResponse(json.dumps(county_boundary_payload).encode("utf-8"))

        if parsed.path == "/gama/parcelswithinbounds":
            ring = json.loads(parse_qs(parsed.query)["points"][0])
            min_x = min(point[0] for point in ring)
            min_y = min(point[1] for point in ring)
            if min_x == 0.0 and min_y == 0.0:
                payload = _tile_feature_collection(
                    {
                        "id": 1001,
                        "type": "Feature",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [0.01, 0.01],
                                    [0.02, 0.01],
                                    [0.02, 0.02],
                                    [0.01, 0.02],
                                    [0.01, 0.01],
                                ]
                            ],
                        },
                        "properties": {
                            "pid": 1001,
                            "county": "Hidalgo",
                            "tp_area": 1.25,
                        },
                    },
                    {
                        "id": 1002,
                        "type": "Feature",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [0.03, 0.03],
                                    [0.04, 0.03],
                                    [0.04, 0.04],
                                    [0.03, 0.04],
                                    [0.03, 0.03],
                                ]
                            ],
                        },
                        "properties": {
                            "pid": 1002,
                            "county": "Hidalgo",
                            "tp_area": 2.5,
                        },
                    },
                )
            elif min_x == 0.05 and min_y == 0.0:
                payload = _tile_feature_collection(
                    {
                        "id": 1002,
                        "type": "Feature",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [0.051, 0.01],
                                    [0.06, 0.01],
                                    [0.06, 0.02],
                                    [0.051, 0.02],
                                    [0.051, 0.01],
                                ]
                            ],
                        },
                        "properties": {
                            "pid": 1002,
                            "county": "Hidalgo",
                            "tp_area": 2.5,
                        },
                    },
                    {
                        "id": 1003,
                        "type": "Feature",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [0.07, 0.01],
                                    [0.08, 0.01],
                                    [0.08, 0.02],
                                    [0.07, 0.02],
                                    [0.07, 0.01],
                                ]
                            ],
                        },
                        "properties": {
                            "pid": 1003,
                            "county": "Hidalgo",
                            "tp_area": 3.75,
                        },
                    },
                )
            else:
                payload = _tile_feature_collection()
            return _FakeResponse(json.dumps(payload).encode("utf-8"))

        if parsed.path == "/gama/appraisalfields/public":
            payload = json.loads(request.data.decode("utf-8"))
            pids = payload["pIDList"]
            rows = []
            for pid in pids:
                rows.append(
                    {
                        "pID": pid,
                        "geoID": f"GEO-{pid}",
                        "owner": f"Owner {pid}",
                    }
                )
            return _FakeResponse(json.dumps({"results": rows}).encode("utf-8"))

        raise AssertionError(f"Unexpected request URL: {request.full_url}")

    monkeypatch.setattr(connector_module, "urlopen", fake_urlopen)

    result = fetch_connector_records(definition)

    assert [record["parcel_id"] for record in result.records] == [
        "MFE:48215:GEO-1001",
        "MFE:48215:GEO-1002",
        "MFE:48215:GEO-1003",
    ]
    assert [record["acreage"] for record in result.records] == ["1.25", "2.5", "3.75"]
    polygon = shapely_wkt.loads(result.records[0]["geometry_wkt"])
    assert round(polygon.centroid.x, 3) == 0.015
    assert round(polygon.centroid.y, 3) == 0.015


def test_fetch_connector_records_trueprodigy_refreshes_public_token_after_401(
    monkeypatch,
) -> None:
    config_path = _make_workspace_temp_dir() / "trueprodigy_refresh_public_token.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "trueprodigy_public_parcels_refresh_test",
                        "source_id": "IF-035",
                        "metro_id": "HOU",
                        "interface_name": "if-035-trueprodigy-public-parcels-v1",
                        "adapter_type": "trueprodigy_public_parcels",
                        "enabled": True,
                        "request": {
                            "endpoint_url": "https://example.test",
                            "timeout_seconds": 30.0,
                        },
                        "fetch_policy": {
                            "max_attempts": 1,
                            "backoff_seconds": 0.0,
                            "rate_limit_per_minute": 180,
                        },
                        "preprocess_options": {
                            "office": "Montgomery",
                            "token_endpoint_url": "https://example.test/trueprodigy/cadpublic/auth/token",
                            "tile_size_degrees": 0.1,
                            "pid_batch_size": 10,
                        },
                        "static_fields": {
                            "county_fips": "48339",
                        },
                        "field_rules": [
                            {
                                "target": "apn",
                                "source": ["geoID", "pID"],
                                "transform": "strip",
                            },
                            {
                                "target": "parcel_id",
                                "transform": "template",
                                "template": "HOU:{county_fips}:{apn}",
                            },
                            {
                                "target": "acreage",
                                "source": "tp_area",
                                "transform": "strip",
                            },
                            {
                                "target": "geometry_wkt",
                                "source": "__geometry__",
                                "transform": "geojson_to_wkt",
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    registry = load_connector_registry(str(config_path))
    definition = registry.get_definition_by_connector_key(
        "trueprodigy_public_parcels_refresh_test"
    )

    auth_calls = 0

    def fake_urlopen(request, timeout=30.0):  # noqa: ANN001
        nonlocal auth_calls
        parsed = urlsplit(request.full_url)

        if parsed.path == "/trueprodigy/cadpublic/auth/token":
            auth_calls += 1
            return _FakeResponse(
                json.dumps({"user": {"token": f"public-token-{auth_calls}"}}).encode("utf-8")
            )

        if parsed.path == "/gama/countylines/geojson":
            return _FakeResponse(
                json.dumps(
                    {
                        "results": [
                            {
                                "row_to_json": {
                                    "type": "FeatureCollection",
                                    "features": [
                                        {
                                            "type": "Feature",
                                            "geometry": {
                                                "type": "Polygon",
                                                "coordinates": [
                                                    [
                                                        [0.0, 0.0],
                                                        [0.1, 0.0],
                                                        [0.1, 0.1],
                                                        [0.0, 0.1],
                                                        [0.0, 0.0],
                                                    ]
                                                ],
                                            },
                                            "properties": {"county": "Montgomery"},
                                        }
                                    ],
                                }
                            }
                        ]
                    }
                ).encode("utf-8")
            )

        if parsed.path == "/gama/parcelswithinbounds":
            return _FakeResponse(
                json.dumps(
                    {
                        "results": [
                            [
                                {
                                    "type": "FeatureCollection",
                                    "features": [
                                        {
                                            "id": 2001,
                                            "type": "Feature",
                                            "geometry": {
                                                "type": "Polygon",
                                                "coordinates": [
                                                    [
                                                        [0.01, 0.01],
                                                        [0.02, 0.01],
                                                        [0.02, 0.02],
                                                        [0.01, 0.02],
                                                        [0.01, 0.01],
                                                    ]
                                                ],
                                            },
                                            "properties": {
                                                "pid": 2001,
                                                "county": "Montgomery",
                                                "tp_area": 4.5,
                                            },
                                        }
                                    ],
                                }
                            ]
                        ]
                    }
                ).encode("utf-8")
            )

        if parsed.path == "/gama/appraisalfields/public":
            if request.headers["Authorization"] == "public-token-1":
                raise HTTPError(
                    request.full_url,
                    401,
                    "UNAUTHORIZED",
                    hdrs=None,
                    fp=BytesIO(b'{"error":"expired"}'),
                )
            return _FakeResponse(
                json.dumps(
                    {
                        "results": [
                            {
                                "pID": 2001,
                                "geoID": "MCAD-2001",
                            }
                        ]
                    }
                ).encode("utf-8")
            )

        raise AssertionError(f"Unexpected request URL: {request.full_url}")

    monkeypatch.setattr(connector_module, "urlopen", fake_urlopen)

    result = fetch_connector_records(definition)

    assert auth_calls == 2
    assert result.records == [
        {
            "county_fips": "48339",
            "apn": "MCAD-2001",
            "parcel_id": "HOU:48339:MCAD-2001",
            "acreage": "4.5",
            "geometry_wkt": "POLYGON ((0.01 0.01, 0.02 0.01, 0.02 0.02, 0.01 0.02, 0.01 0.01))",
        }
    ]


def test_fetch_connector_records_parses_zip_shapefile_download(monkeypatch) -> None:
    config_path = _make_workspace_temp_dir() / "zip_shapefile_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "zip_shapefile_test",
                        "source_id": "UTILITY",
                        "metro_id": "TX",
                        "interface_name": "zip_shapefile_test_v1",
                        "adapter_type": "http_zip_shapefile",
                        "enabled": True,
                        "request": {
                            "endpoint_url": "https://example.test/facilities.zip",
                        },
                        "field_rules": [
                            {
                                "target": "record_key",
                                "source": "NAME",
                                "transform": "strip",
                            },
                            {
                                "target": "attribute_name",
                                "default": "geometry_wkt",
                            },
                            {
                                "target": "attribute_value",
                                "source": "__geometry__",
                                "transform": "geojson_to_wkt",
                            },
                            {
                                "target": "lineage_key",
                                "template": "utility:{NAME}:geometry",
                                "transform": "template",
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    registry = load_connector_registry(str(config_path))
    definition = registry.get_definition_by_connector_key("zip_shapefile_test")
    shapefile_zip = _build_shapefile_zip_bytes()

    monkeypatch.setattr(
        connector_module,
        "urlopen",
        lambda request, timeout=30.0: _FakeResponse(shapefile_zip),
    )

    result = fetch_connector_records(definition)

    assert result.records[0]["record_key"] == "Switchyard A"
    assert result.records[0]["attribute_value"].startswith("POINT")


def test_fetch_connector_records_skips_null_shapes_in_zip_shapefile_download(
    monkeypatch,
) -> None:
    config_path = _make_workspace_temp_dir() / "zip_shapefile_null_shapes_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "zip_shapefile_null_shapes_test",
                        "source_id": "UTILITY",
                        "metro_id": "TX",
                        "interface_name": "zip_shapefile_null_shapes_test_v1",
                        "adapter_type": "http_zip_shapefile",
                        "enabled": True,
                        "request": {
                            "endpoint_url": "https://example.test/facilities_null_shapes.zip",
                        },
                        "field_rules": [
                            {
                                "target": "record_key",
                                "source": "NAME",
                                "transform": "strip",
                            },
                            {
                                "target": "attribute_name",
                                "default": "geometry_wkt",
                            },
                            {
                                "target": "attribute_value",
                                "source": "__geometry__",
                                "transform": "geojson_to_wkt",
                            },
                            {
                                "target": "lineage_key",
                                "template": "utility:{NAME}:geometry",
                                "transform": "template",
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    registry = load_connector_registry(str(config_path))
    definition = registry.get_definition_by_connector_key("zip_shapefile_null_shapes_test")
    shapefile_zip = _build_shapefile_zip_with_null_shape_bytes()

    monkeypatch.setattr(
        connector_module,
        "urlopen",
        lambda request, timeout=30.0: _FakeResponse(shapefile_zip),
    )

    result = fetch_connector_records(definition)

    assert result.records == [
        {
            "record_key": "Switchyard B",
            "attribute_name": "geometry_wkt",
            "attribute_value": "POINT (-96.81 32.81)",
            "lineage_key": "utility:Switchyard B:geometry",
        }
    ]


def test_fetch_connector_records_reprojects_zip_shapefile_geometry_when_prj_present(
    monkeypatch,
) -> None:
    config_path = _make_workspace_temp_dir() / "zip_shapefile_projected_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "zip_shapefile_projected_test",
                        "source_id": "UTILITY",
                        "metro_id": "TX",
                        "interface_name": "zip_shapefile_projected_test_v1",
                        "adapter_type": "http_zip_shapefile",
                        "enabled": True,
                        "request": {
                            "endpoint_url": "https://example.test/facilities_projected.zip",
                        },
                        "field_rules": [
                            {
                                "target": "record_key",
                                "source": "NAME",
                                "transform": "strip",
                            },
                            {
                                "target": "attribute_name",
                                "default": "geometry_wkt",
                            },
                            {
                                "target": "attribute_value",
                                "source": "__geometry__",
                                "transform": "geojson_to_wkt",
                            },
                            {
                                "target": "lineage_key",
                                "template": "utility:{NAME}:geometry",
                                "transform": "template",
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    registry = load_connector_registry(str(config_path))
    definition = registry.get_definition_by_connector_key("zip_shapefile_projected_test")
    shapefile_zip = _build_projected_shapefile_zip_bytes()

    monkeypatch.setattr(
        connector_module,
        "urlopen",
        lambda request, timeout=30.0: _FakeResponse(shapefile_zip),
    )

    result = fetch_connector_records(definition)
    point = shapely_wkt.loads(result.records[0]["attribute_value"])

    assert round(point.x, 1) == -96.8
    assert round(point.y, 1) == 32.8


def test_fetch_connector_records_reprojects_arcgis_json_geometry_and_preserves_native_area(
    monkeypatch,
) -> None:
    config_path = _make_workspace_temp_dir() / "arcgis_json_projected_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "arcgis_projected_test",
                        "source_id": "PARCEL",
                        "metro_id": "SAT",
                        "interface_name": "arcgis_projected_test_v1",
                        "adapter_type": "arcgis_feature_service",
                        "enabled": True,
                        "request": {
                            "endpoint_url": "https://example.test/arcgis/rest/services/Parcels/MapServer/0",
                            "query_params": {
                                "where": "OBJECTID > 0",
                                "outFields": "OBJECTID,PROP_ID",
                                "returnGeometry": "true",
                                "f": "json",
                            },
                        },
                        "field_rules": [
                            {
                                "target": "parcel_id",
                                "transform": "template",
                                "template": "SAT:48029:{PROP_ID}",
                            },
                            {
                                "target": "acreage",
                                "source": "__geometry_native_area__",
                                "transform": "square_feet_to_acres",
                            },
                            {
                                "target": "geometry_wkt",
                                "source": "__geometry__",
                                "transform": "geojson_to_wkt",
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    registry = load_connector_registry(str(config_path))
    definition = registry.get_definition_by_connector_key("arcgis_projected_test")
    payload, expected_acreage = _build_projected_arcgis_polygon_payload()

    monkeypatch.setattr(
        connector_module,
        "urlopen",
        lambda request, timeout=30.0: _FakeResponse(json.dumps(payload).encode("utf-8")),
    )

    result = fetch_connector_records(definition)
    polygon = shapely_wkt.loads(result.records[0]["geometry_wkt"])

    assert result.records[0]["parcel_id"] == "SAT:48029:BCAD-1001"
    assert result.records[0]["acreage"] == expected_acreage
    assert round(polygon.centroid.x, 4) == -98.4936
    assert round(polygon.centroid.y, 4) == 29.4243


def test_fetch_connector_records_parallelizes_arcgis_offset_pages(monkeypatch) -> None:
    config_path = _make_workspace_temp_dir() / "arcgis_parallel_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "arcgis_parallel_test",
                        "source_id": "PARCEL",
                        "metro_id": "DFW",
                        "interface_name": "arcgis_parallel_test_v1",
                        "adapter_type": "arcgis_feature_service",
                        "enabled": True,
                        "request": {
                            "endpoint_url": "https://example.test/arcgis/rest/services/Parcels/MapServer/0",
                            "query_params": {
                                "where": "OBJECTID > 0",
                                "outFields": "OBJECTID",
                                "returnGeometry": "false",
                                "f": "json",
                            },
                            "pagination": {
                                "strategy": "arcgis_offset",
                                "page_size": 2,
                                "parallel_requests": 3,
                            },
                        },
                        "field_rules": [
                            {
                                "target": "record_key",
                                "source": "OBJECTID",
                                "transform": "strip",
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    registry = load_connector_registry(str(config_path))
    definition = registry.get_definition_by_connector_key("arcgis_parallel_test")
    requested_offsets: list[int] = []

    def fake_urlopen(request, timeout=30.0):  # noqa: ANN001
        query = parse_qs(urlsplit(request.full_url).query)
        offset = int(query.get("resultOffset", ["0"])[0])
        requested_offsets.append(offset)
        payload_by_offset = {
            0: {
                "features": [
                    {"attributes": {"OBJECTID": "1"}},
                    {"attributes": {"OBJECTID": "2"}},
                ],
                "exceededTransferLimit": True,
            },
            2: {
                "features": [
                    {"attributes": {"OBJECTID": "3"}},
                    {"attributes": {"OBJECTID": "4"}},
                ],
                "exceededTransferLimit": True,
            },
            4: {
                "features": [{"attributes": {"OBJECTID": "5"}}],
                "exceededTransferLimit": False,
            },
            6: {"features": [], "exceededTransferLimit": False},
        }
        return _FakeResponse(json.dumps(payload_by_offset[offset]).encode("utf-8"))

    monkeypatch.setattr(connector_module, "urlopen", fake_urlopen)

    result = fetch_connector_records(definition)

    assert [record["record_key"] for record in result.records] == [
        "1",
        "2",
        "3",
        "4",
        "5",
    ]
    assert requested_offsets[0] == 0
    assert {2, 4}.issubset(requested_offsets)


def test_fetch_connector_records_supports_headered_json_rows(monkeypatch) -> None:
    config_path = _make_workspace_temp_dir() / "headered_json_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "headered_json_test",
                        "source_id": "MARKET",
                        "metro_id": "TX",
                        "interface_name": "headered_json_test_v1",
                        "adapter_type": "http_json",
                        "enabled": True,
                        "field_rules": [
                            {
                                "target": "record_key",
                                "transform": "template",
                                "template": "{state}{county}{tract}",
                            },
                            {
                                "target": "attribute_name",
                                "default": "population",
                            },
                            {
                                "target": "attribute_value",
                                "source": "B01001_001E",
                                "transform": "strip",
                            },
                            {
                                "target": "lineage_key",
                                "transform": "template",
                                "template": "market:{state}{county}{tract}:population",
                            },
                        ],
                        "request": {
                            "endpoint_url": "https://example.test/headered.json",
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    registry = load_connector_registry(str(config_path))
    definition = registry.get_definition_by_connector_key("headered_json_test")

    monkeypatch.setattr(
        connector_module,
        "urlopen",
        lambda request, timeout=30.0: _FakeResponse(
            json.dumps(
                [
                    ["B01001_001E", "state", "county", "tract"],
                    ["4450", "48", "113", "000100"],
                ]
            ).encode("utf-8")
        ),
    )

    result = fetch_connector_records(definition)

    assert result.records == [
        {
            "record_key": "48113000100",
            "attribute_name": "population",
            "attribute_value": "4450",
            "lineage_key": "market:48113000100:population",
        }
    ]


def test_fetch_connector_records_applies_row_filters_before_mapping(monkeypatch) -> None:
    config_path = _make_workspace_temp_dir() / "filtered_csv_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "filtered_csv_test",
                        "source_id": "EMPLOYMENT",
                        "metro_id": "TX",
                        "interface_name": "filtered-csv-test-v1",
                        "adapter_type": "http_csv",
                        "enabled": True,
                        "request": {
                            "endpoint_url": "https://example.test/qcew.csv",
                        },
                        "row_filters": [
                            {
                                "source": "area_fips",
                                "operator": "startswith",
                                "value": "48",
                            },
                            {
                                "source": "agglvl_code",
                                "operator": "equals",
                                "value": "70",
                            },
                        ],
                        "field_rules": [
                            {
                                "target": "record_key",
                                "source": "area_fips",
                                "transform": "strip",
                            },
                            {
                                "target": "attribute_name",
                                "default": "month3_emplvl",
                            },
                            {
                                "target": "attribute_value",
                                "source": "month3_emplvl",
                                "transform": "strip",
                            },
                            {
                                "target": "lineage_key",
                                "transform": "template",
                                "template": "employment:{area_fips}:month3_emplvl",
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    registry = load_connector_registry(str(config_path))
    definition = registry.get_definition_by_connector_key("filtered_csv_test")
    csv_payload = (
        "area_fips,agglvl_code,month3_emplvl\n"
        "48001,70,20748\n"
        "48000,50,14062349\n"
        "06001,70,123456\n"
    ).encode("utf-8")

    monkeypatch.setattr(
        connector_module,
        "urlopen",
        lambda request, timeout=30.0: _FakeResponse(csv_payload),
    )

    result = fetch_connector_records(definition)

    assert result.records == [
        {
            "record_key": "48001",
            "attribute_name": "month3_emplvl",
            "attribute_value": "20748",
            "lineage_key": "employment:48001:month3_emplvl",
        }
    ]


def test_refresh_source_connector_ingests_market_listings_from_html_scrape(
    db_session: Session,
    monkeypatch,
) -> None:
    db_session.add(MetroCatalog(metro_id="DFW", display_name="Dallas-Fort Worth", state_code="TX"))
    db_session.add(
        CountyCatalog(
            county_fips="48113",
            metro_id="DFW",
            display_name="Dallas",
            state_code="TX",
        )
    )
    db_session.add(
        SourceCatalog(
            source_id="LISTING",
            display_name="Market Listings Scraper",
            owner_name="Data Governance",
            refresh_cadence="daily",
            block_refresh=False,
            metro_coverage="TX",
            target_table_name="market_listings",
            is_active=True,
        )
    )
    db_session.add(
        ListingSourceCatalog(
            listing_source_id="public-broker",
            display_name="Public Broker Listings",
            acquisition_method="html_scrape",
            base_url="https://example.test",
            allows_scraping=True,
            is_active=True,
        )
    )
    db_session.commit()

    config_path = _make_workspace_temp_dir() / "market_listing_html_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "dfw_public_broker_html",
                        "source_id": "LISTING",
                        "metro_id": "DFW",
                        "interface_name": "listing-http-html-v1",
                        "adapter_type": "http_html",
                        "enabled": True,
                        "load_strategy": "market_listing",
                        "preprocess_options": {
                            "listing_source_id": "public-broker",
                            "replace_existing_scope": "listing_source_metro",
                        },
                        "request": {
                            "endpoint_url": "https://example.test/listings",
                            "record_pattern": (
                                "<article class=\\\"listing\\\" "
                                "data-id=\\\"(?P<source_listing_key>[^\\\"]+)\\\">"
                                ".*?<a class=\\\"title\\\" href=\\\"(?P<source_url>[^\\\"]+)\\\">"
                                "(?P<listing_title>.*?)</a>.*?<span class=\\\"status\\\">"
                                "(?P<listing_status>.*?)</span>.*?<span class=\\\"asset\\\">"
                                "(?P<asset_type>.*?)</span>.*?<span class=\\\"acreage\\\">"
                                "(?P<acreage>.*?)</span>.*?<span class=\\\"price\\\">"
                                "(?P<asking_price>.*?)</span>.*?<span class=\\\"city\\\">"
                                "(?P<city>.*?)</span>.*?<span class=\\\"county_fips\\\">"
                                "(?P<county_fips>.*?)</span>.*?<span class=\\\"broker\\\">"
                                "(?P<broker_name>.*?)</span>.*?</article>"
                            )
                        },
                        "field_rules": [
                            {
                                "target": "source_listing_key",
                                "source": "source_listing_key",
                                "transform": "strip",
                            },
                            {
                                "target": "listing_title",
                                "source": "listing_title",
                                "transform": "strip",
                            },
                            {
                                "target": "listing_status",
                                "source": "listing_status",
                                "transform": "strip",
                            },
                            {
                                "target": "asset_type",
                                "source": "asset_type",
                                "transform": "strip",
                            },
                            {
                                "target": "acreage",
                                "source": "acreage",
                                "transform": "strip",
                            },
                            {
                                "target": "asking_price",
                                "source": "asking_price",
                                "transform": "strip",
                            },
                            {
                                "target": "city",
                                "source": "city",
                                "transform": "strip",
                            },
                            {
                                "target": "county_fips",
                                "source": "county_fips",
                                "transform": "strip",
                            },
                            {
                                "target": "broker_name",
                                "source": "broker_name",
                                "transform": "strip",
                            },
                            {
                                "target": "source_url",
                                "source": "source_url",
                                "transform": "strip",
                            },
                            {
                                "target": "lineage_key",
                                "transform": "template",
                                "template": "listing:{source_listing_key}",
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    registry = load_connector_registry(str(config_path))
    html_payload = b"""
            <html><body>
              <article class="listing" data-id="tx-001">
                <a class="title" href="https://example.test/listings/tx-001">
                  Dallas Mega Site
                </a>
                <span class="status">for_sale</span>
                <span class="asset">land</span>
                <span class="acreage">120.5</span>
                <span class="price">4500000</span>
                <span class="city">Dallas</span>
                <span class="county_fips">48113</span>
                <span class="broker">North Texas Brokerage</span>
              </article>
              <article class="listing" data-id="tx-002">
                <a class="title" href="https://example.test/listings/tx-002">
                  Dallas Powered Building
                </a>
                <span class="status">for_lease</span>
                <span class="asset">building</span>
                <span class="acreage">4.25</span>
                <span class="price">9800000</span>
                <span class="city">Dallas</span>
                <span class="county_fips">48113</span>
                <span class="broker">North Texas Brokerage</span>
              </article>
            </body></html>
            """
    monkeypatch.setattr(
        connector_module,
        "urlopen",
        lambda request, timeout=30.0: _FakeResponse(html_payload),
    )

    report = refresh_source_connector(
        db_session,
        registry,
        connector_key="dfw_public_broker_html",
        actor_name="test-suite",
        requested_at=datetime(2026, 4, 19, 8, 30, tzinfo=UTC),
    )

    snapshot = db_session.scalar(
        select(SourceSnapshot).where(SourceSnapshot.source_id == "LISTING")
    )
    listings = db_session.scalars(
        select(MarketListing)
        .where(
            MarketListing.listing_source_id == "public-broker",
            MarketListing.metro_id == "DFW",
            MarketListing.is_active.is_(True),
        )
        .order_by(MarketListing.source_listing_key)
    ).all()

    assert report.status == "success"
    assert report.accepted_count == 2
    assert snapshot is not None
    assert snapshot.source_id == "LISTING"
    assert [listing.source_listing_key for listing in listings] == ["tx-001", "tx-002"]
    assert listings[0].asset_type == "land"
    assert str(listings[0].acreage) == "120.5000"


def test_refresh_source_connector_ingests_myelisting_records_from_paginated_html(
    db_session: Session,
    monkeypatch,
) -> None:
    db_session.add(MetroCatalog(metro_id="TX", display_name="Texas Statewide", state_code="TX"))
    db_session.add(
        SourceCatalog(
            source_id="LISTING",
            display_name="Market Listings Scraper",
            owner_name="Data Governance",
            refresh_cadence="daily",
            block_refresh=False,
            metro_coverage="TX",
            target_table_name="market_listings",
            is_active=True,
        )
    )
    db_session.add(
        ListingSourceCatalog(
            listing_source_id="myelisting",
            display_name="MyEListing Texas Sale Feed",
            acquisition_method="html_scrape",
            base_url="https://myelisting.com/properties/for-sale/texas/all-property-types/",
            allows_scraping=True,
            is_active=True,
        )
    )
    db_session.commit()

    config_path = _make_workspace_temp_dir() / "myelisting_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "tx_myelisting_sale_test",
                        "source_id": "LISTING",
                        "metro_id": "TX",
                        "interface_name": "listing-myelisting-html-v1",
                        "adapter_type": "myelisting_html",
                        "enabled": True,
                        "load_strategy": "market_listing",
                        "fetch_policy": {
                            "checkpoint_field": "listing_refreshed",
                        },
                        "preprocess_options": {
                            "listing_source_id": "myelisting",
                            "replace_existing_scope": "listing_source_metro",
                        },
                        "request": {
                            "endpoint_url": "https://myelisting.com/properties/for-sale/texas/all-property-types/",
                            "start_urls": [
                                "https://myelisting.com/properties/for-sale/texas/all-property-types/"
                            ],
                            "pagination": {
                                "strategy": "none",
                                "max_pages": 4,
                                "parallel_requests": 2,
                            },
                        },
                        "field_rules": [
                            {
                                "target": "source_listing_key",
                                "source": "source_listing_key",
                                "transform": "strip",
                            },
                            {
                                "target": "listing_title",
                                "source": "listing_title",
                                "transform": "strip",
                            },
                            {
                                "target": "listing_status",
                                "source": "listing_status",
                                "transform": "strip",
                            },
                            {
                                "target": "asset_type",
                                "source": "asset_type",
                                "transform": "strip",
                            },
                            {
                                "target": "asking_price",
                                "source": "asking_price",
                                "transform": "strip",
                            },
                            {
                                "target": "acreage",
                                "source": "acreage",
                                "transform": "strip",
                            },
                            {
                                "target": "building_sqft",
                                "source": "building_sqft",
                                "transform": "strip",
                            },
                            {
                                "target": "address_line1",
                                "source": "address_line1",
                                "transform": "strip",
                            },
                            {
                                "target": "city",
                                "source": "city",
                                "transform": "strip",
                            },
                            {
                                "target": "state_code",
                                "source": "state_code",
                                "transform": "upper",
                            },
                            {
                                "target": "postal_code",
                                "source": "postal_code",
                                "transform": "strip",
                            },
                            {
                                "target": "latitude",
                                "source": "latitude",
                                "transform": "strip",
                            },
                            {
                                "target": "longitude",
                                "source": "longitude",
                                "transform": "strip",
                            },
                            {
                                "target": "broker_name",
                                "source": "broker_name",
                                "transform": "strip",
                            },
                            {
                                "target": "source_url",
                                "source": "source_url",
                                "transform": "strip",
                            },
                            {
                                "target": "last_verified_at",
                                "source": "listing_refreshed",
                                "transform": "strip",
                            },
                            {
                                "target": "lineage_key",
                                "transform": "template",
                                "template": "listing:myelisting:{source_listing_key}",
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    registry = load_connector_registry(str(config_path))

    search_page_1 = """
    <html>
      <body>
        <script type="application/ld+json" id="pageListings">
          {
            "@context": "http://schema.org",
            "@type": "SearchResultsPage",
            "about": [
              {
                "@type": "ListItem",
                "item": {
                  "@type": "Offer",
                  "name": "1517 East Anderson Lane Austin, TX 78752",
                  "url": "https://myelisting.com/listing/295410/1517-east-anderson-lane-austin-tx-78752/",
                  "price": "2100000"
                },
                "position": 1
              }
            ]
          }
        </script>
      </body>
    </html>
    """
    search_page_2 = """
    <html>
      <body>
        <script type="application/ld+json" id="pageListings">
          {
            "@context": "http://schema.org",
            "@type": "SearchResultsPage",
            "about": [
              {
                "@type": "ListItem",
                "item": {
                  "@type": "Offer",
                  "name": "16815 Royal Crest Drive Houston, TX 77058",
                  "url": "https://myelisting.com/listing/316815/16815-royal-crest-drive-houston-tx-77058/",
                  "price": ""
                },
                "position": 1
              }
            ]
          }
        </script>
      </body>
    </html>
    """
    detail_page_1 = """
    <html>
      <body>
        <script>
          var pdfdata = {
            "listing": {
              "ID": 295410,
              "listing_lat": "30.33121",
              "listing_lng": "-97.68537",
              "listing_islease": 0,
              "listing_title": "1517 East Anderson Lane",
              "listing_refreshed": "2026-04-14 15:37:32",
              "listing_address": "1517 East Anderson Lane",
              "listing_city": "Austin",
              "listing_state": "TX",
              "listing_zip": "78752",
              "call_price": 0,
              "price": 2100000,
              "build_sf": 0,
              "lot_acre": "3.580",
              "listing_type": "sale",
              "proptype": "Commercial Land"
            },
            "agents": [
              {
                "user_firstname": "Megan",
                "user_lastname": "Ford",
                "office_name": "Brinegar Properties CRE"
              }
            ]
          };
          var vcpt = 80;
        </script>
      </body>
    </html>
    """
    detail_page_2 = """
    <html>
      <body>
        <h1>Fallback Listing</h1>
      </body>
    </html>
    """
    search_page_after_last = """
    <html>
      <body>
        <main>No additional results</main>
      </body>
    </html>
    """

    def fake_urlopen(request, timeout=30.0):  # noqa: ANN001
        url = request.full_url
        if url == "https://myelisting.com/properties/for-sale/texas/all-property-types/":
            return _FakeResponse(search_page_1.encode("utf-8"))
        if url == "https://myelisting.com/properties/for-sale/texas/all-property-types/page-2/":
            return _FakeResponse(search_page_2.encode("utf-8"))
        if url == "https://myelisting.com/properties/for-sale/texas/all-property-types/page-3/":
            return _FakeResponse(search_page_after_last.encode("utf-8"))
        if url == "https://myelisting.com/listing/295410/1517-east-anderson-lane-austin-tx-78752/":
            return _FakeResponse(detail_page_1.encode("utf-8"))
        if url == "https://myelisting.com/listing/316815/16815-royal-crest-drive-houston-tx-77058/":
            return _FakeResponse(detail_page_2.encode("utf-8"))
        raise AssertionError(f"Unexpected URL fetched: {url}")

    monkeypatch.setattr(connector_module, "urlopen", fake_urlopen)

    report = refresh_source_connector(
        db_session,
        registry,
        connector_key="tx_myelisting_sale_test",
        actor_name="test-suite",
        requested_at=datetime(2026, 4, 19, 9, 0, tzinfo=UTC),
    )

    listings = db_session.scalars(
        select(MarketListing)
        .where(
            MarketListing.listing_source_id == "myelisting",
            MarketListing.metro_id == "TX",
            MarketListing.is_active.is_(True),
        )
        .order_by(MarketListing.source_listing_key)
    ).all()

    assert report.status == "success"
    assert report.accepted_count == 2
    assert [listing.source_listing_key for listing in listings] == ["295410", "316815"]
    assert listings[0].asset_type == "commercial land"
    assert str(listings[0].asking_price) == "2100000.00"
    assert str(listings[0].acreage) == "3.5800"
    assert listings[0].broker_name == "Brinegar Properties CRE (Megan Ford)"
    assert listings[1].listing_title == "16815 Royal Crest Drive Houston, TX 77058"
    assert listings[1].state_code == "TX"
    assert listings[1].asking_price is None


def test_fetch_connector_records_paginates_acrevalue_land_listings(monkeypatch) -> None:
    config_path = _make_workspace_temp_dir() / "acrevalue_land_connectors.json"
    field_rules = [
        {"target": field_name, "source": field_name, "transform": "strip"}
        for field_name in (
            "source_listing_key",
            "listing_title",
            "listing_status",
            "asset_type",
            "asking_price",
            "acreage",
            "building_sqft",
            "address_line1",
            "city",
            "state_code",
            "postal_code",
            "latitude",
            "longitude",
            "broker_name",
            "source_url",
        )
    ]
    field_rules.append(
        {
            "target": "last_verified_at",
            "source": "listing_refreshed",
            "transform": "strip",
        }
    )
    field_rules.append(
        {
            "target": "lineage_key",
            "transform": "template",
            "template": "listing:acrevalue:{source_listing_key}",
        }
    )
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "tx_acrevalue_land_test",
                        "source_id": "LISTING",
                        "metro_id": "TX",
                        "interface_name": "listing-acrevalue-land-v1",
                        "adapter_type": "acrevalue_land_listings",
                        "enabled": True,
                        "load_strategy": "market_listing",
                        "request": {
                            "endpoint_url": "https://www.acrevalue.com/listings/api/listings/",
                            "query_params": {
                                "sorting_key": "-ordering_date",
                                "status": "For Sale",
                                "filters": "{\"state\":\"TX\"}",
                            },
                            "pagination": {
                                "strategy": "page_number",
                                "page_size": 2,
                                "parallel_requests": 2,
                            },
                        },
                        "field_rules": field_rules,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    registry = load_connector_registry(str(config_path))
    definition = registry.get_definition_by_connector_key("tx_acrevalue_land_test")
    requested_pages: list[str] = []

    def fake_urlopen(request, timeout=30.0):  # noqa: ANN001
        parsed = urlsplit(request.full_url)
        query = parse_qs(parsed.query)
        assert parsed.path == "/listings/api/listings/"
        requested_pages.append(query["page"][0])
        assert query["data_limit"] == ["2"]
        assert query["filters"] == ['{"state":"TX"}']
        listings = {
            "1": [
                {
                    "listing_id": "27020145",
                    "listing_title": ".88 Acres, Ponderosa, Fritch, TX 79036",
                    "status_classification": "For Sale",
                    "property_types": "Recreational Property,Undeveloped Land",
                    "price": 23789,
                    "acres": 0.88,
                    "address": "Ponderosa",
                    "city": "Fritch",
                    "state": "TX",
                    "zipcode": "79036",
                    "latitude": 35.6237983703613,
                    "longitude": -101.575996398926,
                    "broker": "Land Investor Since 2001",
                    "date_updated": "04/21/2026",
                    "link": "https://www.land.com/property/ponderosa-fritch-texas-79036/27020145/",
                },
                {
                    "listing_id": "27013379",
                    "listing_title": "125 Acres in Gillespie County",
                    "status": "Active",
                    "property_classification": "Farm",
                    "price": 1875000,
                    "acres": 125,
                    "city": "Fredericksburg",
                    "state": "TX",
                    "date_listed": "2026-04-20",
                    "link": "https://www.land.com/property/125-acres-in-gillespie-county-texas/27013379/",
                },
            ],
            "2": [
                {
                    "guid": "guid-003",
                    "listing_title": "20 Acres near Bonham",
                    "status_classification": "For Sale",
                    "acres": "20",
                    "city": "Bonham",
                    "state": "TX",
                    "standalone_link": "/land-for-sale-near-me/bonham-tx/guid-003/",
                }
            ],
        }
        payload = {
            "status": "success",
            "data": {
                "listings": listings[query["page"][0]],
                "listings_count": 3,
            },
        }
        return _FakeResponse(json.dumps(payload).encode("utf-8"))

    monkeypatch.setattr(connector_module, "urlopen", fake_urlopen)

    result = fetch_connector_records(definition)

    assert sorted(requested_pages) == ["1", "2"]
    assert [record["source_listing_key"] for record in result.records] == [
        "27020145",
        "27013379",
        "guid-003",
    ]
    assert result.records[0]["listing_status"] == "sale"
    assert result.records[0]["asset_type"] == "Recreational Property,Undeveloped Land"
    assert result.records[0]["acreage"] == "0.88"
    assert result.records[0]["last_verified_at"] == "2026-04-21T00:00:00+00:00"
    assert result.records[2]["source_url"] == (
        "https://www.acrevalue.com/land-for-sale-near-me/bonham-tx/guid-003/"
    )


def test_refresh_source_connector_ingests_myelisting_records_from_listing_sitemap_xml(
    db_session: Session,
    monkeypatch,
) -> None:
    db_session.add(MetroCatalog(metro_id="TX", display_name="Texas Statewide", state_code="TX"))
    db_session.add(
        SourceCatalog(
            source_id="LISTING",
            display_name="Market Listings Scraper",
            owner_name="Data Governance",
            refresh_cadence="daily",
            block_refresh=False,
            metro_coverage="TX",
            target_table_name="market_listings",
            is_active=True,
        )
    )
    db_session.add(
        ListingSourceCatalog(
            listing_source_id="myelisting",
            display_name="MyEListing Texas Sale Feed",
            acquisition_method="html_scrape",
            base_url="https://myelisting.com/properties/for-sale/texas/all-property-types/",
            allows_scraping=True,
            is_active=True,
        )
    )
    db_session.commit()

    config_path = _make_workspace_temp_dir() / "myelisting_sitemap_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "tx_myelisting_sitemap_test",
                        "source_id": "LISTING",
                        "metro_id": "TX",
                        "interface_name": "listing-myelisting-html-v1",
                        "adapter_type": "myelisting_html",
                        "enabled": True,
                        "load_strategy": "market_listing",
                        "preprocess_options": {
                            "listing_source_id": "myelisting",
                            "replace_existing_scope": "listing_source_metro",
                        },
                        "request": {
                            "endpoint_url": "https://myelisting.com/properties/for-sale/texas/all-property-types/",
                            "record_pattern": "-tx(?:-|/)",
                            "start_urls": [
                                "https://myelisting.com/sitemap.xml"
                            ],
                        },
                        "field_rules": [
                            {
                                "target": "source_listing_key",
                                "source": "source_listing_key",
                                "transform": "strip",
                            },
                            {
                                "target": "listing_title",
                                "source": "listing_title",
                                "transform": "strip",
                            },
                            {
                                "target": "listing_status",
                                "source": "listing_status",
                                "transform": "strip",
                            },
                            {
                                "target": "asset_type",
                                "source": "asset_type",
                                "transform": "strip",
                            },
                            {
                                "target": "asking_price",
                                "source": "asking_price",
                                "transform": "strip",
                            },
                            {
                                "target": "acreage",
                                "source": "acreage",
                                "transform": "strip",
                            },
                            {
                                "target": "building_sqft",
                                "source": "building_sqft",
                                "transform": "strip",
                            },
                            {
                                "target": "address_line1",
                                "source": "address_line1",
                                "transform": "strip",
                            },
                            {
                                "target": "city",
                                "source": "city",
                                "transform": "strip",
                            },
                            {
                                "target": "state_code",
                                "source": "state_code",
                                "transform": "upper",
                            },
                            {
                                "target": "postal_code",
                                "source": "postal_code",
                                "transform": "strip",
                            },
                            {
                                "target": "latitude",
                                "source": "latitude",
                                "transform": "strip",
                            },
                            {
                                "target": "longitude",
                                "source": "longitude",
                                "transform": "strip",
                            },
                            {
                                "target": "broker_name",
                                "source": "broker_name",
                                "transform": "strip",
                            },
                            {
                                "target": "source_url",
                                "source": "source_url",
                                "transform": "strip",
                            },
                            {
                                "target": "last_verified_at",
                                "source": "listing_refreshed",
                                "transform": "strip",
                            },
                            {
                                "target": "lineage_key",
                                "transform": "template",
                                "template": "listing:myelisting:{source_listing_key}",
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    registry = load_connector_registry(str(config_path))
    requested_urls: list[str] = []
    sitemap_index = """
    <?xml version="1.0" encoding="UTF-8"?>
    <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <sitemap>
        <loc>https://myelisting.com/sitemap_main.xml</loc>
      </sitemap>
      <sitemap>
        <loc>https://myelisting.com/sitemap_listings.xml</loc>
      </sitemap>
    </sitemapindex>
    """
    listing_sitemap = """
    <?xml version="1.0" encoding="UTF-8"?>
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <url>
        <loc>https://myelisting.com/listing/295410/1517-east-anderson-lane-austin-tx-78752/</loc>
      </url>
      <url>
        <loc>https://myelisting.com/listing/316815/16815-royal-crest-drive-houston-tx-77058/</loc>
      </url>
      <url>
        <loc>https://myelisting.com/listing/302383/18247-sherman-way-los-angeles-ca-91335/</loc>
      </url>
    </urlset>
    """
    sale_detail_page = """
    <html>
      <body>
        <script>
          var pdfdata = {
            "listing": {
              "ID": 295410,
              "listing_islease": 0,
              "listing_title": "1517 East Anderson Lane",
              "listing_refreshed": "2026-04-14 15:37:32",
              "listing_address": "1517 East Anderson Lane",
              "listing_city": "Austin",
              "listing_state": "TX",
              "listing_zip": "78752",
              "call_price": 0,
              "price": 2100000,
              "lot_acre": "3.580",
              "listing_type": "sale",
              "proptype": "Commercial Land"
            },
            "agents": []
          };
        </script>
      </body>
    </html>
    """
    lease_detail_page = """
    <html>
      <body>
        <script>
          var pdfdata = {
            "listing": {
              "ID": 316815,
              "listing_islease": 1,
              "listing_title": "16815 Royal Crest Drive",
              "listing_refreshed": "2026-04-18 08:00:00",
              "listing_address": "16815 Royal Crest Drive",
              "listing_city": "Houston",
              "listing_state": "TX",
              "listing_zip": "77058",
              "call_price": 1,
              "price": 0,
              "listing_type": "lease",
              "proptype": "Industrial"
            },
            "agents": []
          };
        </script>
      </body>
    </html>
    """

    def fake_urlopen(request, timeout=30.0):  # noqa: ANN001
        url = request.full_url
        requested_urls.append(url)
        if url == "https://myelisting.com/sitemap.xml":
            return _FakeResponse(sitemap_index.encode("utf-8"))
        if url == "https://myelisting.com/sitemap_listings.xml":
            return _FakeResponse(listing_sitemap.encode("utf-8"))
        if url == "https://myelisting.com/listing/295410/1517-east-anderson-lane-austin-tx-78752/":
            return _FakeResponse(sale_detail_page.encode("utf-8"))
        if url == "https://myelisting.com/listing/316815/16815-royal-crest-drive-houston-tx-77058/":
            return _FakeResponse(lease_detail_page.encode("utf-8"))
        raise AssertionError(f"Unexpected URL fetched: {url}")

    monkeypatch.setattr(connector_module, "urlopen", fake_urlopen)

    report = refresh_source_connector(
        db_session,
        registry,
        connector_key="tx_myelisting_sitemap_test",
        actor_name="test-suite",
        requested_at=datetime(2026, 4, 19, 9, 0, tzinfo=UTC),
    )

    listings = db_session.scalars(
        select(MarketListing)
        .where(
            MarketListing.listing_source_id == "myelisting",
            MarketListing.metro_id == "TX",
            MarketListing.is_active.is_(True),
        )
        .order_by(MarketListing.source_listing_key)
    ).all()

    assert report.status == "success"
    assert report.accepted_count == 1
    assert [listing.source_listing_key for listing in listings] == ["295410"]
    assert listings[0].listing_status == "sale"
    assert all("302383" not in url for url in requested_urls)


def test_refresh_source_connector_ingests_myelisting_lease_records_from_listing_sitemap_xml(
    db_session: Session,
    monkeypatch,
) -> None:
    db_session.add(MetroCatalog(metro_id="TX", display_name="Texas Statewide", state_code="TX"))
    db_session.add(
        SourceCatalog(
            source_id="LISTING",
            display_name="Market Listings Scraper",
            owner_name="Data Governance",
            refresh_cadence="daily",
            block_refresh=False,
            metro_coverage="TX",
            target_table_name="market_listings",
            is_active=True,
        )
    )
    db_session.add(
        ListingSourceCatalog(
            listing_source_id="myelisting_lease",
            display_name="MyEListing Texas Lease Feed",
            acquisition_method="html_scrape",
            base_url="https://myelisting.com/properties/for-lease/texas/all-property-types/",
            allows_scraping=True,
            is_active=True,
        )
    )
    db_session.commit()

    config_path = _make_workspace_temp_dir() / "myelisting_lease_sitemap_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "tx_myelisting_lease_test",
                        "source_id": "LISTING",
                        "metro_id": "TX",
                        "interface_name": "listing-myelisting-html-v1",
                        "adapter_type": "myelisting_html",
                        "enabled": True,
                        "load_strategy": "market_listing",
                        "preprocess_options": {
                            "listing_source_id": "myelisting_lease",
                            "replace_existing_scope": "listing_source_metro",
                        },
                        "request": {
                            "endpoint_url": "https://myelisting.com/properties/for-lease/texas/all-property-types/",
                            "record_pattern": "-tx(?:-|/)",
                            "start_urls": [
                                "https://myelisting.com/sitemap.xml"
                            ],
                        },
                        "field_rules": [
                            {
                                "target": "source_listing_key",
                                "source": "source_listing_key",
                                "transform": "strip",
                            },
                            {
                                "target": "listing_title",
                                "source": "listing_title",
                                "transform": "strip",
                            },
                            {
                                "target": "listing_status",
                                "source": "listing_status",
                                "transform": "strip",
                            },
                            {
                                "target": "asset_type",
                                "source": "asset_type",
                                "transform": "strip",
                            },
                            {
                                "target": "asking_price",
                                "source": "asking_price",
                                "transform": "strip",
                            },
                            {
                                "target": "acreage",
                                "source": "acreage",
                                "transform": "strip",
                            },
                            {
                                "target": "building_sqft",
                                "source": "building_sqft",
                                "transform": "strip",
                            },
                            {
                                "target": "address_line1",
                                "source": "address_line1",
                                "transform": "strip",
                            },
                            {
                                "target": "city",
                                "source": "city",
                                "transform": "strip",
                            },
                            {
                                "target": "state_code",
                                "source": "state_code",
                                "transform": "upper",
                            },
                            {
                                "target": "postal_code",
                                "source": "postal_code",
                                "transform": "strip",
                            },
                            {
                                "target": "latitude",
                                "source": "latitude",
                                "transform": "strip",
                            },
                            {
                                "target": "longitude",
                                "source": "longitude",
                                "transform": "strip",
                            },
                            {
                                "target": "broker_name",
                                "source": "broker_name",
                                "transform": "strip",
                            },
                            {
                                "target": "source_url",
                                "source": "source_url",
                                "transform": "strip",
                            },
                            {
                                "target": "last_verified_at",
                                "source": "listing_refreshed",
                                "transform": "strip",
                            },
                            {
                                "target": "lineage_key",
                                "transform": "template",
                                "template": "listing:myelisting_lease:{source_listing_key}",
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    registry = load_connector_registry(str(config_path))

    requested_urls: list[str] = []
    sitemap_index = """
    <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <sitemap>
        <loc>https://myelisting.com/sitemap_listings.xml</loc>
      </sitemap>
    </sitemapindex>
    """
    listing_sitemap = """
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <url>
        <loc>https://myelisting.com/listing/295410/1517-east-anderson-lane-austin-tx-78752/</loc>
      </url>
      <url>
        <loc>https://myelisting.com/listing/316815/16815-royal-crest-drive-houston-tx-77058/</loc>
      </url>
    </urlset>
    """
    sale_detail_page = """
    <html>
      <body>
        <script>
          var pdfdata = {
            "listing": {
              "ID": 295410,
              "listing_islease": 0,
              "listing_title": "1517 East Anderson Lane",
              "listing_refreshed": "2026-04-14 15:37:32",
              "listing_address": "1517 East Anderson Lane",
              "listing_city": "Austin",
              "listing_state": "TX",
              "listing_zip": "78752",
              "call_price": 0,
              "price": 2100000,
              "lot_acre": "3.580",
              "listing_type": "sale",
              "proptype": "Commercial Land"
            },
            "agents": []
          };
        </script>
      </body>
    </html>
    """
    lease_detail_page = """
    <html>
      <body>
        <script>
          var pdfdata = {
            "listing": {
              "ID": 316815,
              "listing_islease": 1,
              "listing_title": "16815 Royal Crest Drive",
              "listing_refreshed": "2026-04-18 08:00:00",
              "listing_address": "16815 Royal Crest Drive",
              "listing_city": "Houston",
              "listing_state": "TX",
              "listing_zip": "77058",
              "call_price": 1,
              "price": 0,
              "listing_type": "lease",
              "proptype": "Industrial"
            },
            "agents": []
          };
        </script>
      </body>
    </html>
    """

    def fake_urlopen(request, timeout=30.0):  # noqa: ANN001
        url = request.full_url
        requested_urls.append(url)
        if url == "https://myelisting.com/sitemap.xml":
            return _FakeResponse(sitemap_index.encode("utf-8"))
        if url == "https://myelisting.com/sitemap_listings.xml":
            return _FakeResponse(listing_sitemap.encode("utf-8"))
        if url == "https://myelisting.com/listing/295410/1517-east-anderson-lane-austin-tx-78752/":
            return _FakeResponse(sale_detail_page.encode("utf-8"))
        if url == "https://myelisting.com/listing/316815/16815-royal-crest-drive-houston-tx-77058/":
            return _FakeResponse(lease_detail_page.encode("utf-8"))
        raise AssertionError(f"Unexpected URL fetched: {url}")

    monkeypatch.setattr(connector_module, "urlopen", fake_urlopen)

    report = refresh_source_connector(
        db_session,
        registry,
        connector_key="tx_myelisting_lease_test",
        actor_name="test-suite",
        requested_at=datetime(2026, 4, 19, 9, 0, tzinfo=UTC),
    )

    listings = db_session.scalars(
        select(MarketListing)
        .where(
            MarketListing.listing_source_id == "myelisting_lease",
            MarketListing.metro_id == "TX",
            MarketListing.is_active.is_(True),
        )
        .order_by(MarketListing.source_listing_key)
    ).all()

    assert report.status == "success"
    assert report.accepted_count == 1
    assert [listing.source_listing_key for listing in listings] == ["316815"]
    assert listings[0].listing_status == "lease"
    assert "https://myelisting.com/listing/316815/16815-royal-crest-drive-houston-tx-77058/" in requested_urls


def test_refresh_source_connector_supports_unique_source_id_parcel_load_strategy(
    db_session: Session,
) -> None:
    _seed_parcel_refresh_catalog(db_session)
    config_path = _make_workspace_temp_dir() / "parcel_load_strategy_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "if029_dcad_fixture",
                        "source_id": "IF029",
                        "metro_id": "DFW",
                        "interface_name": "if029_fixture_v1",
                        "adapter_type": "fixture",
                        "enabled": True,
                        "load_strategy": "parcel",
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
                                "parcel_id": "IF029:48113:D100",
                                "county_fips": "48113",
                                "acreage": "42.5",
                                "geometry_wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                                "lineage_key": "parcel:IF029:48113:D100",
                                "apn": "D100",
                            }
                        ],
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
        connector_key="if029_dcad_fixture",
        actor_name="test-suite",
        requested_at=datetime(2026, 4, 18, 10, 0, tzinfo=UTC),
    )

    snapshot = db_session.scalar(
        select(SourceSnapshot).where(SourceSnapshot.source_id == "IF029")
    )
    parcel = db_session.get(RawParcel, "IF029:48113:D100")

    assert report.status == "success"
    assert report.accepted_count == 1
    assert snapshot is not None
    assert snapshot.source_id == "IF029"
    assert parcel is not None
    assert parcel.apn == "D100"


def test_refresh_source_connector_extract_decimal_transform_parses_text_acreage(
    db_session: Session,
) -> None:
    _seed_parcel_refresh_catalog(db_session)
    config_path = _make_workspace_temp_dir() / "parcel_extract_decimal_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "if029_dcad_extract_decimal_fixture",
                        "source_id": "IF029",
                        "metro_id": "DFW",
                        "interface_name": "if029_extract_decimal_fixture_v1",
                        "adapter_type": "fixture",
                        "enabled": True,
                        "load_strategy": "parcel",
                        "static_fields": {
                            "county_fips": "48113",
                        },
                        "field_rules": [
                            {
                                "target": "apn",
                                "source": "Acct",
                                "transform": "strip",
                            },
                            {
                                "target": "parcel_id",
                                "transform": "template",
                                "template": "IF029:{county_fips}:{apn}",
                            },
                            {
                                "target": "acreage",
                                "source": "RecAcs",
                                "transform": "extract_decimal",
                            },
                            {
                                "target": "geometry_wkt",
                                "source": "__geometry__",
                                "transform": "geojson_to_wkt",
                            },
                            {
                                "target": "lineage_key",
                                "transform": "template",
                                "template": "parcel:{parcel_id}",
                            },
                        ],
                        "fixture_records": [
                            {
                                "Acct": "60210500000110000",
                                "RecAcs": "1.075 Acre",
                                "__geometry__": {
                                    "type": "Polygon",
                                    "coordinates": [
                                        [[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]
                                    ],
                                },
                            }
                        ],
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
        connector_key="if029_dcad_extract_decimal_fixture",
        actor_name="test-suite",
        requested_at=datetime(2026, 4, 18, 10, 5, tzinfo=UTC),
    )

    parcel = db_session.get(RawParcel, "IF029:48113:60210500000110000")

    assert report.status == "success"
    assert report.accepted_count == 1
    assert parcel is not None
    assert str(parcel.acreage) == "1.07"


def test_fetch_connector_records_geojson_to_wkt_supports_raw_arcgis_polygon_geometry() -> None:
    config_path = _make_workspace_temp_dir() / "arcgis_geometry_to_wkt_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "arcgis_geometry_to_wkt_fixture",
                        "source_id": "IF029",
                        "metro_id": "DFW",
                        "interface_name": "if029-arcgis-geometry-to-wkt-fixture-v1",
                        "adapter_type": "fixture",
                        "enabled": True,
                        "load_strategy": "parcel",
                        "static_fields": {
                            "county_fips": "48113",
                        },
                        "field_rules": [
                            {
                                "target": "apn",
                                "source": "Acct",
                                "transform": "strip",
                            },
                            {
                                "target": "parcel_id",
                                "transform": "template",
                                "template": "IF029:{county_fips}:{apn}",
                            },
                            {
                                "target": "geometry_wkt",
                                "source": "__geometry__",
                                "transform": "geojson_to_wkt",
                            },
                        ],
                        "fixture_records": [
                            {
                                "Acct": "D100",
                                "__geometry__": {
                                    "rings": [
                                        [
                                            [0.0, 0.0],
                                            [0.0, 1.0],
                                            [1.0, 1.0],
                                            [1.0, 0.0],
                                            [0.0, 0.0],
                                        ]
                                    ]
                                },
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    registry = load_connector_registry(str(config_path))
    result = fetch_connector_records(
        registry.get_definition_by_connector_key("arcgis_geometry_to_wkt_fixture")
    )

    assert len(result.records) == 1
    polygon = shapely_wkt.loads(result.records[0]["geometry_wkt"])
    assert isinstance(polygon, Polygon)
    assert round(polygon.area, 2) == 1.0


def test_fetch_connector_records_geojson_to_wkt_sanitizes_malformed_arcgis_ring_vertices() -> None:
    config_path = _make_workspace_temp_dir() / "arcgis_geometry_sanitize_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "arcgis_geometry_sanitize_fixture",
                        "source_id": "IF036",
                        "metro_id": "SAT",
                        "interface_name": "if036-arcgis-geometry-sanitize-fixture-v1",
                        "adapter_type": "fixture",
                        "enabled": True,
                        "load_strategy": "parcel",
                        "static_fields": {
                            "county_fips": "48029",
                        },
                        "field_rules": [
                            {
                                "target": "apn",
                                "source": "Acct",
                                "transform": "strip",
                            },
                            {
                                "target": "parcel_id",
                                "transform": "template",
                                "template": "IF036:{county_fips}:{apn}",
                            },
                            {
                                "target": "geometry_wkt",
                                "source": "__geometry__",
                                "transform": "geojson_to_wkt",
                            },
                        ],
                        "fixture_records": [
                            {
                                "Acct": "B100",
                                "__geometry__": {
                                    "rings": [
                                        [
                                            [0.0, 0.0],
                                            ["", ""],
                                            [0.0, 1.0],
                                            [1.0, 1.0],
                                            [1.0, 0.0],
                                            [0.0, 0.0],
                                        ]
                                    ]
                                },
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    registry = load_connector_registry(str(config_path))
    result = fetch_connector_records(
        registry.get_definition_by_connector_key("arcgis_geometry_sanitize_fixture")
    )

    assert len(result.records) == 1
    polygon = shapely_wkt.loads(result.records[0]["geometry_wkt"])
    assert isinstance(polygon, Polygon)
    assert round(polygon.area, 2) == 1.0


def test_refresh_source_connector_expands_evidence_attributes_without_county_lookup(
    db_session: Session,
) -> None:
    db_session.add(
        MetroCatalog(metro_id="TX", display_name="Texas Statewide", state_code="TX")
    )
    db_session.add(
        SourceCatalog(
            source_id="IF001",
            display_name="HIFLD Substations",
            owner_name="Data Governance",
            refresh_cadence="weekly",
            block_refresh=False,
            metro_coverage="TX",
            target_table_name="substations",
            is_active=True,
        )
    )
    db_session.commit()

    config_path = _make_workspace_temp_dir() / "evidence_expand_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "if001_hifld_substations_fixture",
                        "source_id": "IF001",
                        "metro_id": "TX",
                        "interface_name": "if001_fixture_v1",
                        "adapter_type": "fixture",
                        "enabled": True,
                        "load_strategy": "evidence",
                        "preprocess_strategy": "expand_evidence_attributes",
                        "preprocess_options": {
                            "record_key_template": "substation:{facility_id}",
                            "lineage_key_template": "if001:{record_key}:{attribute_name}",
                            "attribute_fields": [
                                {
                                    "attribute_name": "facility_name",
                                    "source": "facility_name",
                                    "transform": "strip",
                                },
                                {
                                    "attribute_name": "voltage_kv",
                                    "source": "voltage_kv",
                                    "transform": "strip",
                                },
                            ],
                        },
                        "fixture_records": [
                            {
                                "facility_id": "TX-001",
                                "facility_name": "North Dallas Substation",
                                "voltage_kv": "345",
                            }
                        ],
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
        connector_key="if001_hifld_substations_fixture",
        actor_name="test-suite",
        requested_at=datetime(2026, 4, 18, 11, 0, tzinfo=UTC),
    )

    evidence_rows = db_session.scalars(
        select(SourceEvidence)
        .where(SourceEvidence.source_id == "IF001")
        .order_by(SourceEvidence.attribute_name)
    ).all()

    assert report.status == "success"
    assert report.accepted_count == 2
    assert [row.attribute_name for row in evidence_rows] == [
        "facility_name",
        "voltage_kv",
    ]
    assert evidence_rows[0].record_key == "substation:TX-001"
    assert evidence_rows[0].metro_id == "TX"


def test_refresh_source_connector_expands_evidence_attributes_with_nested_list_paths(
    db_session: Session,
) -> None:
    db_session.add(
        MetroCatalog(metro_id="TX", display_name="Texas Statewide", state_code="TX")
    )
    db_session.add(
        SourceCatalog(
            source_id="IF-021",
            display_name="USGS NWIS Water",
            owner_name="Data Governance",
            refresh_cadence="daily",
            block_refresh=False,
            metro_coverage="TX",
            target_table_name="water_levels",
            is_active=True,
        )
    )
    db_session.commit()

    config_path = _make_workspace_temp_dir() / "nwis_nested_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "if021_usgs_nwis_fixture",
                        "source_id": "IF-021",
                        "metro_id": "TX",
                        "interface_name": "if-021-http-json-v1",
                        "adapter_type": "fixture",
                        "enabled": True,
                        "load_strategy": "evidence",
                        "preprocess_strategy": "expand_evidence_attributes",
                        "preprocess_options": {
                            "record_key_template": "{name}",
                            "lineage_key_template": "if021:{record_key}:{attribute_name}",
                            "attribute_fields": [
                                {
                                    "attribute_name": "site_code",
                                    "source": "sourceInfo.siteCode.0.value",
                                    "transform": "strip",
                                },
                                {
                                    "attribute_name": "variable_code",
                                    "source": "variable.variableCode.0.value",
                                    "transform": "strip",
                                },
                                {
                                    "attribute_name": "latest_value",
                                    "source": "values.0.value.0.value",
                                    "transform": "strip",
                                },
                                {
                                    "attribute_name": "latest_value_timestamp",
                                    "source": "values.0.value.0.dateTime",
                                    "transform": "strip",
                                },
                            ],
                        },
                        "fixture_records": [
                            {
                                "name": "USGS:07227420:00060:00000",
                                "sourceInfo": {
                                    "siteCode": [
                                        {
                                            "value": "07227420",
                                        }
                                    ]
                                },
                                "variable": {
                                    "variableCode": [
                                        {
                                            "value": "00060",
                                        }
                                    ]
                                },
                                "values": [
                                    {
                                        "value": [
                                            {
                                                "value": "0.01",
                                                "dateTime": "2026-04-18T01:00:00.000-05:00",
                                            }
                                        ]
                                    }
                                ],
                            }
                        ],
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
        connector_key="if021_usgs_nwis_fixture",
        actor_name="test-suite",
        requested_at=datetime(2026, 4, 18, 12, 0, tzinfo=UTC),
    )

    evidence_rows = db_session.scalars(
        select(SourceEvidence)
        .where(SourceEvidence.source_id == "IF-021")
        .order_by(SourceEvidence.attribute_name)
    ).all()

    assert report.status == "success"
    assert report.accepted_count == 4
    assert [row.attribute_name for row in evidence_rows] == [
        "latest_value",
        "latest_value_timestamp",
        "site_code",
        "variable_code",
    ]
    assert evidence_rows[2].attribute_value == "07227420"
    assert evidence_rows[3].attribute_value == "00060"


def test_refresh_source_connector_expands_evidence_attributes_from_headered_json_rows(
    db_session: Session,
    monkeypatch,
) -> None:
    db_session.add(
        MetroCatalog(metro_id="TX", display_name="Texas Statewide", state_code="TX")
    )
    db_session.add(
        SourceCatalog(
            source_id="IF-010",
            display_name="Census ACS",
            owner_name="Data Governance",
            refresh_cadence="daily",
            block_refresh=False,
            metro_coverage="TX",
            target_table_name="census_demographics",
            is_active=True,
        )
    )
    db_session.commit()

    config_path = _make_workspace_temp_dir() / "census_acs_headered_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "tx_census_acs_fixture",
                        "source_id": "IF-010",
                        "metro_id": "TX",
                        "interface_name": "if-010-http-json-v1",
                        "adapter_type": "http_json",
                        "enabled": True,
                        "load_strategy": "evidence",
                        "preprocess_strategy": "expand_evidence_attributes",
                        "preprocess_options": {
                            "replace_existing_scope": "source_metro",
                            "record_key_template": "census-tract:{state}{county}{tract}",
                            "lineage_key_template": "if010:{record_key}:{attribute_name}",
                            "attribute_fields": [
                                {
                                    "attribute_name": "tract_geoid",
                                    "transform": "template",
                                    "template": "{state}{county}{tract}",
                                },
                                {
                                    "attribute_name": "tract_name",
                                    "source": "NAME",
                                    "transform": "strip",
                                },
                                {
                                    "attribute_name": "total_population",
                                    "source": "B01001_001E",
                                    "transform": "strip",
                                },
                                {
                                    "attribute_name": "median_household_income",
                                    "source": "B19013_001E",
                                    "transform": "strip",
                                },
                            ],
                        },
                        "request": {
                            "endpoint_url": "https://example.test/census-acs",
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    registry = load_connector_registry(str(config_path))
    monkeypatch.setattr(
        connector_module,
        "urlopen",
        lambda request, timeout=30.0: _FakeResponse(
            json.dumps(
                [
                    ["NAME", "B01001_001E", "B19013_001E", "state", "county", "tract"],
                    [
                        "Census Tract 1; Dallas County; Texas",
                        "4450",
                        "65000",
                        "48",
                        "113",
                        "000100",
                    ],
                ]
            ).encode("utf-8")
        ),
    )

    report = refresh_source_connector(
        db_session,
        registry,
        connector_key="tx_census_acs_fixture",
        actor_name="test-suite",
        requested_at=datetime(2026, 4, 19, 7, 0, tzinfo=UTC),
    )

    evidence_rows = db_session.scalars(
        select(SourceEvidence)
        .where(SourceEvidence.source_id == "IF-010")
        .order_by(SourceEvidence.attribute_name)
    ).all()

    assert report.status == "success"
    assert report.accepted_count == 4
    assert [row.attribute_name for row in evidence_rows] == [
        "median_household_income",
        "total_population",
        "tract_geoid",
        "tract_name",
    ]
    assert all(row.record_key == "census-tract:48113000100" for row in evidence_rows)


def _seed_parcel_refresh_catalog(session: Session) -> None:
    session.add(MetroCatalog(metro_id="DFW", display_name="Dallas-Fort Worth", state_code="TX"))
    session.add(
        CountyCatalog(
            county_fips="48113",
            metro_id="DFW",
            display_name="Dallas",
            state_code="TX",
        )
    )
    session.add(
        SourceCatalog(
            source_id="IF029",
            display_name="DCAD (Dallas)",
            owner_name="Data Governance",
            refresh_cadence="daily",
            block_refresh=True,
            metro_coverage="DFW",
            target_table_name="raw_parcels",
            is_active=True,
        )
    )
    session.commit()


def _build_shapefile_zip_bytes() -> bytes:
    shp_buffer = BytesIO()
    shx_buffer = BytesIO()
    dbf_buffer = BytesIO()
    writer = shapefile.Writer(shp=shp_buffer, shx=shx_buffer, dbf=dbf_buffer)
    writer.field("NAME", "C")
    writer.point(-96.8, 32.8)
    writer.record("Switchyard A")
    writer.close()

    zip_buffer = BytesIO()
    with ZipFile(zip_buffer, "w") as archive:
        archive.writestr("facilities.shp", shp_buffer.getvalue())
        archive.writestr("facilities.shx", shx_buffer.getvalue())
        archive.writestr("facilities.dbf", dbf_buffer.getvalue())
    return zip_buffer.getvalue()


def _build_projected_shapefile_zip_bytes() -> bytes:
    transformer = Transformer.from_crs(
        CRS.from_epsg(4326),
        CRS.from_epsg(3857),
        always_xy=True,
    )
    projected_x, projected_y = transformer.transform(-96.8, 32.8)

    shp_buffer = BytesIO()
    shx_buffer = BytesIO()
    dbf_buffer = BytesIO()
    writer = shapefile.Writer(shp=shp_buffer, shx=shx_buffer, dbf=dbf_buffer)
    writer.field("NAME", "C")
    writer.point(projected_x, projected_y)
    writer.record("Projected Switchyard")
    writer.close()

    zip_buffer = BytesIO()
    with ZipFile(zip_buffer, "w") as archive:
        archive.writestr("facilities.shp", shp_buffer.getvalue())
        archive.writestr("facilities.shx", shx_buffer.getvalue())
        archive.writestr("facilities.dbf", dbf_buffer.getvalue())
        archive.writestr("facilities.prj", CRS.from_epsg(3857).to_wkt())
    return zip_buffer.getvalue()


def _build_shapefile_zip_with_null_shape_bytes() -> bytes:
    shp_buffer = BytesIO()
    shx_buffer = BytesIO()
    dbf_buffer = BytesIO()
    writer = shapefile.Writer(shp=shp_buffer, shx=shx_buffer, dbf=dbf_buffer)
    writer.field("NAME", "C")
    writer.null()
    writer.record("Null Shape")
    writer.point(-96.81, 32.81)
    writer.record("Switchyard B")
    writer.close()

    zip_buffer = BytesIO()
    with ZipFile(zip_buffer, "w") as archive:
        archive.writestr("facilities.shp", shp_buffer.getvalue())
        archive.writestr("facilities.shx", shx_buffer.getvalue())
        archive.writestr("facilities.dbf", dbf_buffer.getvalue())
    return zip_buffer.getvalue()


def _build_projected_arcgis_polygon_payload() -> tuple[dict[str, object], str]:
    ring_lon_lat = [
        (-98.4941, 29.4238),
        (-98.4931, 29.4238),
        (-98.4931, 29.4248),
        (-98.4941, 29.4248),
        (-98.4941, 29.4238),
    ]
    transformer = Transformer.from_crs(
        CRS.from_epsg(4326),
        CRS.from_epsg(2278),
        always_xy=True,
    )
    projected_ring = [list(transformer.transform(lon, lat)) for lon, lat in ring_lon_lat]
    native_area = Polygon(projected_ring).area
    expected_acreage = f"{(native_area / 43560):.6f}".rstrip("0").rstrip(".")

    return (
        {
            "spatialReference": {
                "wkid": 102740,
                "latestWkid": 2278,
            },
            "features": [
                {
                    "attributes": {
                        "OBJECTID": 1,
                        "PROP_ID": "BCAD-1001",
                    },
                    "geometry": {
                        "rings": [projected_ring],
                    },
                }
            ],
        },
        expected_acreage,
    )


def _make_workspace_temp_dir() -> Path:
    temp_dir = Path("temp") / f"connector-protocol-tests-{uuid4().hex}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir
