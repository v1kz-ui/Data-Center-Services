from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models.catalogs import SourceCatalog
from app.db.models.connectors import SourceRefreshCheckpoint
from app.db.models.ingestion import SourceSnapshot
from app.db.models.territory import CountyCatalog, MetroCatalog
from ingestion.connectors import load_connector_registry
from ingestion.refresh import build_refresh_plan, refresh_due_connectors, refresh_source_connector


def test_refresh_source_connector_uses_checkpoint_for_incremental_fixture_loads(
    db_session: Session,
) -> None:
    _seed_reference_catalogs(db_session)
    registry = load_connector_registry("configs/source_connectors.json")

    initial_report = refresh_source_connector(
        db_session,
        registry,
        source_id="PARCEL",
        metro_id="DFW",
        actor_name="Test Operator",
    )
    follow_up_report = refresh_source_connector(
        db_session,
        registry,
        source_id="PARCEL",
        metro_id="DFW",
        actor_name="Test Operator",
    )

    assert initial_report.status == "success"
    assert initial_report.accepted_count == 2
    assert follow_up_report.status == "success"
    assert follow_up_report.row_count == 0
    assert follow_up_report.accepted_count == 0
    assert follow_up_report.checkpoint_in_ts is not None
    assert follow_up_report.checkpoint_out_ts is not None


def test_build_refresh_plan_marks_connectors_with_recent_snapshots_as_not_due(
    db_session: Session,
) -> None:
    _seed_reference_catalogs(db_session)
    registry = load_connector_registry("configs/source_connectors.json")
    refresh_source_connector(
        db_session,
        registry,
        source_id="PARCEL",
        metro_id="DFW",
        actor_name="Test Operator",
    )

    plan = build_refresh_plan(db_session, registry)
    parcel_item = next(item for item in plan if item.connector_key == "dfw_parcel_pilot")
    zoning_item = next(item for item in plan if item.connector_key == "dfw_zoning_pilot")
    live_parcel_item = next(
        item for item in plan if item.connector_key == "dfw_dallas_arcgis_parcels_live"
    )

    assert parcel_item.due is False
    assert parcel_item.due_reason == "within_cadence"
    assert parcel_item.latest_snapshot_status == "success"
    assert zoning_item.due is True
    assert zoning_item.due_reason == "missing_snapshot"
    assert live_parcel_item.enabled is False
    assert live_parcel_item.due is False
    assert live_parcel_item.due_reason == "connector_disabled"


def test_refresh_due_connectors_refreshes_same_scope_connectors_independently(
    db_session: Session,
) -> None:
    _seed_reference_catalogs(db_session)
    config_path = _make_workspace_temp_dir() / "same_scope_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "dfw_parcel_priority_a",
                        "source_id": "PARCEL",
                        "metro_id": "DFW",
                        "interface_name": "parcel_fixture_priority_a_v1",
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
                                "parcel_id": "DFW-A-1001",
                                "county_fips": "48085",
                                "acreage": "11.5",
                                "geometry_wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                                "lineage_key": "parcel:DFW-A-1001",
                                "apn": "A1001",
                                "updated_at": "2026-04-18T10:00:00Z",
                            },
                            {
                                "parcel_id": "DFW-A-1002",
                                "county_fips": "48085",
                                "acreage": "12.5",
                                "geometry_wkt": "POLYGON ((2 0, 2 1, 3 1, 3 0, 2 0))",
                                "lineage_key": "parcel:DFW-A-1002",
                                "apn": "A1002",
                                "updated_at": "2026-04-18T10:15:00Z",
                            },
                        ],
                    },
                    {
                        "connector_key": "dfw_parcel_priority_b",
                        "source_id": "PARCEL",
                        "metro_id": "DFW",
                        "interface_name": "parcel_fixture_priority_b_v1",
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
                                "parcel_id": "DFW-B-2001",
                                "county_fips": "48085",
                                "acreage": "21.0",
                                "geometry_wkt": "POLYGON ((4 0, 4 1, 5 1, 5 0, 4 0))",
                                "lineage_key": "parcel:DFW-B-2001",
                                "apn": "B2001",
                                "updated_at": "2026-04-18T11:00:00Z",
                            }
                        ],
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    registry = load_connector_registry(str(config_path))

    batch_report = refresh_due_connectors(
        db_session,
        registry,
        actor_name="Scheduler",
    )
    snapshots = db_session.scalars(
        select(SourceSnapshot)
        .where(SourceSnapshot.source_id == "PARCEL", SourceSnapshot.metro_id == "DFW")
        .order_by(SourceSnapshot.connector_key, SourceSnapshot.snapshot_ts)
    ).all()
    checkpoints = db_session.scalars(
        select(SourceRefreshCheckpoint).order_by(SourceRefreshCheckpoint.connector_key)
    ).all()
    plan = build_refresh_plan(db_session, registry)
    connector_items = [
        item
        for item in plan
        if item.connector_key in {"dfw_parcel_priority_a", "dfw_parcel_priority_b"}
    ]

    assert batch_report.total_due == 2
    assert batch_report.completed == 2
    assert {report.connector_key for report in batch_report.reports} == {
        "dfw_parcel_priority_a",
        "dfw_parcel_priority_b",
    }
    assert {snapshot.connector_key for snapshot in snapshots} == {
        "dfw_parcel_priority_a",
        "dfw_parcel_priority_b",
    }
    assert {checkpoint.connector_key for checkpoint in checkpoints} == {
        "dfw_parcel_priority_a",
        "dfw_parcel_priority_b",
    }
    assert [item.connector_key for item in connector_items] == [
        "dfw_parcel_priority_a",
        "dfw_parcel_priority_b",
    ]
    assert all(item.due is False for item in connector_items)
    assert all(item.latest_snapshot_status == "success" for item in connector_items)


def _seed_reference_catalogs(session: Session) -> None:
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


def _make_workspace_temp_dir() -> Path:
    temp_dir = Path("temp") / f"connector-refresh-tests-{uuid4().hex}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir
