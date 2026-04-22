from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models.catalogs import SourceCatalog
from app.db.models.connectors import SourceRefreshCheckpoint, SourceRefreshJob
from app.db.models.enums import SourceSnapshotStatus
from app.db.models.ingestion import SourceSnapshot
from app.db.models.source_data import SourceEvidence
from app.db.models.territory import CountyCatalog, MetroCatalog, RawParcel
from ingestion.connectors import load_connector_registry
from ingestion.refresh import (
    build_refresh_plan,
    refresh_due_connectors,
    refresh_source_connector,
)


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

    plan = build_refresh_plan(
        db_session,
        registry,
        as_of=datetime(2026, 4, 18, 12, 0, tzinfo=UTC),
    )
    parcel_item = next(item for item in plan if item.connector_key == "dfw_parcel_pilot")
    zoning_item = next(item for item in plan if item.connector_key == "dfw_zoning_pilot")
    live_parcel_item = next(
        item for item in plan if item.connector_key == "dfw_dallas_arcgis_parcels_live"
    )
    peeringdb_item = next(
        item for item in plan if item.connector_key == "tx_peeringdb_facilities_live"
    )

    assert parcel_item.due is False
    assert parcel_item.due_reason == "within_cadence"
    assert parcel_item.latest_snapshot_status == "success"
    assert zoning_item.due is True
    assert zoning_item.due_reason == "missing_snapshot"
    assert live_parcel_item.enabled is False
    assert live_parcel_item.due is False
    assert live_parcel_item.due_reason == "connector_disabled"
    assert peeringdb_item.enabled is True
    assert peeringdb_item.due is False
    assert peeringdb_item.due_reason == "source_not_configured"


def test_build_refresh_plan_marks_quarantined_snapshots_as_due(
    db_session: Session,
) -> None:
    _seed_reference_catalogs(db_session)
    registry = load_connector_registry("configs/source_connectors.json")

    db_session.add(
        SourceSnapshot(
            source_id="PARCEL",
            metro_id="DFW",
            connector_key="dfw_parcel_pilot",
            snapshot_ts=datetime(2026, 4, 18, 10, 0, tzinfo=UTC),
            source_version="parcel_fixture_v1",
            row_count=100,
            status=SourceSnapshotStatus.QUARANTINED,
            error_message="100 duplicate row(s) were skipped during ingest.",
            created_at=datetime(2026, 4, 18, 10, 0, tzinfo=UTC),
            updated_at=datetime(2026, 4, 18, 10, 0, tzinfo=UTC),
        )
    )
    db_session.commit()

    plan = build_refresh_plan(
        db_session,
        registry,
        as_of=datetime(2026, 4, 18, 12, 0, tzinfo=UTC),
    )
    parcel_item = next(item for item in plan if item.connector_key == "dfw_parcel_pilot")

    assert parcel_item.due is True
    assert parcel_item.due_reason == "latest_snapshot_quarantined"
    assert parcel_item.latest_snapshot_status == "quarantined"


def test_build_refresh_plan_prefers_most_recent_loaded_snapshot_over_source_timestamp(
    db_session: Session,
) -> None:
    db_session.add(MetroCatalog(metro_id="TX", display_name="Texas Statewide", state_code="TX"))
    db_session.add(
        SourceCatalog(
            source_id="IF001",
            display_name="HIFLD Substations",
            owner_name="Data Governance",
            refresh_cadence="daily",
            block_refresh=False,
            metro_coverage="TX",
            target_table_name="source_evidence",
            is_active=True,
        )
    )
    db_session.commit()

    config_path = _make_workspace_temp_dir() / "latest_snapshot_order_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "if001_fixture_priority",
                        "source_id": "IF001",
                        "metro_id": "TX",
                        "interface_name": "if001_fixture_v1",
                        "adapter_type": "fixture",
                        "enabled": True,
                        "load_strategy": "evidence",
                        "fixture_records": [],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    registry = load_connector_registry(str(config_path))

    db_session.add_all(
        [
            SourceSnapshot(
                source_id="IF001",
                metro_id="TX",
                connector_key="if001_fixture_priority",
                snapshot_ts=datetime(2026, 4, 19, 13, 0, tzinfo=UTC),
                source_version="if001:failed",
                row_count=0,
                status=SourceSnapshotStatus.FAILED,
                error_message="load failed",
                created_at=datetime(2026, 4, 19, 13, 0, tzinfo=UTC),
                updated_at=datetime(2026, 4, 19, 13, 0, tzinfo=UTC),
            ),
            SourceSnapshot(
                source_id="IF001",
                metro_id="TX",
                connector_key="if001_fixture_priority",
                snapshot_ts=datetime(2026, 4, 1, 0, 0, tzinfo=UTC),
                source_version="if001:success",
                row_count=1,
                status=SourceSnapshotStatus.SUCCESS,
                error_message=None,
                created_at=datetime(2026, 4, 19, 14, 0, tzinfo=UTC),
                updated_at=datetime(2026, 4, 19, 14, 0, tzinfo=UTC),
            ),
        ]
    )
    db_session.commit()

    plan = build_refresh_plan(
        db_session,
        registry,
        as_of=datetime(2026, 4, 19, 15, 0, tzinfo=UTC),
    )
    item = next(item for item in plan if item.connector_key == "if001_fixture_priority")

    assert item.latest_snapshot_status == "success"
    assert item.due is True
    assert item.due_reason == "cadence_due"


def test_live_tarrant_connector_uses_account_as_unique_parcel_identifier() -> None:
    registry = load_connector_registry("configs/source_connectors.json")

    definition = registry.get_definition("IF-030", "DFW")
    apn_rule = next(rule for rule in definition.field_rules if rule.target == "apn")
    row_filter_source = [rule.source for rule in definition.row_filters]

    assert apn_rule.source == ["ACCOUNT", "TAXPIN"]
    assert "ACCOUNT" in row_filter_source
    assert definition.request.query_params["where"] == "ACCOUNT IS NOT NULL"


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


def test_refresh_source_connector_persists_failed_job_and_checkpoint_after_ingest_rollback(
    db_session: Session,
) -> None:
    db_session.add(MetroCatalog(metro_id="TX", display_name="Texas Statewide", state_code="TX"))
    db_session.add(
        SourceCatalog(
            source_id="IF001",
            display_name="HIFLD Substations",
            owner_name="Data Governance",
            refresh_cadence="daily",
            block_refresh=False,
            metro_coverage="TX",
            target_table_name="source_evidence",
            is_active=True,
        )
    )
    db_session.commit()

    config_path = _make_workspace_temp_dir() / "failed_evidence_checkpoint_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "if001_failed_fixture",
                        "source_id": "IF001",
                        "metro_id": "TX",
                        "interface_name": "if001_failed_fixture_v1",
                        "adapter_type": "fixture",
                        "enabled": True,
                        "load_strategy": "evidence",
                        "preprocess_strategy": "expand_evidence_attributes",
                        "preprocess_options": {
                            "record_key_template": "substation:{facility_id}",
                            "lineage_key_template": "if001:{record_key}:{attribute_name}",
                            "county_fips_source": "county_fips",
                            "attribute_fields": [
                                {
                                    "attribute_name": "facility_name",
                                    "source": "facility_name",
                                    "transform": "strip",
                                }
                            ],
                        },
                        "fixture_records": [
                            {
                                "facility_id": "TX-ERR-1",
                                "facility_name": "Broken County Lookup",
                                "county_fips": "48113",
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
        connector_key="if001_failed_fixture",
        actor_name="test-suite",
    )

    persisted_job = db_session.get(SourceRefreshJob, UUID(report.job_id))
    checkpoint = db_session.scalar(
        select(SourceRefreshCheckpoint).where(
            SourceRefreshCheckpoint.connector_key == "if001_failed_fixture"
        )
    )

    assert report.status == "failed"
    assert report.snapshot_id is not None
    assert "no county mapping" in (report.error_message or "").lower()
    assert persisted_job is not None
    assert persisted_job.status == "failed"
    assert checkpoint is not None
    assert checkpoint.last_status == "failed"
    assert checkpoint.last_job_id == UUID(report.job_id)


def test_refresh_source_connector_spatial_filter_expand_evidence_attributes_filters_to_boundary(
    db_session: Session,
) -> None:
    db_session.add(MetroCatalog(metro_id="TX", display_name="Texas Statewide", state_code="TX"))
    db_session.add(
        SourceCatalog(
            source_id="IF024",
            display_name="Critical Habitat",
            owner_name="Data Governance",
            refresh_cadence="daily",
            block_refresh=False,
            metro_coverage="TX",
            target_table_name="source_evidence",
            is_active=True,
        )
    )
    db_session.commit()

    temp_dir = _make_workspace_temp_dir()
    boundary_path = temp_dir / "tx-boundary.geojson"
    boundary_path.write_text(
        json.dumps(
            {
                "type": "Feature",
                "properties": {"metro_id": "TX"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-100.0, 30.0],
                            [-94.0, 30.0],
                            [-94.0, 34.0],
                            [-100.0, 34.0],
                            [-100.0, 30.0],
                        ]
                    ],
                },
            }
        ),
        encoding="utf-8",
    )
    config_path = temp_dir / "spatial_filter_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "if024_spatial_filter_fixture",
                        "source_id": "IF024",
                        "metro_id": "TX",
                        "interface_name": "if024_spatial_filter_fixture_v1",
                        "adapter_type": "fixture",
                        "enabled": True,
                        "load_strategy": "evidence",
                        "preprocess_strategy": "spatial_filter_expand_evidence_attributes",
                        "preprocess_options": {
                            "boundary_geojson_path": str(boundary_path),
                            "record_key_template": "habitat:{feature_id}",
                            "lineage_key_template": "if024:{record_key}:{attribute_name}",
                            "attribute_fields": [
                                {
                                    "attribute_name": "common_name",
                                    "source": "common_name",
                                    "transform": "strip",
                                }
                            ],
                        },
                        "fixture_records": [
                            {
                                "feature_id": "inside",
                                "common_name": "Inside Habitat",
                                "__geometry__": {
                                    "type": "Point",
                                    "coordinates": [-97.5, 32.0],
                                },
                            },
                            {
                                "feature_id": "outside",
                                "common_name": "Outside Habitat",
                                "__geometry__": {
                                    "type": "Point",
                                    "coordinates": [-110.0, 32.0],
                                },
                            },
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
        connector_key="if024_spatial_filter_fixture",
        actor_name="test-suite",
    )

    evidence_rows = db_session.scalars(select(SourceEvidence)).all()

    assert report.status == "success"
    assert report.row_count == 1
    assert report.accepted_count == 1
    assert len(evidence_rows) == 1
    assert evidence_rows[0].record_key == "habitat:inside"
    assert evidence_rows[0].attribute_value == "Inside Habitat"


def test_refresh_source_connector_replace_existing_scope_source_metro_replaces_active_evidence(
    db_session: Session,
) -> None:
    db_session.add(MetroCatalog(metro_id="TX", display_name="Texas Statewide", state_code="TX"))
    db_session.add(
        SourceCatalog(
            source_id="IF001",
            display_name="HIFLD Substations",
            owner_name="Data Governance",
            refresh_cadence="daily",
            block_refresh=False,
            metro_coverage="TX",
            target_table_name="source_evidence",
            is_active=True,
        )
    )
    db_session.commit()

    temp_dir = _make_workspace_temp_dir()
    config_path = temp_dir / "replace_scope_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "if001_replace_scope_fixture",
                        "source_id": "IF001",
                        "metro_id": "TX",
                        "interface_name": "if001_replace_scope_fixture_v1",
                        "adapter_type": "fixture",
                        "enabled": True,
                        "load_strategy": "evidence",
                        "preprocess_strategy": "expand_evidence_attributes",
                        "preprocess_options": {
                            "replace_existing_scope": "source_metro",
                            "record_key_template": "substation:{facility_id}",
                            "lineage_key_template": "if001:{record_key}:{attribute_name}",
                            "attribute_fields": [
                                {
                                    "attribute_name": "facility_name",
                                    "source": "facility_name",
                                    "transform": "strip",
                                }
                            ],
                        },
                        "fixture_records": [
                            {
                                "facility_id": "TX-001",
                                "facility_name": "North Dallas Substation",
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    initial_registry = load_connector_registry(str(config_path))
    initial_report = refresh_source_connector(
        db_session,
        initial_registry,
        connector_key="if001_replace_scope_fixture",
        actor_name="test-suite",
    )

    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "if001_replace_scope_fixture",
                        "source_id": "IF001",
                        "metro_id": "TX",
                        "interface_name": "if001_replace_scope_fixture_v1",
                        "adapter_type": "fixture",
                        "enabled": True,
                        "load_strategy": "evidence",
                        "preprocess_strategy": "expand_evidence_attributes",
                        "preprocess_options": {
                            "replace_existing_scope": "source_metro",
                            "record_key_template": "substation:{facility_id}",
                            "lineage_key_template": "if001:{record_key}:{attribute_name}",
                            "attribute_fields": [
                                {
                                    "attribute_name": "facility_name",
                                    "source": "facility_name",
                                    "transform": "strip",
                                }
                            ],
                        },
                        "fixture_records": [
                            {
                                "facility_id": "TX-002",
                                "facility_name": "Fort Worth Substation",
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    replacement_registry = load_connector_registry(str(config_path))
    replacement_report = refresh_source_connector(
        db_session,
        replacement_registry,
        connector_key="if001_replace_scope_fixture",
        actor_name="test-suite",
    )

    active_rows = db_session.scalars(
        select(SourceEvidence)
        .where(
            SourceEvidence.source_id == "IF001",
            SourceEvidence.metro_id == "TX",
            SourceEvidence.is_active.is_(True),
        )
        .order_by(SourceEvidence.record_key)
    ).all()
    inactive_rows = db_session.scalars(
        select(SourceEvidence)
        .where(
            SourceEvidence.source_id == "IF001",
            SourceEvidence.metro_id == "TX",
            SourceEvidence.is_active.is_(False),
        )
        .order_by(SourceEvidence.record_key)
    ).all()

    assert initial_report.status == "success"
    assert replacement_report.status == "success"
    assert [row.record_key for row in active_rows] == ["substation:TX-002"]
    assert [row.record_key for row in inactive_rows] == ["substation:TX-001"]
    assert active_rows[0].attribute_value == "Fort Worth Substation"


def test_refresh_source_connector_replace_existing_scope_county_metro_replaces_only_target_parcels(
    db_session: Session,
) -> None:
    _seed_reference_catalogs(db_session)
    db_session.add(
        CountyCatalog(
            county_fips="48113",
            metro_id="DFW",
            display_name="Dallas",
            state_code="TX",
        )
    )
    db_session.commit()

    config_path = _make_workspace_temp_dir() / "parcel_replace_scope_connectors.json"
    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "parcel_replace_scope_fixture",
                        "source_id": "PARCEL",
                        "metro_id": "DFW",
                        "interface_name": "parcel_replace_scope_fixture_v1",
                        "adapter_type": "fixture",
                        "enabled": True,
                        "load_strategy": "parcel",
                        "preprocess_options": {
                            "replace_existing_scope": "county_metro",
                        },
                        "field_map": {
                            "parcel_id": "parcel_id",
                            "county_fips": "county_fips",
                            "acreage": "acreage",
                            "geometry_wkt": "geometry_wkt",
                            "lineage_key": "lineage_key",
                        },
                        "fixture_records": [
                            {
                                "parcel_id": "DAL-300",
                                "county_fips": "48113",
                                "acreage": "20",
                                "geometry_wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                                "lineage_key": "parcel:DAL-300",
                            },
                            {
                                "parcel_id": "COL-300",
                                "county_fips": "48085",
                                "acreage": "10",
                                "geometry_wkt": "POLYGON ((2 0, 2 1, 3 1, 3 0, 2 0))",
                                "lineage_key": "parcel:COL-300",
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    initial_registry = load_connector_registry(str(config_path))
    initial_report = refresh_source_connector(
        db_session,
        initial_registry,
        connector_key="parcel_replace_scope_fixture",
        actor_name="test-suite",
    )

    config_path.write_text(
        json.dumps(
            {
                "definitions": [
                    {
                        "connector_key": "parcel_replace_scope_fixture",
                        "source_id": "PARCEL",
                        "metro_id": "DFW",
                        "interface_name": "parcel_replace_scope_fixture_v1",
                        "adapter_type": "fixture",
                        "enabled": True,
                        "load_strategy": "parcel",
                        "preprocess_options": {
                            "replace_existing_scope": "county_metro",
                        },
                        "field_map": {
                            "parcel_id": "parcel_id",
                            "county_fips": "county_fips",
                            "acreage": "acreage",
                            "geometry_wkt": "geometry_wkt",
                            "lineage_key": "lineage_key",
                        },
                        "fixture_records": [
                            {
                                "parcel_id": "DAL-301",
                                "county_fips": "48113",
                                "acreage": "24",
                                "geometry_wkt": "POLYGON ((10 0, 10 1, 11 1, 11 0, 10 0))",
                                "lineage_key": "parcel:DAL-301",
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    replacement_registry = load_connector_registry(str(config_path))
    replacement_report = refresh_source_connector(
        db_session,
        replacement_registry,
        connector_key="parcel_replace_scope_fixture",
        actor_name="test-suite",
    )

    old_dallas = db_session.get(RawParcel, "DAL-300")
    new_dallas = db_session.get(RawParcel, "DAL-301")
    collin = db_session.get(RawParcel, "COL-300")

    assert initial_report.status == "success"
    assert replacement_report.status == "success"
    assert old_dallas is not None
    assert old_dallas.is_active is False
    assert new_dallas is not None
    assert new_dallas.is_active is True
    assert collin is not None
    assert collin.is_active is True


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
