from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

from app.db.models.batching import ScoreBatch, ScoreRun
from app.db.models.catalogs import SourceCatalog
from app.db.models.enums import ScoreBatchStatus, ScoreRunStatus, SourceSnapshotStatus
from app.db.models.ingestion import SourceSnapshot
from app.db.models.source_data import RawZoning, SourceEvidence
from app.db.models.territory import CountyCatalog, MetroCatalog, ParcelRepPoint, RawParcel


def test_get_evaluation_scope_endpoint_returns_run_scope(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    run_id = _seed_evaluation_api_context(session_factory)

    response = client.get(f"/admin/runs/{run_id}/evaluation/scope")
    payload = response.json()

    assert response.status_code == 200
    assert payload["run_id"] == run_id
    assert payload["metro_id"] == "DFW"
    assert payload["parcel_count"] == 3


def test_execute_evaluation_endpoint_returns_summary(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    run_id = _seed_evaluation_api_context(session_factory)

    response = client.post(
        f"/admin/runs/{run_id}/evaluation",
        json={
            "rule_version": "phase4-r1",
            "allowed_band_wkt": "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))",
            "minimum_acreage": 10,
            "blocked_zoning_codes": ["RES"],
            "evidence_exclusion_rules": [
                {
                    "source_id": "FLOOD",
                    "attribute_name": "fema_zone",
                    "blocked_values": ["AE"],
                    "exclusion_code": "FLOOD_ZONE_BLOCKED",
                    "exclusion_reason": "Flood zone AE is excluded.",
                }
            ],
        },
    )
    payload = response.json()

    assert response.status_code == 200
    assert payload["evaluated_count"] == 3
    assert payload["excluded_count"] == 1
    assert payload["pending_scoring_count"] == 1
    assert payload["size_filtered_count"] == 1


def test_get_evaluation_summary_endpoint_returns_post_run_counts(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    run_id = _seed_evaluation_api_context(session_factory)
    client.post(
        f"/admin/runs/{run_id}/evaluation",
        json={
            "rule_version": "phase4-r1",
            "allowed_band_wkt": "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))",
            "minimum_acreage": 10,
            "blocked_zoning_codes": ["RES"],
        },
    )

    response = client.get(f"/admin/runs/{run_id}/evaluation")
    payload = response.json()

    assert response.status_code == 200
    assert payload["run_id"] == run_id
    assert payload["evaluated_count"] == 3
    assert {item["status"] for item in payload["status_counts"]} == {
        "excluded",
        "pending_scoring",
        "prefiltered_size",
    }


def test_execute_evaluation_endpoint_rejects_invalid_band_wkt(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    run_id = _seed_evaluation_api_context(session_factory)

    response = client.post(
        f"/admin/runs/{run_id}/evaluation",
        json={
            "rule_version": "phase4-r1",
            "allowed_band_wkt": "NOT_A_GEOMETRY",
        },
    )
    payload = response.json()

    assert response.status_code == 422
    assert payload["detail"] == "allowed_band_wkt is not valid WKT."


def _seed_evaluation_api_context(session_factory: sessionmaker[Session]) -> str:
    session = session_factory()
    try:
        now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
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
                SourceCatalog(
                    source_id="FLOOD",
                    display_name="Flood Risk Feed",
                    owner_name="Data Governance",
                    refresh_cadence="daily",
                    block_refresh=True,
                    metro_coverage="DFW",
                    target_table_name="source_evidence",
                    is_active=True,
                ),
            ]
        )
        session.flush()

        session.add_all(
            [
                SourceSnapshot(
                    source_id="PARCEL",
                    metro_id="DFW",
                    snapshot_ts=now - timedelta(hours=1),
                    source_version="parcel_v1",
                    row_count=3,
                    checksum="parcel-checksum",
                    status=SourceSnapshotStatus.SUCCESS,
                ),
                SourceSnapshot(
                    source_id="ZONING",
                    metro_id="DFW",
                    snapshot_ts=now - timedelta(hours=1),
                    source_version="zoning_v1",
                    row_count=1,
                    checksum="zoning-checksum",
                    status=SourceSnapshotStatus.SUCCESS,
                ),
                SourceSnapshot(
                    source_id="FLOOD",
                    metro_id="DFW",
                    snapshot_ts=now - timedelta(hours=1),
                    source_version="flood_v1",
                    row_count=1,
                    checksum="flood-checksum",
                    status=SourceSnapshotStatus.SUCCESS,
                ),
            ]
        )
        session.flush()

        batch = ScoreBatch(
            status=ScoreBatchStatus.BUILDING,
            expected_metros=1,
            completed_metros=0,
        )
        session.add(batch)
        session.flush()

        run = ScoreRun(batch_id=batch.batch_id, metro_id="DFW", status=ScoreRunStatus.RUNNING)
        session.add(run)
        session.flush()

        session.add_all(
            [
                RawParcel(
                    parcel_id="P-API-SURVIVE",
                    county_fips="48085",
                    metro_id="DFW",
                    acreage=25,
                    geometry_wkt="POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                    lineage_key="parcel:P-API-SURVIVE",
                    is_active=True,
                ),
                RawParcel(
                    parcel_id="P-API-SIZE",
                    county_fips="48085",
                    metro_id="DFW",
                    acreage=5,
                    geometry_wkt="POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                    lineage_key="parcel:P-API-SIZE",
                    is_active=True,
                ),
                RawParcel(
                    parcel_id="P-API-EXCL",
                    county_fips="48085",
                    metro_id="DFW",
                    acreage=25,
                    geometry_wkt="POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                    lineage_key="parcel:P-API-EXCL",
                    is_active=True,
                ),
            ]
        )
        session.add_all(
            [
                ParcelRepPoint(
                    parcel_id="P-API-SURVIVE",
                    rep_point_wkt="POINT (5 5)",
                    geometry_method="representative_point",
                ),
                ParcelRepPoint(
                    parcel_id="P-API-SIZE",
                    rep_point_wkt="POINT (5 5)",
                    geometry_method="representative_point",
                ),
                ParcelRepPoint(
                    parcel_id="P-API-EXCL",
                    rep_point_wkt="POINT (5 5)",
                    geometry_method="representative_point",
                ),
            ]
        )
        session.add(
            RawZoning(
                parcel_id="P-API-EXCL",
                county_fips="48085",
                metro_id="DFW",
                zoning_code="RES",
                land_use_code="RESIDENTIAL",
                lineage_key="zoning:P-API-EXCL",
                is_active=True,
            )
        )
        session.add(
            SourceEvidence(
                source_id="FLOOD",
                metro_id="DFW",
                county_fips="48085",
                parcel_id="P-API-EXCL",
                record_key="flood:P-API-EXCL",
                attribute_name="fema_zone",
                attribute_value="AE",
                lineage_key="flood:P-API-EXCL",
                is_active=True,
            )
        )
        session.commit()
        return str(run.run_id)
    finally:
        session.close()
