from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

from app.db.models.catalogs import SourceCatalog
from app.db.models.enums import SourceSnapshotStatus
from app.db.models.ingestion import SourceSnapshot
from app.db.models.territory import CountyCatalog, MetroCatalog


def test_admin_source_load_endpoint_returns_created_report(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _seed_reference_catalogs(session_factory)

    response = client.post(
        "/admin/sources/PARCEL/metros/DFW/loads",
        json={
            "source_version": "parcel_csv_v1",
            "snapshot_ts": "2026-04-13T20:00:00Z",
            "records": [
                {
                    "parcel_id": "P-API-1",
                    "county_fips": "48085",
                    "acreage": "30.0",
                    "geometry_wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                    "lineage_key": "parcel:P-API-1",
                }
            ],
        },
    )

    payload = response.json()

    assert response.status_code == 201
    assert payload["status"] == "success"
    assert payload["accepted_count"] == 1
    assert payload["rejected_count"] == 0


def test_admin_source_freshness_endpoint_returns_operator_report(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _seed_reference_catalogs(session_factory)
    evaluation_time = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    _seed_snapshot(session_factory, "PARCEL", "DFW", evaluation_time - timedelta(hours=4))
    _seed_snapshot(session_factory, "ZONING", "DFW", evaluation_time - timedelta(hours=4))
    _seed_snapshot(session_factory, "FLOOD", "DFW", evaluation_time - timedelta(hours=4))

    response = client.get("/admin/sources/freshness/DFW")
    payload = response.json()

    assert response.status_code == 200
    assert payload["metro_id"] == "DFW"
    assert payload["passed"] is True
    assert {status["source_id"] for status in payload["statuses"]} >= {"PARCEL", "ZONING", "FLOOD"}


def test_admin_source_health_endpoint_returns_snapshot_counts(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _seed_reference_catalogs(session_factory)

    client.post(
        "/admin/sources/PARCEL/metros/DFW/loads",
        json={
            "source_version": "parcel_csv_v1",
            "snapshot_ts": "2026-04-13T22:00:00Z",
            "records": [
                {
                    "parcel_id": "P-API-2",
                    "county_fips": "48085",
                    "acreage": "11.0",
                    "geometry_wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                    "lineage_key": "parcel:P-API-2",
                },
                {
                    "parcel_id": "P-API-3",
                    "county_fips": "99999",
                    "acreage": "11.0",
                    "geometry_wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                    "lineage_key": "parcel:P-API-3",
                },
            ],
        },
    )

    response = client.get("/admin/sources/health", params={"metro_id": "DFW"})
    payload = response.json()
    parcel_health = next(item for item in payload if item["source_id"] == "PARCEL")

    assert response.status_code == 200
    assert parcel_health["latest_snapshot_status"] == "quarantined"
    assert parcel_health["row_count"] == 2
    assert parcel_health["accepted_count"] == 1
    assert parcel_health["rejected_count"] == 1


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
                SourceCatalog(
                    source_id="UTILITY",
                    display_name="Utility Evidence Feed",
                    owner_name="Data Governance",
                    refresh_cadence="weekly",
                    block_refresh=False,
                    metro_coverage="DFW",
                    target_table_name="source_evidence",
                    is_active=True,
                ),
            ]
        )
        session.commit()
    finally:
        session.close()


def _seed_snapshot(
    session_factory: sessionmaker[Session],
    source_id: str,
    metro_id: str,
    snapshot_ts: datetime,
    status: SourceSnapshotStatus = SourceSnapshotStatus.SUCCESS,
) -> None:
    session = session_factory()
    try:
        session.add(
            SourceSnapshot(
                source_id=source_id,
                metro_id=metro_id,
                snapshot_ts=snapshot_ts,
                source_version=f"{source_id.lower()}_v1",
                row_count=1,
                checksum=f"{source_id}-checksum",
                status=status,
            )
        )
        session.commit()
    finally:
        session.close()
