from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

from app.db.models.batching import ScoreRun
from app.db.models.catalogs import SourceCatalog
from app.db.models.enums import ScoreRunStatus, SourceSnapshotStatus
from app.db.models.ingestion import SourceSnapshot
from app.db.models.territory import CountyCatalog, MetroCatalog
from orchestrator.service import create_batch, reconcile_batch


def test_admin_monitoring_overview_returns_scoped_operator_summary(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _seed_monitoring_context(session_factory)

    response = client.get("/admin/monitoring/overview", params={"metro_id": "DFW"})

    assert response.status_code == 200
    payload = response.json()
    alert_codes = {alert["code"] for alert in payload["alerts"]}
    threshold_by_code = {item["code"]: item for item in payload["thresholds"]}
    run_counts = {item["status"]: item["count"] for item in payload["run_status_counts"]}
    batch_counts = {item["status"]: item["count"] for item in payload["batch_status_counts"]}

    assert payload["metro_id"] == "DFW"
    assert payload["freshness"]["passed"] is False
    assert payload["latest_batch"]["status"] == "failed"
    assert payload["recent_failed_runs"][0]["metro_id"] == "DFW"
    assert run_counts["failed"] == 1
    assert batch_counts["failed"] == 1
    assert payload["threshold_trigger_count"] == 5
    assert threshold_by_code["FAILED_RUN_COUNT"]["triggered"] is True
    assert threshold_by_code["FAILED_RUN_COUNT"]["observed_value"] == 1
    assert threshold_by_code["FAILED_SNAPSHOT_COUNT"]["observed_value"] == 1
    assert threshold_by_code["QUARANTINED_SNAPSHOT_COUNT"]["observed_value"] == 1
    assert threshold_by_code["FRESHNESS_FAILURE_COUNT"]["observed_value"] == 2
    assert threshold_by_code["LATEST_BATCH_FAILED_COUNT"]["observed_value"] == 1
    assert {
        "FAILED_RUN",
        "FRESHNESS_FAILURE",
        "LATEST_BATCH_FAILED",
        "SOURCE_SNAPSHOT_FAILED",
        "SOURCE_SNAPSHOT_QUARANTINED",
    }.issubset(alert_codes)


def test_admin_monitoring_overview_without_metro_returns_global_counts(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _seed_monitoring_context(session_factory)

    response = client.get("/admin/monitoring/overview")

    assert response.status_code == 200
    payload = response.json()
    run_counts = {item["status"]: item["count"] for item in payload["run_status_counts"]}

    assert payload["metro_id"] is None
    assert payload["freshness"] is None
    assert payload["latest_batch"]["status"] == "failed"
    assert run_counts["failed"] == 1
    assert run_counts["running"] == 1
    assert payload["threshold_trigger_count"] == 4


def _seed_monitoring_context(session_factory: sessionmaker[Session]) -> None:
    session = session_factory()
    try:
        now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)

        session.add(MetroCatalog(metro_id="DFW", display_name="Dallas-Fort Worth", state_code="TX"))
        session.add(MetroCatalog(metro_id="AUS", display_name="Austin", state_code="TX"))
        session.add(
            CountyCatalog(
                county_fips="48085",
                metro_id="DFW",
                display_name="Collin",
                state_code="TX",
            )
        )
        session.add(
            CountyCatalog(
                county_fips="48453",
                metro_id="AUS",
                display_name="Travis",
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
                    row_count=10,
                    checksum="parcel-checksum",
                    status=SourceSnapshotStatus.SUCCESS,
                ),
                SourceSnapshot(
                    source_id="ZONING",
                    metro_id="DFW",
                    snapshot_ts=now - timedelta(hours=1),
                    source_version="zoning_v1",
                    row_count=8,
                    checksum="zoning-checksum",
                    status=SourceSnapshotStatus.QUARANTINED,
                    error_message="2 rows quarantined.",
                ),
                SourceSnapshot(
                    source_id="FLOOD",
                    metro_id="DFW",
                    snapshot_ts=now - timedelta(hours=1),
                    source_version="flood_v1",
                    row_count=5,
                    checksum="flood-checksum",
                    status=SourceSnapshotStatus.FAILED,
                    error_message="Source unavailable.",
                ),
            ]
        )
        session.commit()

        batch = create_batch(session, ["DFW", "AUS"])
    finally:
        session.close()

    session = session_factory()
    try:
        runs = (
            session.query(ScoreRun)
            .filter(ScoreRun.batch_id == UUID(batch.batch_id))
            .order_by(ScoreRun.metro_id)
            .all()
        )
        dfw_run = next(run for run in runs if run.metro_id == "DFW")
        dfw_run.status = ScoreRunStatus.FAILED
        dfw_run.failure_reason = "MISSING_SOURCE"
        dfw_run.completed_at = now
        session.commit()
        reconcile_batch(session, batch.batch_id)
    finally:
        session.close()
