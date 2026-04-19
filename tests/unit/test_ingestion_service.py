from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from sqlalchemy.orm import Session

from app.db.models.catalogs import SourceCatalog
from app.db.models.enums import SourceSnapshotStatus
from app.db.models.ingestion import SourceSnapshot
from app.db.models.source_data import RawZoning, SourceEvidence, SourceRecordRejection
from app.db.models.territory import CountyCatalog, MetroCatalog, ParcelRepPoint, RawParcel
from ingestion.service import (
    evaluate_freshness,
    ingest_evidence_records,
    ingest_parcel_records,
    ingest_zoning_records,
    summarize_source_health,
)


def test_ingest_parcel_records_persists_canonical_parcels_and_rep_points(
    db_session: Session,
) -> None:
    _seed_reference_catalogs(db_session)

    report = ingest_parcel_records(
        session=db_session,
        metro_id="dfw",
        source_version="parcel_csv_v1",
        records=[
            {
                "parcel_id": "P-100",
                "county_fips": "48085",
                "acreage": "25.5",
                "geometry_wkt": "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))",
                "lineage_key": "parcel:P-100",
            }
        ],
        loaded_at=datetime(2026, 4, 13, 12, 0, tzinfo=UTC),
    )

    stored_parcel = db_session.get(RawParcel, "P-100")
    stored_rep_point = db_session.get(ParcelRepPoint, "P-100")
    stored_snapshot = db_session.get(SourceSnapshot, UUID(report.snapshot_id))

    assert report.status == "success"
    assert report.accepted_count == 1
    assert report.rejected_count == 0
    assert stored_parcel is not None
    assert stored_parcel.metro_id == "DFW"
    assert stored_parcel.acreage == Decimal("25.5")
    assert stored_rep_point is not None
    assert stored_rep_point.rep_point_wkt.startswith("POINT")
    assert stored_snapshot is not None
    assert stored_snapshot.status is SourceSnapshotStatus.SUCCESS


def test_ingest_parcel_records_quarantines_invalid_rows_without_dropping_valid_rows(
    db_session: Session,
) -> None:
    _seed_reference_catalogs(db_session)

    report = ingest_parcel_records(
        session=db_session,
        metro_id="DFW",
        source_version="parcel_csv_v1",
        records=[
            {
                "parcel_id": "P-200",
                "county_fips": "48085",
                "acreage": "12.0",
                "geometry_wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                "lineage_key": "parcel:P-200",
            },
            {
                "parcel_id": "P-201",
                "county_fips": "99999",
                "acreage": "8.0",
                "geometry_wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                "lineage_key": "parcel:P-201",
            },
        ],
        loaded_at=datetime(2026, 4, 13, 13, 0, tzinfo=UTC),
    )

    stored_snapshot = db_session.get(SourceSnapshot, UUID(report.snapshot_id))
    rejection_count = db_session.query(SourceRecordRejection).count()

    assert report.status == "quarantined"
    assert report.accepted_count == 1
    assert report.rejected_count == 1
    assert db_session.get(RawParcel, "P-200") is not None
    assert db_session.get(RawParcel, "P-201") is None
    assert stored_snapshot is not None
    assert stored_snapshot.status is SourceSnapshotStatus.QUARANTINED
    assert rejection_count == 1


def test_ingest_zoning_records_persists_canonical_zoning_rows(db_session: Session) -> None:
    _seed_reference_catalogs(db_session)
    ingest_parcel_records(
        session=db_session,
        metro_id="DFW",
        source_version="parcel_csv_v1",
        records=[
            {
                "parcel_id": "P-300",
                "county_fips": "48085",
                "acreage": "18.0",
                "geometry_wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                "lineage_key": "parcel:P-300",
            }
        ],
        loaded_at=datetime(2026, 4, 13, 14, 0, tzinfo=UTC),
    )

    report = ingest_zoning_records(
        session=db_session,
        metro_id="DFW",
        source_version="zoning_csv_v1",
        records=[
            {
                "parcel_id": "P-300",
                "county_fips": "48085",
                "zoning_code": "LI",
                "land_use_code": "INDUSTRIAL",
                "lineage_key": "zoning:P-300",
            }
        ],
        loaded_at=datetime(2026, 4, 13, 15, 0, tzinfo=UTC),
    )

    zoning_rows = db_session.query(RawZoning).all()

    assert report.status == "success"
    assert len(zoning_rows) == 1
    assert zoning_rows[0].parcel_id == "P-300"
    assert zoning_rows[0].zoning_code == "LI"


def test_ingest_evidence_records_persists_generic_source_evidence(db_session: Session) -> None:
    _seed_reference_catalogs(db_session)
    ingest_parcel_records(
        session=db_session,
        metro_id="DFW",
        source_version="parcel_csv_v1",
        records=[
            {
                "parcel_id": "P-400",
                "county_fips": "48085",
                "acreage": "20.0",
                "geometry_wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                "lineage_key": "parcel:P-400",
            }
        ],
        loaded_at=datetime(2026, 4, 13, 16, 0, tzinfo=UTC),
    )

    report = ingest_evidence_records(
        session=db_session,
        source_id="FLOOD",
        metro_id="DFW",
        source_version="flood_evidence_v1",
        records=[
            {
                "record_key": "flood:P-400",
                "parcel_id": "P-400",
                "county_fips": "48085",
                "attribute_name": "fema_zone",
                "attribute_value": "X",
                "lineage_key": "flood:P-400:fema_zone",
            }
        ],
        loaded_at=datetime(2026, 4, 13, 17, 0, tzinfo=UTC),
    )

    evidence_rows = db_session.query(SourceEvidence).all()

    assert report.status == "success"
    assert len(evidence_rows) == 1
    assert evidence_rows[0].source_id == "FLOOD"
    assert evidence_rows[0].attribute_value == "X"


def test_freshness_gate_fails_for_missing_required_sources(db_session: Session) -> None:
    _seed_reference_catalogs(db_session)

    report = evaluate_freshness(
        db_session,
        "DFW",
        evaluated_at=datetime(2026, 4, 13, 18, 0, tzinfo=UTC),
    )
    failures = {
        status.source_id: status.freshness_code
        for status in report.statuses
        if not status.passed
    }

    assert report.passed is False
    assert failures["PARCEL"] == "MISSING_SOURCE"
    assert failures["ZONING"] == "MISSING_SOURCE"
    assert failures["FLOOD"] == "MISSING_SOURCE"


def test_freshness_gate_allows_stale_non_blocking_source_when_required_sources_are_fresh(
    db_session: Session,
) -> None:
    _seed_reference_catalogs(db_session)
    evaluation_time = datetime(2026, 4, 13, 18, 0, tzinfo=UTC)
    _seed_snapshot(db_session, "PARCEL", "DFW", evaluation_time - timedelta(hours=6))
    _seed_snapshot(db_session, "ZONING", "DFW", evaluation_time - timedelta(hours=6))
    _seed_snapshot(db_session, "FLOOD", "DFW", evaluation_time - timedelta(hours=6))
    _seed_snapshot(db_session, "UTILITY", "DFW", evaluation_time - timedelta(days=12))

    report = evaluate_freshness(db_session, "DFW", evaluated_at=evaluation_time)
    utility_status = next(status for status in report.statuses if status.source_id == "UTILITY")

    assert report.passed is True
    assert utility_status.freshness_code == "STALE_SOURCE"
    assert utility_status.passed is True


def test_source_health_summary_reports_accepted_and_rejected_counts(db_session: Session) -> None:
    _seed_reference_catalogs(db_session)
    ingest_parcel_records(
        session=db_session,
        metro_id="DFW",
        source_version="parcel_csv_v1",
        records=[
            {
                "parcel_id": "P-500",
                "county_fips": "48085",
                "acreage": "14.0",
                "geometry_wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                "lineage_key": "parcel:P-500",
            },
            {
                "parcel_id": "P-501",
                "county_fips": "99999",
                "acreage": "14.0",
                "geometry_wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                "lineage_key": "parcel:P-501",
            },
        ],
        loaded_at=datetime(2026, 4, 13, 19, 0, tzinfo=UTC),
    )

    summaries = summarize_source_health(db_session, metro_id="DFW")
    parcel_summary = next(summary for summary in summaries if summary.source_id == "PARCEL")

    assert parcel_summary.latest_snapshot_status == "quarantined"
    assert parcel_summary.row_count == 2
    assert parcel_summary.accepted_count == 1
    assert parcel_summary.rejected_count == 1


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
            SourceCatalog(
                source_id="MARKET",
                display_name="Market Proxy Feed",
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


def _seed_snapshot(
    session: Session,
    source_id: str,
    metro_id: str,
    snapshot_ts: datetime,
    status: SourceSnapshotStatus = SourceSnapshotStatus.SUCCESS,
) -> None:
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
