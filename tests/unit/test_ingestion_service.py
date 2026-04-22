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
from ingestion import service as ingestion_service
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


def test_ingest_parcel_records_quarantines_oversized_acreage_values(
    db_session: Session,
) -> None:
    _seed_reference_catalogs(db_session)

    report = ingest_parcel_records(
        session=db_session,
        metro_id="DFW",
        source_version="parcel_csv_v1",
        records=[
            {
                "parcel_id": "P-210",
                "county_fips": "48085",
                "acreage": "15.0",
                "geometry_wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                "lineage_key": "parcel:P-210",
            },
            {
                "parcel_id": "P-211",
                "county_fips": "48085",
                "acreage": "10000000000",
                "geometry_wkt": "POLYGON ((2 0, 2 1, 3 1, 3 0, 2 0))",
                "lineage_key": "parcel:P-211",
            },
        ],
        loaded_at=datetime(2026, 4, 13, 13, 15, tzinfo=UTC),
    )

    stored_snapshot = db_session.get(SourceSnapshot, UUID(report.snapshot_id))

    assert report.status == "quarantined"
    assert report.accepted_count == 1
    assert report.rejected_count == 1
    assert db_session.get(RawParcel, "P-210") is not None
    assert db_session.get(RawParcel, "P-211") is None
    assert stored_snapshot is not None
    assert stored_snapshot.status is SourceSnapshotStatus.QUARANTINED


def test_ingest_parcel_records_treats_duplicate_only_rows_as_success(
    db_session: Session,
) -> None:
    _seed_reference_catalogs(db_session)

    report = ingest_parcel_records(
        session=db_session,
        metro_id="DFW",
        source_version="parcel_csv_v1",
        records=[
            {
                "parcel_id": "P-220",
                "county_fips": "48085",
                "acreage": "4.0",
                "geometry_wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                "lineage_key": "parcel:P-220",
            },
            {
                "parcel_id": "P-220",
                "county_fips": "48085",
                "acreage": "4.0",
                "geometry_wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                "lineage_key": "parcel:P-220",
            },
        ],
        loaded_at=datetime(2026, 4, 13, 13, 20, tzinfo=UTC),
    )

    stored_snapshot = db_session.get(SourceSnapshot, UUID(report.snapshot_id))
    rejection_rows = db_session.query(SourceRecordRejection).all()

    assert report.status == "success"
    assert report.accepted_count == 1
    assert report.rejected_count == 1
    assert stored_snapshot is not None
    assert stored_snapshot.status is SourceSnapshotStatus.SUCCESS
    assert "duplicate row" in (stored_snapshot.error_message or "").lower()
    assert len(rejection_rows) == 1
    assert rejection_rows[0].rejection_code == "DUPLICATE_RECORD"


def test_ingest_parcel_records_treats_minor_validation_rejections_as_success(
    db_session: Session,
) -> None:
    _seed_reference_catalogs(db_session)

    records = [
        {
            "parcel_id": f"P-23{index:02d}",
            "county_fips": "48085",
            "acreage": "3.0",
            "geometry_wkt": (
                f"POLYGON (({index} 0, {index} 1, {index + 0.5} 1, "
                f"{index + 0.5} 0, {index} 0))"
            ),
            "lineage_key": f"parcel:P-23{index:02d}",
        }
        for index in range(100)
    ]
    records.append(
        {
            "parcel_id": "P-2499",
            "county_fips": "99999",
            "acreage": "3.0",
            "geometry_wkt": "POLYGON ((200 0, 200 1, 201 1, 201 0, 200 0))",
            "lineage_key": "parcel:P-2499",
        }
    )

    report = ingest_parcel_records(
        session=db_session,
        metro_id="DFW",
        source_version="parcel_csv_v1",
        records=records,
        loaded_at=datetime(2026, 4, 13, 13, 25, tzinfo=UTC),
    )

    stored_snapshot = db_session.get(SourceSnapshot, UUID(report.snapshot_id))

    assert report.status == "success"
    assert report.accepted_count == 100
    assert report.rejected_count == 1
    assert stored_snapshot is not None
    assert stored_snapshot.status is SourceSnapshotStatus.SUCCESS
    assert "validation_failure=1" in (stored_snapshot.error_message or "").lower()


def test_ingest_parcel_records_replace_existing_scope_county_metro_replaces_only_target_county(
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

    initial_report = ingest_parcel_records(
        session=db_session,
        metro_id="DFW",
        source_version="parcel_csv_v1",
        records=[
            {
                "parcel_id": "DAL-100",
                "county_fips": "48113",
                "acreage": "12.0",
                "geometry_wkt": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                "lineage_key": "parcel:DAL-100",
            },
            {
                "parcel_id": "COL-100",
                "county_fips": "48085",
                "acreage": "8.5",
                "geometry_wkt": "POLYGON ((2 0, 2 1, 3 1, 3 0, 2 0))",
                "lineage_key": "parcel:COL-100",
            },
        ],
        loaded_at=datetime(2026, 4, 13, 13, 30, tzinfo=UTC),
    )

    replacement_report = ingest_parcel_records(
        session=db_session,
        metro_id="DFW",
        source_version="parcel_csv_v2",
        replace_existing_scope="county_metro",
        records=[
            {
                "parcel_id": "DAL-200",
                "county_fips": "48113",
                "acreage": "16.0",
                "geometry_wkt": "POLYGON ((10 0, 10 1, 11 1, 11 0, 10 0))",
                "lineage_key": "parcel:DAL-200",
            }
        ],
        loaded_at=datetime(2026, 4, 13, 14, 0, tzinfo=UTC),
    )

    old_dallas = db_session.get(RawParcel, "DAL-100")
    new_dallas = db_session.get(RawParcel, "DAL-200")
    collin = db_session.get(RawParcel, "COL-100")

    assert initial_report.status == "success"
    assert replacement_report.status == "success"
    assert old_dallas is not None
    assert old_dallas.is_active is False
    assert new_dallas is not None
    assert new_dallas.is_active is True
    assert collin is not None
    assert collin.is_active is True


def test_chunked_strings_splits_large_identifier_sets() -> None:
    identifiers = [f"P-{index}" for index in range(12_005)]

    batches = ingestion_service._chunked_strings(identifiers, batch_size=5_000)

    assert [len(batch) for batch in batches] == [5_000, 5_000, 2_005]
    assert batches[0][0] == "P-0"
    assert batches[-1][-1] == "P-12004"


def test_load_existing_parcel_helpers_return_all_rows_from_multiple_batches(
    db_session: Session,
) -> None:
    _seed_reference_catalogs(db_session)
    parcel_count = 5_200
    db_session.add_all(
        [
            RawParcel(
                parcel_id=f"P-{index}",
                county_fips="48085",
                metro_id="DFW",
                acreage=Decimal("1.0"),
                geometry_wkt="POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                lineage_key=f"parcel:P-{index}",
                is_active=True,
            )
            for index in range(parcel_count)
        ]
    )
    db_session.add_all(
        [
            ParcelRepPoint(
                parcel_id=f"P-{index}",
                rep_point_wkt="POINT (0.5 0.5)",
                geometry_method="representative_point",
            )
            for index in range(parcel_count)
        ]
    )
    db_session.commit()

    parcel_ids = [f"P-{index}" for index in range(parcel_count)]
    existing_parcels = ingestion_service._load_existing_parcels_by_id(db_session, parcel_ids)
    existing_rep_points = ingestion_service._load_existing_rep_points_by_id(
        db_session, parcel_ids
    )

    assert len(existing_parcels) == parcel_count
    assert len(existing_rep_points) == parcel_count
    assert existing_parcels["P-0"].parcel_id == "P-0"
    assert existing_rep_points["P-5199"].parcel_id == "P-5199"


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


def test_ingest_evidence_records_ignores_exact_duplicate_payload_rows(
    db_session: Session,
) -> None:
    _seed_reference_catalogs(db_session)

    report = ingest_evidence_records(
        session=db_session,
        source_id="FLOOD",
        metro_id="DFW",
        source_version="flood_evidence_v1",
        records=[
            {
                "record_key": "flood:tract-100",
                "attribute_name": "fema_zone",
                "attribute_value": "X",
                "lineage_key": "flood:tract-100:fema_zone:first",
            },
            {
                "record_key": "flood:tract-100",
                "attribute_name": "fema_zone",
                "attribute_value": "X",
                "lineage_key": "flood:tract-100:fema_zone:second",
            },
        ],
        loaded_at=datetime(2026, 4, 13, 17, 30, tzinfo=UTC),
    )

    evidence_rows = db_session.query(SourceEvidence).all()
    rejection_rows = db_session.query(SourceRecordRejection).all()
    stored_snapshot = db_session.get(SourceSnapshot, UUID(report.snapshot_id))

    assert report.status == "success"
    assert report.rejected_count == 0
    assert len(evidence_rows) == 1
    assert len(rejection_rows) == 0
    assert stored_snapshot is not None
    assert stored_snapshot.status is SourceSnapshotStatus.SUCCESS
    assert evidence_rows[0].record_key == "flood:tract-100"
    assert evidence_rows[0].attribute_name == "fema_zone"
    assert evidence_rows[0].attribute_value == "X"


def test_ingest_evidence_records_quarantines_conflicting_duplicate_payload_rows(
    db_session: Session,
) -> None:
    _seed_reference_catalogs(db_session)

    report = ingest_evidence_records(
        session=db_session,
        source_id="FLOOD",
        metro_id="DFW",
        source_version="flood_evidence_v1",
        records=[
            {
                "record_key": "flood:tract-101",
                "attribute_name": "fema_zone",
                "attribute_value": "X",
                "lineage_key": "flood:tract-101:fema_zone:first",
            },
            {
                "record_key": "flood:tract-101",
                "attribute_name": "fema_zone",
                "attribute_value": "AE",
                "lineage_key": "flood:tract-101:fema_zone:second",
            },
        ],
        loaded_at=datetime(2026, 4, 13, 17, 45, tzinfo=UTC),
    )

    evidence_rows = db_session.query(SourceEvidence).all()
    rejection_rows = db_session.query(SourceRecordRejection).all()
    stored_snapshot = db_session.get(SourceSnapshot, UUID(report.snapshot_id))

    assert report.status == "quarantined"
    assert report.accepted_count == 1
    assert report.rejected_count == 1
    assert len(evidence_rows) == 1
    assert len(rejection_rows) == 1
    assert rejection_rows[0].rejection_code == "DUPLICATE_RECORD"
    assert "Conflicting evidence" in rejection_rows[0].rejection_message
    assert stored_snapshot is not None
    assert stored_snapshot.status is SourceSnapshotStatus.QUARANTINED
    assert evidence_rows[0].attribute_value == "X"


def test_ingest_evidence_records_replace_existing_scope_source_metro_replaces_active_rows(
    db_session: Session,
) -> None:
    _seed_reference_catalogs(db_session)

    initial_report = ingest_evidence_records(
        session=db_session,
        source_id="FLOOD",
        metro_id="DFW",
        source_version="flood_evidence_v1",
        records=[
            {
                "record_key": "flood:tract-200",
                "attribute_name": "fema_zone",
                "attribute_value": "X",
                "lineage_key": "flood:tract-200:fema_zone",
            },
            {
                "record_key": "flood:tract-201",
                "attribute_name": "fema_zone",
                "attribute_value": "AE",
                "lineage_key": "flood:tract-201:fema_zone",
            },
        ],
        loaded_at=datetime(2026, 4, 13, 18, 0, tzinfo=UTC),
    )
    replacement_report = ingest_evidence_records(
        session=db_session,
        source_id="FLOOD",
        metro_id="DFW",
        source_version="flood_evidence_v2",
        records=[
            {
                "record_key": "flood:tract-202",
                "attribute_name": "fema_zone",
                "attribute_value": "AO",
                "lineage_key": "flood:tract-202:fema_zone",
            }
        ],
        loaded_at=datetime(2026, 4, 13, 18, 15, tzinfo=UTC),
        replace_existing_scope="source_metro",
    )

    active_rows = (
        db_session.query(SourceEvidence)
        .filter_by(source_id="FLOOD", metro_id="DFW", is_active=True)
        .order_by(SourceEvidence.record_key)
        .all()
    )
    inactive_rows = (
        db_session.query(SourceEvidence)
        .filter_by(source_id="FLOOD", metro_id="DFW", is_active=False)
        .order_by(SourceEvidence.record_key)
        .all()
    )

    assert initial_report.status == "success"
    assert replacement_report.status == "success"
    assert [row.record_key for row in active_rows] == ["flood:tract-202"]
    assert {row.record_key for row in inactive_rows} == {
        "flood:tract-200",
        "flood:tract-201",
    }
    assert active_rows[0].source_snapshot_id == UUID(replacement_report.snapshot_id)


def test_ingest_evidence_records_replace_existing_scope_preserves_prior_rows_when_all_rows_fail_validation(  # noqa: E501
    db_session: Session,
) -> None:
    _seed_reference_catalogs(db_session)

    ingest_evidence_records(
        session=db_session,
        source_id="FLOOD",
        metro_id="DFW",
        source_version="flood_evidence_v1",
        records=[
            {
                "record_key": "flood:tract-300",
                "attribute_name": "fema_zone",
                "attribute_value": "X",
                "lineage_key": "flood:tract-300:fema_zone",
            }
        ],
        loaded_at=datetime(2026, 4, 13, 18, 30, tzinfo=UTC),
    )
    failed_report = ingest_evidence_records(
        session=db_session,
        source_id="FLOOD",
        metro_id="DFW",
        source_version="flood_evidence_v2",
        records=[
            {
                "record_key": "flood:tract-301",
                "county_fips": "99999",
                "attribute_name": "fema_zone",
                "attribute_value": "AE",
                "lineage_key": "flood:tract-301:fema_zone",
            }
        ],
        loaded_at=datetime(2026, 4, 13, 18, 45, tzinfo=UTC),
        replace_existing_scope="source_metro",
    )

    active_rows = (
        db_session.query(SourceEvidence)
        .filter_by(source_id="FLOOD", metro_id="DFW", is_active=True)
        .order_by(SourceEvidence.record_key)
        .all()
    )

    assert failed_report.status == "quarantined"
    assert failed_report.accepted_count == 0
    assert failed_report.rejected_count == 1
    assert [row.record_key for row in active_rows] == ["flood:tract-300"]


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


def test_freshness_gate_uses_texas_shared_snapshots_for_texas_metros(
    db_session: Session,
) -> None:
    _seed_reference_catalogs(db_session)
    db_session.add(
        MetroCatalog(metro_id="TX", display_name="Texas Statewide", state_code="TX")
    )
    db_session.add(
        SourceCatalog(
            source_id="IF-001",
            display_name="HIFLD Substations",
            owner_name="Federal / State Program",
            refresh_cadence="daily",
            block_refresh=False,
            metro_coverage="TX",
            target_table_name="source_evidence",
            is_active=True,
        )
    )
    db_session.commit()

    evaluation_time = datetime(2026, 4, 18, 18, 0, tzinfo=UTC)
    _seed_snapshot(db_session, "IF-001", "TX", evaluation_time - timedelta(hours=2))

    report = evaluate_freshness(db_session, "DFW", evaluated_at=evaluation_time)
    statewide_status = next(status for status in report.statuses if status.source_id == "IF-001")

    assert statewide_status.freshness_code == "FRESH"
    assert statewide_status.passed is True
    assert statewide_status.latest_snapshot_ts is not None


def test_ingest_evidence_records_accepts_texas_statewide_sources_for_texas_metros(
    db_session: Session,
) -> None:
    _seed_reference_catalogs(db_session)
    db_session.add(
        SourceCatalog(
            source_id="IF-026",
            display_name="OSM Overpass (Highways)",
            owner_name="Federal / State Program",
            refresh_cadence="daily",
            block_refresh=False,
            metro_coverage="TX",
            target_table_name="highway_corridors",
            is_active=True,
        )
    )
    db_session.commit()

    report = ingest_evidence_records(
        session=db_session,
        source_id="IF-026",
        metro_id="DFW",
        source_version="if-026-test-v1",
        records=[
            {
                "record_key": "osm-way:4342988",
                "attribute_name": "highway_type",
                "attribute_value": "motorway",
                "lineage_key": "if026:osm-way:4342988:highway_type",
            }
        ],
        loaded_at=datetime(2026, 4, 18, 20, 0, tzinfo=UTC),
    )

    evidence_row = db_session.query(SourceEvidence).filter_by(source_id="IF-026").one()

    assert report.status == "success"
    assert report.accepted_count == 1
    assert evidence_row.metro_id == "DFW"


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
