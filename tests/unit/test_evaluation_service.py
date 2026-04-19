from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from sqlalchemy.orm import Session

from app.db.models.batching import ScoreBatch, ScoreRun
from app.db.models.catalogs import FactorCatalog, SourceCatalog
from app.db.models.enums import (
    ParcelEvaluationStatus,
    ScoreBatchStatus,
    ScoreRunStatus,
    SourceSnapshotStatus,
)
from app.db.models.evaluation import ParcelEvaluation, ParcelExclusionEvent
from app.db.models.ingestion import SourceSnapshot
from app.db.models.scoring import ScoreFactorDetail
from app.db.models.source_data import RawZoning, SourceEvidence
from app.db.models.territory import (
    CountyCatalog,
    MetroCatalog,
    ParcelRepPoint,
    RawParcel,
)
from evaluation.models import EvaluationPolicy, EvidenceExclusionRule
from evaluation.service import (
    EvaluationReplayBlockedError,
    evaluate_run,
    get_evaluation_summary,
)


def test_evaluate_run_categorizes_parcels_and_logs_exclusions(db_session: Session) -> None:
    run_id = _seed_evaluation_context(db_session)

    summary = evaluate_run(
        db_session,
        run_id,
        EvaluationPolicy(
            rule_version="phase4-r1",
            allowed_band_wkt="POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))",
            minimum_acreage=Decimal("10"),
            blocked_zoning_codes=("RES",),
            evidence_exclusion_rules=(
                EvidenceExclusionRule(
                    source_id="FLOOD",
                    attribute_name="fema_zone",
                    blocked_values=("AE",),
                    exclusion_code="FLOOD_ZONE_BLOCKED",
                    exclusion_reason="Flood zone AE is excluded.",
                ),
            ),
        ),
    )

    evaluations = db_session.query(ParcelEvaluation).all()
    events = db_session.query(ParcelExclusionEvent).all()
    statuses = {evaluation.parcel_id: evaluation.status for evaluation in evaluations}

    assert summary.run_status == "running"
    assert summary.evaluated_count == 5
    assert summary.band_filtered_count == 1
    assert summary.size_filtered_count == 1
    assert summary.excluded_count == 2
    assert summary.pending_scoring_count == 1
    assert summary.pending_exclusion_check_count == 0
    assert statuses["P-SURVIVE"] is ParcelEvaluationStatus.PENDING_SCORING
    assert statuses["P-BAND"] is ParcelEvaluationStatus.PREFILTERED_BAND
    assert statuses["P-SIZE"] is ParcelEvaluationStatus.PREFILTERED_SIZE
    assert statuses["P-ZONING"] is ParcelEvaluationStatus.EXCLUDED
    assert statuses["P-FLOOD"] is ParcelEvaluationStatus.EXCLUDED
    assert len(events) == 2
    assert {event.exclusion_code for event in events} == {
        "ZONING_BLOCKED",
        "FLOOD_ZONE_BLOCKED",
    }


def test_evaluate_run_is_idempotent_for_same_run(db_session: Session) -> None:
    run_id = _seed_evaluation_context(db_session)
    policy = EvaluationPolicy(
        rule_version="phase4-r1",
        allowed_band_wkt="POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))",
        minimum_acreage=Decimal("10"),
        blocked_zoning_codes=("RES",),
    )

    evaluate_run(db_session, run_id, policy)
    evaluate_run(db_session, run_id, policy)

    evaluations = db_session.query(ParcelEvaluation).all()
    events = db_session.query(ParcelExclusionEvent).all()

    assert len(evaluations) == 5
    assert len(events) == 1


def test_evaluate_run_fails_when_freshness_gate_fails(db_session: Session) -> None:
    run_id = _seed_evaluation_context(db_session, include_fresh_snapshots=False)

    summary = evaluate_run(
        db_session,
        run_id,
        EvaluationPolicy(rule_version="phase4-r1"),
    )
    run = db_session.get(ScoreRun, UUID(run_id))
    batch = db_session.get(ScoreBatch, UUID(summary.batch_id))

    assert summary.run_status == "failed"
    assert summary.failure_reason == "MISSING_SOURCE"
    assert summary.evaluated_count == 0
    assert run is not None
    assert run.status is ScoreRunStatus.FAILED
    assert batch is not None
    assert batch.status is ScoreBatchStatus.FAILED
    assert db_session.query(ParcelEvaluation).count() == 0


def test_evaluate_run_blocks_replay_when_scoring_outputs_exist(db_session: Session) -> None:
    run_id = _seed_evaluation_context(db_session)
    run = db_session.get(ScoreRun, UUID(run_id))

    assert run is not None

    db_session.add(
        FactorCatalog(
            factor_id="POWER",
            display_name="Power availability",
            description="Power readiness factor",
            ordinal=1,
            is_active=True,
        )
    )
    db_session.flush()
    db_session.add(
        ScoreFactorDetail(
            run_id=run.run_id,
            parcel_id="P-SURVIVE",
            factor_id="POWER",
            points_awarded=Decimal("10"),
            rationale="Seeded scoring output for replay protection.",
        )
    )
    db_session.commit()

    with pytest.raises(EvaluationReplayBlockedError):
        evaluate_run(db_session, run_id, EvaluationPolicy(rule_version="phase4-r1"))


def test_get_evaluation_summary_returns_status_counts(db_session: Session) -> None:
    run_id = _seed_evaluation_context(db_session)
    evaluate_run(
        db_session,
        run_id,
        EvaluationPolicy(
            rule_version="phase4-r1",
            allowed_band_wkt="POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))",
            minimum_acreage=Decimal("10"),
            blocked_zoning_codes=("RES",),
        ),
    )

    summary = get_evaluation_summary(db_session, run_id, rule_version="phase4-r1")
    status_counts = {item.status: item.count for item in summary.status_counts}

    assert status_counts["excluded"] == 1
    assert status_counts["pending_scoring"] == 2
    assert status_counts["prefiltered_band"] == 1
    assert status_counts["prefiltered_size"] == 1


def _seed_evaluation_context(
    session: Session,
    *,
    include_fresh_snapshots: bool = True,
) -> str:
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

    if include_fresh_snapshots:
        session.add_all(
            [
                SourceSnapshot(
                    source_id="PARCEL",
                    metro_id="DFW",
                    snapshot_ts=now - timedelta(hours=1),
                    source_version="parcel_v1",
                    row_count=5,
                    checksum="parcel-checksum",
                    status=SourceSnapshotStatus.SUCCESS,
                ),
                SourceSnapshot(
                    source_id="ZONING",
                    metro_id="DFW",
                    snapshot_ts=now - timedelta(hours=1),
                    source_version="zoning_v1",
                    row_count=2,
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

    run = ScoreRun(
        batch_id=batch.batch_id,
        metro_id="DFW",
        status=ScoreRunStatus.RUNNING,
    )
    session.add(run)
    session.flush()

    parcel_specs = [
        ("P-SURVIVE", Decimal("40"), "POINT (5 5)", None, None, None),
        ("P-BAND", Decimal("40"), "POINT (50 50)", None, None, None),
        ("P-SIZE", Decimal("5"), "POINT (5 5)", None, None, None),
        ("P-ZONING", Decimal("40"), "POINT (5 5)", "RES", "RESIDENTIAL", None),
        ("P-FLOOD", Decimal("40"), "POINT (5 5)", "LI", "INDUSTRIAL", "AE"),
    ]

    for parcel_id, acreage, rep_point_wkt, zoning_code, land_use_code, flood_zone in parcel_specs:
        session.add(
            RawParcel(
                parcel_id=parcel_id,
                county_fips="48085",
                metro_id="DFW",
                acreage=acreage,
                geometry_wkt="POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                lineage_key=f"parcel:{parcel_id}",
                is_active=True,
            )
        )
        session.add(
            ParcelRepPoint(
                parcel_id=parcel_id,
                rep_point_wkt=rep_point_wkt,
                geometry_method="representative_point",
            )
        )

        if zoning_code:
            session.add(
                RawZoning(
                    parcel_id=parcel_id,
                    county_fips="48085",
                    metro_id="DFW",
                    zoning_code=zoning_code,
                    land_use_code=land_use_code,
                    lineage_key=f"zoning:{parcel_id}",
                    is_active=True,
                )
            )

        if flood_zone:
            session.add(
                SourceEvidence(
                    source_id="FLOOD",
                    metro_id="DFW",
                    county_fips="48085",
                    parcel_id=parcel_id,
                    record_key=f"flood:{parcel_id}",
                    attribute_name="fema_zone",
                    attribute_value=flood_zone,
                    lineage_key=f"flood:{parcel_id}",
                    is_active=True,
                )
            )

    session.commit()
    return str(run.run_id)
