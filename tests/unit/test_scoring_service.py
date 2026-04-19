from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from sqlalchemy.orm import Session

from app.db.models.batching import ScoreBatch, ScoreRun
from app.db.models.catalogs import (
    BonusCatalog,
    FactorCatalog,
    ScoringProfile,
    ScoringProfileFactor,
    SourceCatalog,
)
from app.db.models.enums import (
    ParcelEvaluationStatus,
    ScoreBatchStatus,
    ScoreRunStatus,
    ScoringProfileStatus,
    SourceSnapshotStatus,
)
from app.db.models.evaluation import ParcelEvaluation
from app.db.models.ingestion import SourceSnapshot
from app.db.models.scoring import ScoreBonusDetail, ScoreFactorDetail, ScoreFactorInput
from app.db.models.source_data import SourceEvidence
from app.db.models.territory import CountyCatalog, MetroCatalog, RawParcel
from scoring.models import ScoringPolicy
from scoring.service import (
    ScoringProfileValidationError,
    get_parcel_scoring_detail,
    score_run,
)


def test_score_run_writes_factor_bonus_and_confidence_outputs(db_session: Session) -> None:
    run_id = _seed_scoring_context(db_session)

    summary = score_run(db_session, run_id, ScoringPolicy())
    parcel_a = db_session.get(
        ParcelEvaluation,
        UUID(_evaluation_id_for(db_session, run_id, "P-SCORE-A")),
    )
    parcel_b = db_session.get(
        ParcelEvaluation,
        UUID(_evaluation_id_for(db_session, run_id, "P-SCORE-B")),
    )
    factor_rows = db_session.query(ScoreFactorDetail).all()
    bonus_rows = db_session.query(ScoreBonusDetail).all()
    provenance_rows = db_session.query(ScoreFactorInput).all()
    f01_detail = (
        db_session.query(ScoreFactorDetail)
        .filter_by(parcel_id="P-SCORE-A", factor_id="F01")
        .one()
    )

    assert summary.run_status == "completed"
    assert summary.profile_name == "default_v1"
    assert summary.scored_count == 2
    assert summary.pending_scoring_count == 0
    assert summary.factor_detail_count == 20
    assert summary.bonus_detail_count == 10
    assert summary.provenance_count == 22
    assert parcel_a is not None
    assert parcel_b is not None
    assert parcel_a.status is ParcelEvaluationStatus.SCORED
    assert parcel_a.viability_score == Decimal("60.00")
    assert parcel_a.confidence_score == Decimal("100.00")
    assert parcel_b.viability_score == Decimal("58.00")
    assert parcel_b.confidence_score == Decimal("63.00")
    assert len(factor_rows) == 20
    assert len(bonus_rows) == 10
    assert len(provenance_rows) == 22
    assert f01_detail.points_awarded == Decimal("1.00")
    assert "f01_measured" in f01_detail.rationale


def test_score_run_is_idempotent_for_completed_run_rerun(db_session: Session) -> None:
    run_id = _seed_scoring_context(db_session)
    policy = ScoringPolicy()

    score_run(db_session, run_id, policy)
    summary = score_run(db_session, run_id, policy)

    assert summary.run_status == "completed"
    assert db_session.query(ScoreFactorDetail).count() == 20
    assert db_session.query(ScoreBonusDetail).count() == 10
    assert db_session.query(ScoreFactorInput).count() == 22


def test_get_parcel_scoring_detail_returns_factor_bonus_and_provenance(db_session: Session) -> None:
    run_id = _seed_scoring_context(db_session)
    score_run(db_session, run_id, ScoringPolicy())

    detail = get_parcel_scoring_detail(db_session, run_id, "P-SCORE-A")

    assert detail.status == "scored"
    assert detail.viability_score == Decimal("60.00")
    assert detail.confidence_score == Decimal("100.00")
    assert len(detail.factor_details) == 10
    assert len(detail.bonus_details) == 5
    assert detail.evidence_quality_counts == {"measured": 10, "proxy": 1}
    assert detail.factor_details[0].factor_id == "F01"
    assert len(detail.factor_details[0].inputs) == 2
    assert detail.bonus_details[0].bonus_id == "B01"
    assert detail.bonus_details[0].applied is True


def test_score_run_reconciles_parent_batch_to_completed(db_session: Session) -> None:
    run_id = _seed_scoring_context(db_session)

    summary = score_run(db_session, run_id, ScoringPolicy())
    batch = db_session.get(ScoreBatch, UUID(summary.batch_id))

    assert batch is not None
    assert batch.status is ScoreBatchStatus.COMPLETED
    assert batch.completed_metros == 1


def test_score_run_rejects_invalid_profile_budget(db_session: Session) -> None:
    run_id = _seed_scoring_context(db_session, invalid_budget=True)

    with pytest.raises(ScoringProfileValidationError):
        score_run(db_session, run_id, ScoringPolicy())


def _evaluation_id_for(session: Session, run_id: str, parcel_id: str) -> str:
    evaluation = (
        session.query(ParcelEvaluation)
        .filter_by(run_id=UUID(run_id), parcel_id=parcel_id)
        .one()
    )
    return str(evaluation.evaluation_id)


def _seed_scoring_context(
    session: Session,
    *,
    invalid_budget: bool = False,
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
                source_id="SCORING",
                display_name="Scoring Evidence Feed",
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
                source_id="SCORING",
                metro_id="DFW",
                snapshot_ts=now - timedelta(hours=1),
                source_version="scoring_v1",
                row_count=25,
                checksum="scoring-checksum",
                status=SourceSnapshotStatus.SUCCESS,
            ),
        ]
    )
    session.flush()

    session.add_all(
        [
            FactorCatalog(
                factor_id=f"F{index:02d}",
                display_name=f"Factor {index:02d}",
                description=f"Seed factor {index:02d}",
                ordinal=index,
                is_active=True,
            )
            for index in range(1, 11)
        ]
    )
    session.add_all(
        [
            BonusCatalog(
                bonus_id=f"B{index:02d}",
                display_name=f"Bonus {index:02d}",
                description=f"Seed bonus {index:02d}",
                max_points=5,
                is_active=True,
            )
            for index in range(1, 6)
        ]
    )
    session.flush()

    profile = ScoringProfile(
        profile_name="default_v1",
        version_label="v1",
        status=ScoringProfileStatus.ACTIVE,
        effective_from=now - timedelta(days=1),
        effective_to=None,
    )
    session.add(profile)
    session.flush()
    for index in range(1, 11):
        max_points = 9 if invalid_budget else 10
        session.add(
            ScoringProfileFactor(
                profile_id=profile.profile_id,
                factor_id=f"F{index:02d}",
                max_points=max_points,
                ordinal=index,
            )
        )

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

    session.add_all(
        [
            RawParcel(
                parcel_id="P-SCORE-A",
                county_fips="48085",
                metro_id="DFW",
                acreage=25,
                geometry_wkt="POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                lineage_key="parcel:P-SCORE-A",
                is_active=True,
            ),
            RawParcel(
                parcel_id="P-SCORE-B",
                county_fips="48085",
                metro_id="DFW",
                acreage=30,
                geometry_wkt="POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                lineage_key="parcel:P-SCORE-B",
                is_active=True,
            ),
            RawParcel(
                parcel_id="P-EXCLUDED",
                county_fips="48085",
                metro_id="DFW",
                acreage=20,
                geometry_wkt="POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                lineage_key="parcel:P-EXCLUDED",
                is_active=True,
            ),
        ]
    )
    session.flush()

    session.add_all(
        [
            ParcelEvaluation(
                run_id=run.run_id,
                parcel_id="P-SCORE-A",
                status=ParcelEvaluationStatus.PENDING_SCORING,
                status_reason="Ready for scoring.",
            ),
            ParcelEvaluation(
                run_id=run.run_id,
                parcel_id="P-SCORE-B",
                status=ParcelEvaluationStatus.PENDING_SCORING,
                status_reason="Ready for scoring.",
            ),
            ParcelEvaluation(
                run_id=run.run_id,
                parcel_id="P-EXCLUDED",
                status=ParcelEvaluationStatus.EXCLUDED,
                status_reason="Excluded upstream.",
            ),
        ]
    )
    session.flush()

    for index in range(1, 11):
        factor_id = f"f{index:02d}"
        session.add(
            SourceEvidence(
                source_id="SCORING",
                metro_id="DFW",
                county_fips="48085",
                parcel_id="P-SCORE-A",
                record_key=f"a:{factor_id}",
                attribute_name=f"{factor_id}_measured",
                attribute_value=str(Decimal(index) / Decimal("10")),
                lineage_key=f"a:{factor_id}",
                is_active=True,
            )
        )
        session.add(
            SourceEvidence(
                source_id="SCORING",
                metro_id="DFW",
                county_fips="48085",
                parcel_id="P-SCORE-B",
                record_key=f"b:{factor_id}:proxy",
                attribute_name=f"{factor_id}_proxy",
                attribute_value="0.5",
                lineage_key=f"b:{factor_id}:proxy",
                is_active=True,
            )
        )
    session.add(
        SourceEvidence(
            source_id="SCORING",
            metro_id="DFW",
            county_fips="48085",
            parcel_id="P-SCORE-A",
            record_key="a:f01:proxy",
            attribute_name="f01_proxy",
            attribute_value="0.01",
            lineage_key="a:f01:proxy",
            is_active=True,
        )
    )
    session.add(
        SourceEvidence(
            source_id="SCORING",
            metro_id="DFW",
            county_fips="48085",
            parcel_id="P-SCORE-B",
            record_key="b:f02:manual",
            attribute_name="f02_manual",
            attribute_value="0.8",
            lineage_key="b:f02:manual",
            is_active=True,
        )
    )
    session.add_all(
        [
            SourceEvidence(
                source_id="SCORING",
                metro_id="DFW",
                county_fips="48085",
                parcel_id="P-SCORE-A",
                record_key="a:b01",
                attribute_name="b01_measured",
                attribute_value="true",
                lineage_key="a:b01",
                is_active=True,
            ),
            SourceEvidence(
                source_id="SCORING",
                metro_id="DFW",
                county_fips="48085",
                parcel_id="P-SCORE-B",
                record_key="b:b02",
                attribute_name="b02_proxy",
                attribute_value="true",
                lineage_key="b:b02",
                is_active=True,
            ),
        ]
    )

    session.commit()
    return str(run.run_id)
