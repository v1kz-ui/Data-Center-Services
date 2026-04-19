from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy.orm import Session

from scoring.models import ScoringPolicy
from scoring.service import get_parcel_scoring_detail, score_run
from tests.unit.test_scoring_service import _seed_scoring_context


def test_scoring_outputs_match_oracle_fixture(db_session: Session) -> None:
    run_id = _seed_scoring_context(db_session)
    summary = score_run(db_session, run_id, ScoringPolicy())
    detail = get_parcel_scoring_detail(db_session, run_id, "P-SCORE-A")

    oracle_path = Path("tests/fixtures/phase5_scoring_oracle.json")
    oracle = json.loads(oracle_path.read_text(encoding="utf-8"))

    assert _serialize_summary(summary) == oracle["summary"]
    assert _serialize_detail(detail) == oracle["parcel_detail"]


def _serialize_summary(summary) -> dict[str, object]:
    return {
        "profile_name": summary.profile_name,
        "run_status": summary.run_status,
        "scored_count": summary.scored_count,
        "pending_scoring_count": summary.pending_scoring_count,
        "factor_detail_count": summary.factor_detail_count,
        "bonus_detail_count": summary.bonus_detail_count,
        "provenance_count": summary.provenance_count,
        "average_viability_score": str(summary.average_viability_score),
        "average_confidence_score": str(summary.average_confidence_score),
        "status_counts": [
            {"status": item.status, "count": item.count} for item in summary.status_counts
        ],
    }


def _serialize_detail(detail) -> dict[str, object]:
    return {
        "parcel_id": detail.parcel_id,
        "status": detail.status,
        "viability_score": str(detail.viability_score),
        "confidence_score": str(detail.confidence_score),
        "evidence_quality_counts": detail.evidence_quality_counts,
        "factor_details": [
            {
                "factor_id": factor.factor_id,
                "points_awarded": str(factor.points_awarded),
                "input_names": [item.input_name for item in factor.inputs],
            }
            for factor in detail.factor_details
        ],
        "bonus_details": [
            {
                "bonus_id": bonus.bonus_id,
                "applied": bonus.applied,
                "points_awarded": str(bonus.points_awarded),
            }
            for bonus in detail.bonus_details
        ],
    }
