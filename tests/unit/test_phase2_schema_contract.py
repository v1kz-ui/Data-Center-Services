from sqlalchemy import CheckConstraint, Index, UniqueConstraint

from app.db.base import Base


def test_phase2_unique_constraints_are_registered() -> None:
    parcel_evaluations = Base.metadata.tables["parcel_evaluations"]
    score_factor_input = Base.metadata.tables["score_factor_input"]
    score_bonus_detail = Base.metadata.tables["score_bonus_detail"]

    evaluation_constraints = {
        constraint.name
        for constraint in parcel_evaluations.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    factor_input_constraints = {
        constraint.name
        for constraint in score_factor_input.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    bonus_constraints = {
        constraint.name
        for constraint in score_bonus_detail.constraints
        if isinstance(constraint, UniqueConstraint)
    }

    assert "uq_parcel_evaluations_run_parcel" in evaluation_constraints
    assert "uq_score_factor_input_run_parcel_factor_input" in factor_input_constraints
    assert "uq_score_bonus_detail_run_parcel_bonus" in bonus_constraints


def test_phase2_indexes_and_checks_are_registered() -> None:
    raw_parcels = Base.metadata.tables["raw_parcels"]
    parcel_evaluations = Base.metadata.tables["parcel_evaluations"]
    score_run = Base.metadata.tables["score_run"]

    raw_parcel_indexes = {
        index.name
        for index in raw_parcels.indexes
        if isinstance(index, Index)
    }
    evaluation_checks = {
        constraint.name
        for constraint in parcel_evaluations.constraints
        if isinstance(constraint, CheckConstraint)
    }
    score_run_indexes = {
        index.name
        for index in score_run.indexes
        if isinstance(index, Index)
    }

    assert "ix_raw_parcels_county_fips_parcel_id" in raw_parcel_indexes
    assert "ix_raw_parcels_metro_id" in raw_parcel_indexes
    assert "ck_parcel_evaluations_confidence_required_when_scored" in evaluation_checks
    assert "ix_score_run_batch_metro_status" in score_run_indexes
