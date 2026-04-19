from sqlalchemy import Index, UniqueConstraint

from app.db.base import Base


def test_phase3_unique_constraints_are_registered() -> None:
    raw_zoning = Base.metadata.tables["raw_zoning"]
    source_evidence = Base.metadata.tables["source_evidence"]

    zoning_constraints = {
        constraint.name
        for constraint in raw_zoning.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    evidence_constraints = {
        constraint.name
        for constraint in source_evidence.constraints
        if isinstance(constraint, UniqueConstraint)
    }

    assert "uq_raw_zoning_parcel_snapshot" in zoning_constraints
    assert "uq_source_evidence_snapshot_record_attribute" in evidence_constraints


def test_phase3_indexes_are_registered() -> None:
    source_record_rejection = Base.metadata.tables["source_record_rejection"]
    raw_zoning = Base.metadata.tables["raw_zoning"]
    source_evidence = Base.metadata.tables["source_evidence"]

    rejection_indexes = {
        index.name
        for index in source_record_rejection.indexes
        if isinstance(index, Index)
    }
    zoning_indexes = {
        index.name
        for index in raw_zoning.indexes
        if isinstance(index, Index)
    }
    evidence_indexes = {
        index.name
        for index in source_evidence.indexes
        if isinstance(index, Index)
    }

    assert "ix_source_record_rejection_snapshot_row" in rejection_indexes
    assert "ix_raw_zoning_metro_parcel" in zoning_indexes
    assert "ix_source_evidence_source_metro" in evidence_indexes
    assert "ix_source_evidence_parcel_id" in evidence_indexes
