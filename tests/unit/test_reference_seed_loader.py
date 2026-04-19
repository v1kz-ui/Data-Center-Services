from pathlib import Path

from sqlalchemy import select

from app.db.models.catalogs import (
    BonusCatalog,
    FactorCatalog,
    ScoringProfile,
    ScoringProfileFactor,
    SourceCatalog,
    SourceInterface,
)
from app.db.models.territory import CountyCatalog, MetroCatalog
from app.services.reference_seeds import load_reference_seed_bundle


def test_reference_seed_loader_populates_controlled_bundle(db_session) -> None:
    result = load_reference_seed_bundle(db_session)
    db_session.commit()

    assert result.total_inserted == 49
    assert result.total_updated == 0

    assert len(db_session.scalars(select(MetroCatalog)).all()) == 4
    assert len(db_session.scalars(select(CountyCatalog)).all()) == 9
    assert len(db_session.scalars(select(SourceCatalog)).all()) == 5
    assert len(db_session.scalars(select(SourceInterface)).all()) == 5
    assert len(db_session.scalars(select(FactorCatalog)).all()) == 10
    assert len(db_session.scalars(select(BonusCatalog)).all()) == 5
    assert len(db_session.scalars(select(ScoringProfile)).all()) == 1
    assert len(db_session.scalars(select(ScoringProfileFactor)).all()) == 10


def test_reference_seed_loader_is_idempotent(db_session) -> None:
    seed_dir = Path("db/seeds")
    first_result = load_reference_seed_bundle(db_session, seed_dir=seed_dir)
    db_session.commit()

    second_result = load_reference_seed_bundle(db_session, seed_dir=seed_dir)
    db_session.commit()

    assert first_result.total_inserted == 49
    assert second_result.total_inserted == 0
    assert second_result.total_updated == 49
