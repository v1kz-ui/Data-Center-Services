import csv
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


def _seed_row_counts(seed_dir: Path) -> dict[str, int]:
    def _count(filename: str) -> int:
        with (seed_dir / filename).open(encoding="utf-8", newline="") as csv_file:
            return sum(1 for _ in csv.DictReader(csv_file))

    return {
        "metro_catalog": _count("metro_catalog.csv"),
        "county_catalog": _count("county_catalog.csv"),
        "source_catalog": _count("source_catalog.csv"),
        "source_interface": _count("source_interface.csv"),
        "factor_catalog": _count("factor_catalog.csv"),
        "bonus_catalog": _count("bonus_catalog.csv"),
        "scoring_profile": _count("scoring_profile.csv"),
        "scoring_profile_factor": _count("scoring_profile_factor.csv"),
    }


def test_reference_seed_loader_populates_controlled_bundle(db_session) -> None:
    expected_counts = _seed_row_counts(Path("db/seeds"))
    result = load_reference_seed_bundle(db_session)
    db_session.commit()

    assert result.total_inserted == sum(expected_counts.values())
    assert result.total_updated == 0

    assert len(db_session.scalars(select(MetroCatalog)).all()) == expected_counts["metro_catalog"]
    assert len(db_session.scalars(select(CountyCatalog)).all()) == expected_counts["county_catalog"]
    assert len(db_session.scalars(select(SourceCatalog)).all()) == expected_counts["source_catalog"]
    assert len(db_session.scalars(select(SourceInterface)).all()) == expected_counts["source_interface"]
    assert len(db_session.scalars(select(FactorCatalog)).all()) == expected_counts["factor_catalog"]
    assert len(db_session.scalars(select(BonusCatalog)).all()) == expected_counts["bonus_catalog"]
    assert len(db_session.scalars(select(ScoringProfile)).all()) == expected_counts["scoring_profile"]
    assert len(db_session.scalars(select(ScoringProfileFactor)).all()) == expected_counts["scoring_profile_factor"]


def test_reference_seed_loader_is_idempotent(db_session) -> None:
    seed_dir = Path("db/seeds")
    expected_counts = _seed_row_counts(seed_dir)
    first_result = load_reference_seed_bundle(db_session, seed_dir=seed_dir)
    db_session.commit()

    second_result = load_reference_seed_bundle(db_session, seed_dir=seed_dir)
    db_session.commit()

    assert first_result.total_inserted == sum(expected_counts.values())
    assert second_result.total_inserted == 0
    assert second_result.total_updated == sum(expected_counts.values())
