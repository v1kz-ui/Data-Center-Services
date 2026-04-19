from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models.catalogs import (
    BonusCatalog,
    FactorCatalog,
    ScoringProfile,
    ScoringProfileFactor,
    SourceCatalog,
    SourceInterface,
)
from app.db.models.enums import ScoringProfileStatus
from app.db.models.territory import CountyCatalog, MetroCatalog

REPO_ROOT = Path(__file__).resolve().parents[5]
DEFAULT_REFERENCE_SEED_DIR = REPO_ROOT / "db" / "seeds"


@dataclass(slots=True)
class SeedEntityResult:
    entity: str
    inserted: int = 0
    updated: int = 0


@dataclass(slots=True)
class ReferenceSeedLoadResult:
    seed_dir: str
    entities: list[SeedEntityResult]

    @property
    def total_inserted(self) -> int:
        return sum(entity.inserted for entity in self.entities)

    @property
    def total_updated(self) -> int:
        return sum(entity.updated for entity in self.entities)

    def to_dict(self) -> dict[str, object]:
        return {
            "seed_dir": self.seed_dir,
            "total_inserted": self.total_inserted,
            "total_updated": self.total_updated,
            "entities": [asdict(entity) for entity in self.entities],
        }


def load_reference_seed_bundle(
    session: Session,
    *,
    seed_dir: Path | str | None = None,
) -> ReferenceSeedLoadResult:
    resolved_seed_dir = Path(seed_dir or DEFAULT_REFERENCE_SEED_DIR)
    entity_results = [
        _load_metro_catalog(session, resolved_seed_dir / "metro_catalog.csv"),
        _load_county_catalog(session, resolved_seed_dir / "county_catalog.csv"),
        _load_source_catalog(session, resolved_seed_dir / "source_catalog.csv"),
        _load_source_interface(session, resolved_seed_dir / "source_interface.csv"),
        _load_factor_catalog(session, resolved_seed_dir / "factor_catalog.csv"),
        _load_bonus_catalog(session, resolved_seed_dir / "bonus_catalog.csv"),
    ]
    entity_results.append(_load_scoring_profile(session, resolved_seed_dir / "scoring_profile.csv"))
    session.flush()
    entity_results.append(
        _load_scoring_profile_factor(session, resolved_seed_dir / "scoring_profile_factor.csv")
    )
    session.flush()
    return ReferenceSeedLoadResult(
        seed_dir=str(resolved_seed_dir),
        entities=entity_results,
    )


def _load_metro_catalog(session: Session, path: Path) -> SeedEntityResult:
    result = SeedEntityResult(entity="metro_catalog")
    for row in _read_csv_rows(path):
        metro = session.get(MetroCatalog, row["metro_id"])
        is_new = metro is None
        if metro is None:
            metro = MetroCatalog(metro_id=row["metro_id"])
            session.add(metro)
        metro.display_name = row["display_name"]
        metro.state_code = row["state_code"]
        metro.is_active = _parse_bool(row["is_active"])
        _track_change(result, is_new)
    return result


def _load_county_catalog(session: Session, path: Path) -> SeedEntityResult:
    result = SeedEntityResult(entity="county_catalog")
    for row in _read_csv_rows(path):
        county = session.get(CountyCatalog, row["county_fips"])
        is_new = county is None
        if county is None:
            county = CountyCatalog(county_fips=row["county_fips"])
            session.add(county)
        county.metro_id = row["metro_id"]
        county.display_name = row["display_name"]
        county.state_code = row["state_code"]
        county.is_active = _parse_bool(row["is_active"])
        _track_change(result, is_new)
    return result


def _load_source_catalog(session: Session, path: Path) -> SeedEntityResult:
    result = SeedEntityResult(entity="source_catalog")
    for row in _read_csv_rows(path):
        source = session.get(SourceCatalog, row["source_id"])
        is_new = source is None
        if source is None:
            source = SourceCatalog(source_id=row["source_id"])
            session.add(source)
        source.display_name = row["display_name"]
        source.owner_name = row["owner_name"]
        source.refresh_cadence = row["refresh_cadence"]
        source.block_refresh = _parse_bool(row["block_refresh"])
        source.metro_coverage = row["metro_coverage"] or None
        source.target_table_name = row["target_table_name"]
        source.is_active = _parse_bool(row["is_active"])
        _track_change(result, is_new)
    return result


def _load_source_interface(session: Session, path: Path) -> SeedEntityResult:
    result = SeedEntityResult(entity="source_interface")
    for row in _read_csv_rows(path):
        interface = session.scalar(
            select(SourceInterface).where(
                SourceInterface.source_id == row["source_id"],
                SourceInterface.interface_name == row["interface_name"],
            )
        )
        is_new = interface is None
        if interface is None:
            interface = SourceInterface(
                source_id=row["source_id"],
                interface_name=row["interface_name"],
            )
            session.add(interface)
        interface.schema_version = row["schema_version"]
        interface.load_mode = row["load_mode"]
        interface.validation_notes = row["validation_notes"] or None
        _track_change(result, is_new)
    return result


def _load_factor_catalog(session: Session, path: Path) -> SeedEntityResult:
    result = SeedEntityResult(entity="factor_catalog")
    for row in _read_csv_rows(path):
        factor = session.get(FactorCatalog, row["factor_id"])
        is_new = factor is None
        if factor is None:
            factor = FactorCatalog(factor_id=row["factor_id"])
            session.add(factor)
        factor.display_name = row["display_name"]
        factor.description = row["description"]
        factor.ordinal = int(row["ordinal"])
        factor.is_active = _parse_bool(row["is_active"])
        _track_change(result, is_new)
    return result


def _load_bonus_catalog(session: Session, path: Path) -> SeedEntityResult:
    result = SeedEntityResult(entity="bonus_catalog")
    for row in _read_csv_rows(path):
        bonus = session.get(BonusCatalog, row["bonus_id"])
        is_new = bonus is None
        if bonus is None:
            bonus = BonusCatalog(bonus_id=row["bonus_id"])
            session.add(bonus)
        bonus.display_name = row["display_name"]
        bonus.description = row["description"]
        bonus.max_points = int(row["max_points"])
        bonus.is_active = _parse_bool(row["is_active"])
        _track_change(result, is_new)
    return result


def _load_scoring_profile(session: Session, path: Path) -> SeedEntityResult:
    result = SeedEntityResult(entity="scoring_profile")
    for row in _read_csv_rows(path):
        profile = session.scalar(
            select(ScoringProfile).where(ScoringProfile.profile_name == row["profile_name"])
        )
        is_new = profile is None
        if profile is None:
            profile = ScoringProfile(profile_name=row["profile_name"])
            session.add(profile)
        profile.version_label = row["version_label"]
        profile.status = ScoringProfileStatus(row["status"])
        profile.effective_from = _parse_datetime(row["effective_from"])
        profile.effective_to = _parse_datetime(row["effective_to"])
        _track_change(result, is_new)
    return result


def _load_scoring_profile_factor(session: Session, path: Path) -> SeedEntityResult:
    result = SeedEntityResult(entity="scoring_profile_factor")
    for row in _read_csv_rows(path):
        profile = session.scalar(
            select(ScoringProfile).where(ScoringProfile.profile_name == row["profile_name"])
        )
        if profile is None:
            msg = f"Scoring profile `{row['profile_name']}` must exist before factor seeding."
            raise ValueError(msg)

        profile_factor = session.scalar(
            select(ScoringProfileFactor).where(
                ScoringProfileFactor.profile_id == profile.profile_id,
                ScoringProfileFactor.factor_id == row["factor_id"],
            )
        )
        is_new = profile_factor is None
        if profile_factor is None:
            profile_factor = ScoringProfileFactor(
                profile_id=profile.profile_id,
                factor_id=row["factor_id"],
            )
            session.add(profile_factor)
        profile_factor.max_points = int(row["max_points"])
        profile_factor.ordinal = int(row["ordinal"])
        _track_change(result, is_new)
    return result


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as csv_file:
        return list(csv.DictReader(csv_file))


def _parse_bool(raw_value: str) -> bool:
    return raw_value.strip().lower() in {"1", "true", "yes", "y"}


def _parse_datetime(raw_value: str) -> datetime | None:
    cleaned = raw_value.strip()
    if not cleaned:
        return None
    return datetime.fromisoformat(cleaned.replace("Z", "+00:00"))


def _track_change(result: SeedEntityResult, is_new: bool) -> None:
    if is_new:
        result.inserted += 1
    else:
        result.updated += 1
