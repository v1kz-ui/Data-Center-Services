from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models.catalogs import SourceCatalog
from app.db.models.market import MarketListing
from app.services.live_candidate_scoring import (
    ScoredCandidate,
    build_ranked_live_candidate_records,
)
from evaluation.models import EvaluationPolicy
from evaluation.service import evaluate_run
from ingestion.service import ingest_evidence_records
from orchestrator.service import create_batch
from scoring.models import ScoringPolicy
from scoring.service import score_run

_LIVE_SCORING_SOURCE_ID = "LIVE_SCORE"
_LIVE_SCORING_CONNECTOR_KEY = "live_candidate_parcel_scoring"
_LIVE_SCORING_SOURCE_VERSION = "texas_live_v1"
_METRO_NAME_TO_ID = {
    "Dallas-Fort Worth": "DFW",
    "Houston": "HOU",
    "Austin": "AUS",
    "San Antonio": "SAT",
    "Rio Grande Valley": "MFE",
    "El Paso": "ELP",
    "Brazos Valley": "BRV",
}
_FACTOR_ATTRIBUTE_MAP = {
    "F01": ("power", 20.0, "measured"),
    "F02": ("fiber", 12.0, "measured"),
    "F03": ("hazard", 12.0, "proxy"),
    "F04": ("scale", 10.0, "measured"),
    "F05": ("land_use", 10.0, "heuristic"),
    "F06": ("water", 8.0, "measured"),
    "F07": ("environment", 8.0, "proxy"),
    "F08": ("talent", 8.0, "proxy"),
    "F09": ("logistics", 6.0, "measured"),
    "F10": ("market", 6.0, "measured"),
}
_BLOCKED_ZONING_CODES = ("R", "R1", "R-1", "R2", "R-2", "RE", "RES", "SF", "SFR", "MF", "MH")
_BLOCKED_LAND_USE_CODES = (
    "RES",
    "RESIDENTIAL",
    "SINGLE FAMILY",
    "MULTIFAMILY",
    "MOBILE HOME",
)


@dataclass(frozen=True, slots=True)
class LinkedParcelCandidate:
    parcel_id: str
    county_fips: str
    metro_id: str
    metro_name: str
    market_listing_id: str
    listing_source_id: str
    source_listing_key: str
    site_name: str
    source_url: str
    viability_score: int
    confidence_score: int
    city_distance_miles: float
    university_distance_miles: float
    factor_scores: dict[str, float]
    bonus_flags: dict[str, bool]


@dataclass(frozen=True, slots=True)
class EvidenceMaterializationReport:
    source_id: str
    requested_limit: int
    linked_listing_count: int
    unique_parcel_count: int
    evidence_record_count: int
    metro_counts: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class LiveParcelScoringReport:
    requested_limit: int
    profile_name: str
    linked_listing_count: int
    unique_parcel_count: int
    source_id: str
    batch_id: str
    metro_counts: dict[str, int]
    evidence_report: EvidenceMaterializationReport
    evaluation_summaries: list[dict[str, Any]] = field(default_factory=list)
    scoring_summaries: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["evidence_report"] = self.evidence_report.to_dict()
        return payload


def ensure_live_scoring_source(session: Session) -> SourceCatalog:
    source = session.get(SourceCatalog, _LIVE_SCORING_SOURCE_ID)
    if source is None:
        source = SourceCatalog(
            source_id=_LIVE_SCORING_SOURCE_ID,
            display_name="Linked Live Candidate Parcel Scoring",
            owner_name="Anti-Gravity Derived Scoring",
            refresh_cadence="daily",
            block_refresh=False,
            metro_coverage="TX",
            target_table_name="source_evidence",
            is_active=True,
        )
        session.add(source)
    else:
        source.display_name = "Linked Live Candidate Parcel Scoring"
        source.owner_name = "Anti-Gravity Derived Scoring"
        source.refresh_cadence = "daily"
        source.block_refresh = False
        source.metro_coverage = "TX"
        source.target_table_name = "source_evidence"
        source.is_active = True
    session.flush()
    return source


def build_linked_parcel_candidates(
    session: Session,
    *,
    limit: int = 1000,
) -> list[LinkedParcelCandidate]:
    ranked_candidates = build_ranked_live_candidate_records(
        session,
        limit=limit,
        per_metro_cap=None,
        major_metro_minimums=None,
        metro_caps=None,
    )
    if not ranked_candidates:
        return []

    listing_ids = [
        UUID(candidate.opportunity["market_listing_id"])
        for candidate in ranked_candidates
    ]
    listings = session.scalars(
        select(MarketListing).where(MarketListing.market_listing_id.in_(listing_ids))
    ).all()
    listing_by_id = {str(listing.market_listing_id): listing for listing in listings}

    best_by_parcel: dict[str, LinkedParcelCandidate] = {}
    for candidate in ranked_candidates:
        listing = listing_by_id.get(candidate.opportunity["market_listing_id"])
        if listing is None or listing.parcel_id is None or listing.county_fips is None:
            continue

        metro_name = candidate.opportunity["metro"]
        metro_id = _METRO_NAME_TO_ID.get(metro_name)
        if metro_id is None:
            continue

        linked_candidate = LinkedParcelCandidate(
            parcel_id=listing.parcel_id,
            county_fips=listing.county_fips,
            metro_id=metro_id,
            metro_name=metro_name,
            market_listing_id=str(listing.market_listing_id),
            listing_source_id=listing.listing_source_id,
            source_listing_key=listing.source_listing_key,
            site_name=listing.listing_title,
            source_url=listing.source_url,
            viability_score=candidate.viability_score,
            confidence_score=candidate.confidence_score,
            city_distance_miles=float(candidate.city_distance or 0.0),
            university_distance_miles=float(candidate.university_distance or 0.0),
            factor_scores=dict(candidate.factor_scores),
            bonus_flags=_bonus_flags(candidate),
        )
        existing = best_by_parcel.get(linked_candidate.parcel_id)
        if existing is None or _candidate_priority(linked_candidate) > _candidate_priority(
            existing
        ):
            best_by_parcel[linked_candidate.parcel_id] = linked_candidate

    return sorted(
        best_by_parcel.values(),
        key=lambda item: (
            -item.viability_score,
            -item.confidence_score,
            item.metro_name,
            item.site_name,
        ),
    )


def materialize_live_candidate_evidence(
    session: Session,
    *,
    limit: int = 1000,
    source_id: str = _LIVE_SCORING_SOURCE_ID,
) -> tuple[list[LinkedParcelCandidate], EvidenceMaterializationReport]:
    ensure_live_scoring_source(session)
    linked_candidates = build_linked_parcel_candidates(session, limit=limit)
    metro_counts: dict[str, int] = {}
    evidence_record_count = 0

    for metro_id, metro_candidates in _group_candidates_by_metro(linked_candidates).items():
        metro_counts[metro_id] = len(metro_candidates)
        records: list[dict[str, str]] = []
        for candidate in metro_candidates:
            records.extend(_build_evidence_records(candidate))
        if not records:
            continue
        report = ingest_evidence_records(
            session,
            source_id=source_id,
            metro_id=metro_id,
            source_version=_LIVE_SCORING_SOURCE_VERSION,
            records=records,
            loaded_at=datetime.now(UTC),
            connector_key=_LIVE_SCORING_CONNECTOR_KEY,
            replace_existing_scope="source_metro",
        )
        evidence_record_count += report.accepted_count

    linked_listing_count = len(linked_candidates)
    unique_parcel_count = len({candidate.parcel_id for candidate in linked_candidates})
    report = EvidenceMaterializationReport(
        source_id=source_id,
        requested_limit=limit,
        linked_listing_count=linked_listing_count,
        unique_parcel_count=unique_parcel_count,
        evidence_record_count=evidence_record_count,
        metro_counts=metro_counts,
    )
    return linked_candidates, report


def run_live_candidate_parcel_scoring(
    session: Session,
    *,
    limit: int = 1000,
    profile_name: str = _LIVE_SCORING_SOURCE_VERSION,
    minimum_acreage: Decimal = Decimal("1.0"),
) -> LiveParcelScoringReport:
    linked_candidates, evidence_report = materialize_live_candidate_evidence(session, limit=limit)
    linked_listing_count = len(linked_candidates)
    if not linked_candidates:
        raise ValueError("No parcel-linked live candidates are available to score.")

    candidates_by_metro = _group_candidates_by_metro(linked_candidates)
    batch = create_batch(session, list(candidates_by_metro))
    run_lookup = {run.metro_id: run.run_id for run in batch.runs}
    evaluation_summaries: list[dict[str, Any]] = []
    scoring_summaries: list[dict[str, Any]] = []

    for metro_id, metro_candidates in candidates_by_metro.items():
        run_id = run_lookup[metro_id]
        parcel_ids = tuple(candidate.parcel_id for candidate in metro_candidates)
        evaluation_summary = evaluate_run(
            session,
            run_id,
            EvaluationPolicy(
                rule_version=profile_name,
                minimum_acreage=minimum_acreage,
                blocked_zoning_codes=_BLOCKED_ZONING_CODES,
                blocked_land_use_codes=_BLOCKED_LAND_USE_CODES,
                parcel_ids=parcel_ids,
                skip_freshness_gate=True,
            ),
        )
        scoring_summary = score_run(
            session,
            run_id,
            ScoringPolicy(
                profile_name=profile_name,
                skip_freshness_gate=True,
            ),
        )
        evaluation_summaries.append(asdict(evaluation_summary))
        scoring_summaries.append(asdict(scoring_summary))

    metro_counts = {
        metro_id: len(metro_candidates)
        for metro_id, metro_candidates in sorted(candidates_by_metro.items())
    }
    return LiveParcelScoringReport(
        requested_limit=limit,
        profile_name=profile_name,
        linked_listing_count=linked_listing_count,
        unique_parcel_count=len({candidate.parcel_id for candidate in linked_candidates}),
        source_id=_LIVE_SCORING_SOURCE_ID,
        batch_id=batch.batch_id,
        metro_counts=metro_counts,
        evidence_report=evidence_report,
        evaluation_summaries=evaluation_summaries,
        scoring_summaries=scoring_summaries,
    )


def _group_candidates_by_metro(
    candidates: list[LinkedParcelCandidate],
) -> dict[str, list[LinkedParcelCandidate]]:
    grouped: dict[str, list[LinkedParcelCandidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.metro_id, []).append(candidate)
    return grouped


def _candidate_priority(candidate: LinkedParcelCandidate) -> tuple[int, int]:
    return (candidate.viability_score, candidate.confidence_score)


def _bonus_flags(candidate: ScoredCandidate) -> dict[str, bool]:
    listing = candidate.candidate
    acreage = listing.acreage if listing is not None and listing.acreage is not None else 0.0
    building_sqft = (
        listing.building_sqft
        if listing is not None and listing.building_sqft is not None
        else 0.0
    )
    market_normalized = _normalized_factor_score(candidate.factor_scores, "F10")
    return {
        "B01": True,
        "B02": 1.0 <= acreage <= 3.0 or 15_000 <= building_sqft <= 75_000,
        "B03": (candidate.substation_distance or 999.0) <= 10.0
        and (candidate.peering_distance or 999.0) <= 20.0,
        "B04": min(
            candidate.city_distance or 999.0,
            candidate.university_distance or 999.0,
        )
        <= 10.0,
        "B05": market_normalized >= 0.70,
    }


def _build_evidence_records(candidate: LinkedParcelCandidate) -> list[dict[str, str]]:
    record_key = f"parcel-score:{candidate.parcel_id}"
    lineage_prefix = f"{record_key}:{candidate.market_listing_id}"
    records: list[dict[str, str]] = []

    for factor_id, (_factor_key, _, quality) in _FACTOR_ATTRIBUTE_MAP.items():
        records.append(
            {
                "record_key": record_key,
                "attribute_name": f"{factor_id.lower()}_{quality}",
                "attribute_value": _format_decimal(
                    _normalized_factor_score(candidate.factor_scores, factor_id)
                ),
                "lineage_key": f"{lineage_prefix}:{factor_id.lower()}_{quality}",
                "county_fips": candidate.county_fips,
                "parcel_id": candidate.parcel_id,
            }
        )

    for bonus_id, applied in sorted(candidate.bonus_flags.items()):
        records.append(
            {
                "record_key": record_key,
                "attribute_name": f"{bonus_id.lower()}_measured",
                "attribute_value": "true" if applied else "false",
                "lineage_key": f"{lineage_prefix}:{bonus_id.lower()}_measured",
                "county_fips": candidate.county_fips,
                "parcel_id": candidate.parcel_id,
            }
        )

    audit_attributes = {
        "market_listing_id": candidate.market_listing_id,
        "listing_source_id": candidate.listing_source_id,
        "source_listing_key": candidate.source_listing_key,
        "site_name": candidate.site_name,
        "source_url": candidate.source_url,
        "derived_viability_score": str(candidate.viability_score),
        "derived_confidence_score": str(candidate.confidence_score),
        "distance_to_city_miles": _format_decimal(candidate.city_distance_miles),
        "distance_to_university_miles": _format_decimal(candidate.university_distance_miles),
    }
    for attribute_name, attribute_value in audit_attributes.items():
        records.append(
            {
                "record_key": record_key,
                "attribute_name": attribute_name,
                "attribute_value": attribute_value,
                "lineage_key": f"{lineage_prefix}:{attribute_name}",
                "county_fips": candidate.county_fips,
                "parcel_id": candidate.parcel_id,
            }
        )
    return records


def _normalized_factor_score(factor_scores: dict[str, float], factor_id: str) -> Decimal:
    factor_key, max_points, _ = _FACTOR_ATTRIBUTE_MAP[factor_id]
    raw_score = factor_scores.get(factor_key, 0.0)
    if max_points <= 0:
        return Decimal("0")
    normalized = max(0.0, min(raw_score / max_points, 1.0))
    return Decimal(str(normalized)).quantize(Decimal("0.0001"))


def _format_decimal(value: float | Decimal) -> str:
    if isinstance(value, Decimal):
        decimal_value = value
    else:
        decimal_value = Decimal(str(value))
    return format(decimal_value.quantize(Decimal("0.0001")), "f")
