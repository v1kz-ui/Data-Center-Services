from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal


@dataclass(slots=True)
class EvidenceQualityWeights:
    measured: Decimal = Decimal("1.00")
    manual: Decimal = Decimal("0.90")
    proxy: Decimal = Decimal("0.60")
    heuristic: Decimal = Decimal("0.30")
    missing: Decimal = Decimal("0.00")


@dataclass(slots=True)
class ScoringPolicy:
    profile_name: str | None = None
    restart_failed_run: bool = True
    allow_completed_run_rerun: bool = True
    evidence_quality_weights: EvidenceQualityWeights = field(
        default_factory=EvidenceQualityWeights
    )


@dataclass(slots=True)
class ScoringStatusCount:
    status: str
    count: int


@dataclass(slots=True)
class ProvenanceInputDetail:
    input_name: str
    input_value: str
    evidence_quality: str


@dataclass(slots=True)
class FactorDetailBreakdown:
    factor_id: str
    points_awarded: Decimal
    rationale: str | None
    inputs: list[ProvenanceInputDetail] = field(default_factory=list)


@dataclass(slots=True)
class BonusDetailBreakdown:
    bonus_id: str
    applied: bool
    points_awarded: Decimal
    rationale: str | None


@dataclass(slots=True)
class ParcelScoringDetail:
    run_id: str
    batch_id: str
    metro_id: str
    profile_name: str | None
    parcel_id: str
    status: str
    status_reason: str | None
    viability_score: Decimal | None
    confidence_score: Decimal | None
    factor_details: list[FactorDetailBreakdown] = field(default_factory=list)
    bonus_details: list[BonusDetailBreakdown] = field(default_factory=list)
    evidence_quality_counts: dict[str, int] = field(default_factory=dict)


@dataclass(slots=True)
class ScoringSummary:
    run_id: str
    batch_id: str
    metro_id: str
    profile_name: str | None
    run_status: str
    scored_count: int
    pending_scoring_count: int
    factor_detail_count: int
    bonus_detail_count: int
    provenance_count: int
    average_viability_score: Decimal | None
    average_confidence_score: Decimal | None
    status_counts: list[ScoringStatusCount] = field(default_factory=list)
    failure_reason: str | None = None
