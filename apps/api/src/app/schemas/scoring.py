from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class EvidenceQualityWeightsRequest(BaseModel):
    measured: Decimal = Field(default=Decimal("1.00"), ge=0, le=1)
    manual: Decimal = Field(default=Decimal("0.90"), ge=0, le=1)
    proxy: Decimal = Field(default=Decimal("0.60"), ge=0, le=1)
    heuristic: Decimal = Field(default=Decimal("0.30"), ge=0, le=1)
    missing: Decimal = Field(default=Decimal("0.00"), ge=0, le=1)


class ScoringRunRequest(BaseModel):
    profile_name: str | None = None
    restart_failed_run: bool = True
    allow_completed_run_rerun: bool = True
    evidence_quality_weights: EvidenceQualityWeightsRequest = Field(
        default_factory=EvidenceQualityWeightsRequest
    )


class ScoringStatusCountResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    status: str
    count: int


class ProvenanceInputDetailResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    input_name: str
    input_value: str
    evidence_quality: str


class FactorDetailBreakdownResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    factor_id: str
    points_awarded: Decimal
    rationale: str | None
    inputs: list[ProvenanceInputDetailResponse]


class BonusDetailBreakdownResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    bonus_id: str
    applied: bool
    points_awarded: Decimal
    rationale: str | None


class ScoringSummaryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

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
    status_counts: list[ScoringStatusCountResponse]
    failure_reason: str | None


class ParcelScoringDetailResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    run_id: str
    batch_id: str
    metro_id: str
    profile_name: str | None
    parcel_id: str
    status: str
    status_reason: str | None
    viability_score: Decimal | None
    confidence_score: Decimal | None
    factor_details: list[FactorDetailBreakdownResponse]
    bonus_details: list[BonusDetailBreakdownResponse]
    evidence_quality_counts: dict[str, int]
