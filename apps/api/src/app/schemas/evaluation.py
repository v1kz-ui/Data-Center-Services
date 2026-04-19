from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class EvidenceExclusionRuleRequest(BaseModel):
    source_id: str = Field(min_length=1)
    attribute_name: str = Field(min_length=1)
    blocked_values: list[str] = Field(default_factory=list)
    exclusion_code: str = Field(min_length=1)
    exclusion_reason: str = Field(min_length=1)


class EvaluationRunRequest(BaseModel):
    rule_version: str = Field(default="phase4-default", min_length=1)
    allowed_band_wkt: str | None = None
    minimum_acreage: float = Field(default=0, ge=0)
    blocked_zoning_codes: list[str] = Field(default_factory=list)
    blocked_land_use_codes: list[str] = Field(default_factory=list)
    evidence_exclusion_rules: list[EvidenceExclusionRuleRequest] = Field(default_factory=list)
    restart_failed_run: bool = True


class EvaluationStatusCountResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    status: str
    count: int


class EvaluationSummaryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    run_id: str
    batch_id: str
    metro_id: str
    run_status: str
    rule_version: str
    evaluated_count: int
    band_filtered_count: int
    size_filtered_count: int
    excluded_count: int
    pending_scoring_count: int
    pending_exclusion_check_count: int
    status_counts: list[EvaluationStatusCountResponse]
    failure_reason: str | None


class EvaluationRunScopeResponse(BaseModel):
    run_id: str
    batch_id: str
    metro_id: str
    county_fips: list[str]
    parcel_count: int
