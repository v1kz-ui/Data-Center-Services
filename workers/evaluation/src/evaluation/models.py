from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal


@dataclass(slots=True)
class EvidenceExclusionRule:
    source_id: str
    attribute_name: str
    blocked_values: tuple[str, ...]
    exclusion_code: str
    exclusion_reason: str


@dataclass(slots=True)
class EvaluationPolicy:
    rule_version: str = "phase4-default"
    allowed_band_wkt: str | None = None
    minimum_acreage: Decimal = Decimal("0")
    blocked_zoning_codes: tuple[str, ...] = ()
    blocked_land_use_codes: tuple[str, ...] = ()
    evidence_exclusion_rules: tuple[EvidenceExclusionRule, ...] = ()
    restart_failed_run: bool = True


@dataclass(slots=True)
class EvaluationStatusCount:
    status: str
    count: int


@dataclass(slots=True)
class EvaluationSummary:
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
    status_counts: list[EvaluationStatusCount] = field(default_factory=list)
    failure_reason: str | None = None
