from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class SourceInventoryEntryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    if_code: str
    name: str
    category: str
    phase: int
    protocol: str
    auth: str
    url: str | None
    target_table: str | None
    secondary_url: str | None
    target_partition: str | None
    county: str | None
    city: str | None
    reference_only: bool
    free: bool
    notes: list[str]


class SourceInventoryFlagResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    flag_key: str
    name: str
    phase: int
    metro: str
    target_behavior: str
    notes: list[str]


class SourceInventoryPhaseCountResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    phase: int
    source_count: int
    config_flag_count: int


class SourceInventoryCategoryCountResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    category: str
    source_count: int


class SourceInventorySummaryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    version: str
    captured_at: str | None
    total_sources: int
    total_config_flags: int
    filtered_source_count: int
    filtered_config_flag_count: int
    phase_counts: list[SourceInventoryPhaseCountResponse]
    category_counts: list[SourceInventoryCategoryCountResponse]
    sources: list[SourceInventoryEntryResponse]
    config_flags: list[SourceInventoryFlagResponse]


class SourceInventoryCoverageItemResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    if_code: str
    name: str
    category: str
    phase: int
    implemented: bool
    connector_keys: list[str]
    enabled_connector_keys: list[str]


class SourceInventoryCoveragePhaseResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    phase: int
    total_sources: int
    covered_sources: int
    uncovered_sources: int


class SourceInventoryCoverageResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    total_sources: int
    covered_sources: int
    uncovered_sources: int
    coverage_percent: float
    phase_coverage: list[SourceInventoryCoveragePhaseResponse]
    covered_if_codes: list[str]
    uncovered_if_codes: list[str]
    items: list[SourceInventoryCoverageItemResponse]
    unmapped_connector_keys: list[str]
    orphaned_connector_if_codes: list[str]
