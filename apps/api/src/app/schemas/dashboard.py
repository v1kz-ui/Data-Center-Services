from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


class DashboardMetricResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    label: str
    value: int
    detail: str
    tone: str


class TexasOpportunityResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    rank: int
    site_id: str
    site_name: str
    corridor_name: str
    metro: str
    city: str
    county: str
    region: str
    university_anchor: str
    acreage: float | None = None
    acreage_band: str
    distance_to_city_miles: int
    distance_to_university_miles: int
    viability_score: int
    power_score: int
    fiber_score: int
    water_score: int
    talent_score: int
    social_score: int
    political_score: int
    approval_score: int
    readiness_stage: str
    social_category: str
    political_category: str
    approval_stage: str
    approval_headwinds: list[str]
    approval_summary: str
    score_band: str
    strengths: list[str]
    summary: str
    lat: float
    lon: float
    confidence_score: int | None = None
    listing_source_id: str | None = None
    listing_status: str | None = None
    asking_price: float | None = None
    broker_name: str | None = None
    price_per_acre: float | None = None
    price_status: str | None = None
    source_name: str | None = None
    source_url: str | None = None
    source_listing_key: str | None = None
    market_listing_id: str | None = None
    rank_bucket: str | None = None
    evidence: dict[str, Any] | None = None
    market: dict[str, Any] | None = None
    nearest_substation_name: str | None = None
    nearest_substation_distance_miles: float | None = None
    nearest_substation_voltage_kv: float | None = None
    nearest_peering_facility_name: str | None = None
    nearest_peering_distance_miles: float | None = None
    nearest_peering_carrier_count: int | None = None
    nearest_highway_name: str | None = None
    nearest_highway_distance_miles: float | None = None
    nearest_water_name: str | None = None
    nearest_water_distance_miles: float | None = None


class DashboardCorridorResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    site_count: int
    average_viability_score: float
    priority_now_count: int
    lead_metro: str
    lead_university: str


class TexasDashboardSummaryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    app_name: str
    display_name: str
    environment: str
    version: str
    generated_at: str
    market: str
    data_mode: str
    hero_title: str
    hero_subtitle: str
    opportunity_count: int
    priority_now_count: int
    top_tier_count: int
    corridor_count: int
    university_count: int
    metrics: list[DashboardMetricResponse]
    filters: dict[str, list[str]]
    featured_opportunities: list[TexasOpportunityResponse]
    opportunities: list[TexasOpportunityResponse]
    corridors: list[DashboardCorridorResponse]
    snapshot: dict[str, Any] | None = None
    coverage: dict[str, Any] | None = None
    data_coverage: dict[str, Any]
    monitoring: dict[str, Any]
