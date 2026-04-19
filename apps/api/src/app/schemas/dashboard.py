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
    acreage_band: str
    distance_to_city_miles: int
    distance_to_university_miles: int
    viability_score: int
    power_score: int
    fiber_score: int
    water_score: int
    talent_score: int
    readiness_stage: str
    score_band: str
    strengths: list[str]
    summary: str
    lat: float
    lon: float


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
    data_coverage: dict[str, Any]
    monitoring: dict[str, Any]
