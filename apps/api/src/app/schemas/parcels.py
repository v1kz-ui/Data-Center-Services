from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class ParcelSearchResultResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    parcel_id: str
    metro_id: str
    county_fips: str
    apn: str | None
    acreage: Decimal
    zoning_code: str | None
    land_use_code: str | None
    rep_point_wkt: str | None
    evidence_count: int
    lineage_key: str


class ParcelSearchPageResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    total_count: int
    limit: int
    offset: int
    items: list[ParcelSearchResultResponse]
