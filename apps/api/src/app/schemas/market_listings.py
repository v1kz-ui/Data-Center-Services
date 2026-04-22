from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class MarketListingSearchResultResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    market_listing_id: str
    source_id: str
    listing_source_id: str
    listing_source_name: str
    metro_id: str
    county_fips: str | None
    parcel_id: str | None
    source_listing_key: str
    listing_title: str
    asset_type: str | None
    listing_status: str | None
    asking_price: Decimal | None
    acreage: Decimal | None
    building_sqft: Decimal | None
    address_line1: str | None
    city: str | None
    state_code: str | None
    postal_code: str | None
    latitude: Decimal | None
    longitude: Decimal | None
    broker_name: str | None
    source_url: str
    last_verified_at: datetime | None
    lineage_key: str


class MarketListingSearchPageResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    total_count: int
    limit: int
    offset: int
    items: list[MarketListingSearchResultResponse]
