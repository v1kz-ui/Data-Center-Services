from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.db.models.market import ListingSourceCatalog, MarketListing


@dataclass(slots=True)
class MarketListingSearchResult:
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


@dataclass(slots=True)
class MarketListingSearchPage:
    total_count: int
    limit: int
    offset: int
    items: list[MarketListingSearchResult] = field(default_factory=list)


def search_market_listings(
    session: Session,
    *,
    listing_source_id: str | None = None,
    metro_id: str | None = None,
    county_fips: str | None = None,
    parcel_id: str | None = None,
    asset_type: str | None = None,
    listing_status: str | None = None,
    min_acreage: Decimal | None = None,
    max_acreage: Decimal | None = None,
    min_asking_price: Decimal | None = None,
    max_asking_price: Decimal | None = None,
    q: str | None = None,
    limit: int = 25,
    offset: int = 0,
) -> MarketListingSearchPage:
    statement = (
        select(
            MarketListing.market_listing_id,
            MarketListing.source_id,
            MarketListing.listing_source_id,
            ListingSourceCatalog.display_name.label("listing_source_name"),
            MarketListing.metro_id,
            MarketListing.county_fips,
            MarketListing.parcel_id,
            MarketListing.source_listing_key,
            MarketListing.listing_title,
            MarketListing.asset_type,
            MarketListing.listing_status,
            MarketListing.asking_price,
            MarketListing.acreage,
            MarketListing.building_sqft,
            MarketListing.address_line1,
            MarketListing.city,
            MarketListing.state_code,
            MarketListing.postal_code,
            MarketListing.latitude,
            MarketListing.longitude,
            MarketListing.broker_name,
            MarketListing.source_url,
            MarketListing.last_verified_at,
            MarketListing.lineage_key,
        )
        .select_from(MarketListing)
        .join(
            ListingSourceCatalog,
            ListingSourceCatalog.listing_source_id == MarketListing.listing_source_id,
        )
        .where(MarketListing.is_active.is_(True))
    )

    if listing_source_id:
        statement = statement.where(
            MarketListing.listing_source_id == listing_source_id.strip().lower()
        )
    if metro_id:
        statement = statement.where(MarketListing.metro_id == metro_id.strip().upper())
    if county_fips:
        statement = statement.where(MarketListing.county_fips == county_fips.strip())
    if parcel_id:
        statement = statement.where(MarketListing.parcel_id.ilike(f"%{parcel_id.strip()}%"))
    if asset_type:
        statement = statement.where(MarketListing.asset_type == asset_type.strip().lower())
    if listing_status:
        statement = statement.where(
            MarketListing.listing_status == listing_status.strip().lower()
        )
    if min_acreage is not None:
        statement = statement.where(MarketListing.acreage >= min_acreage)
    if max_acreage is not None:
        statement = statement.where(MarketListing.acreage <= max_acreage)
    if min_asking_price is not None:
        statement = statement.where(MarketListing.asking_price >= min_asking_price)
    if max_asking_price is not None:
        statement = statement.where(MarketListing.asking_price <= max_asking_price)
    if q:
        token = f"%{q.strip()}%"
        statement = statement.where(
            or_(
                MarketListing.listing_title.ilike(token),
                MarketListing.address_line1.ilike(token),
                MarketListing.city.ilike(token),
                MarketListing.broker_name.ilike(token),
            )
        )

    total_count = int(
        session.scalar(select(func.count()).select_from(statement.order_by(None).subquery())) or 0
    )
    rows = session.execute(
        statement.order_by(
            MarketListing.asking_price.asc().nulls_last(),
            MarketListing.acreage.desc().nulls_last(),
            MarketListing.listing_title,
        )
        .limit(limit)
        .offset(offset)
    ).all()

    return MarketListingSearchPage(
        total_count=total_count,
        limit=limit,
        offset=offset,
        items=[
            MarketListingSearchResult(
                market_listing_id=str(UUID(str(row.market_listing_id))),
                source_id=row.source_id,
                listing_source_id=row.listing_source_id,
                listing_source_name=row.listing_source_name,
                metro_id=row.metro_id,
                county_fips=row.county_fips,
                parcel_id=row.parcel_id,
                source_listing_key=row.source_listing_key,
                listing_title=row.listing_title,
                asset_type=row.asset_type,
                listing_status=row.listing_status,
                asking_price=row.asking_price,
                acreage=row.acreage,
                building_sqft=row.building_sqft,
                address_line1=row.address_line1,
                city=row.city,
                state_code=row.state_code,
                postal_code=row.postal_code,
                latitude=row.latitude,
                longitude=row.longitude,
                broker_name=row.broker_name,
                source_url=row.source_url,
                last_verified_at=row.last_verified_at,
                lineage_key=row.lineage_key,
            )
            for row in rows
        ],
    )
