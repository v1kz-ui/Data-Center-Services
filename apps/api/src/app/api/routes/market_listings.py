from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.core.security import AppRole, require_roles
from app.db.session import get_db
from app.schemas.market_listings import MarketListingSearchPageResponse
from app.services.market_listings import search_market_listings

router = APIRouter(
    dependencies=[Depends(require_roles(AppRole.ADMIN, AppRole.OPERATOR, AppRole.READER))]
)
DbSession = Annotated[Session, Depends(get_db)]


@router.get("/market-listings/search", response_model=MarketListingSearchPageResponse)
def search_scraped_market_listings(
    db: DbSession,
    listing_source_id: Annotated[str | None, Query()] = None,
    metro_id: Annotated[str | None, Query()] = None,
    county_fips: Annotated[str | None, Query()] = None,
    parcel_id: Annotated[str | None, Query()] = None,
    asset_type: Annotated[str | None, Query()] = None,
    listing_status: Annotated[str | None, Query()] = None,
    min_acreage: Annotated[Decimal | None, Query(ge=0)] = None,
    max_acreage: Annotated[Decimal | None, Query(ge=0)] = None,
    min_asking_price: Annotated[Decimal | None, Query(ge=0)] = None,
    max_asking_price: Annotated[Decimal | None, Query(ge=0)] = None,
    q: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 25,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> MarketListingSearchPageResponse:
    page = search_market_listings(
        db,
        listing_source_id=listing_source_id,
        metro_id=metro_id,
        county_fips=county_fips,
        parcel_id=parcel_id,
        asset_type=asset_type,
        listing_status=listing_status,
        min_acreage=min_acreage,
        max_acreage=max_acreage,
        min_asking_price=min_asking_price,
        max_asking_price=max_asking_price,
        q=q,
        limit=limit,
        offset=offset,
    )
    return MarketListingSearchPageResponse.model_validate(page)
