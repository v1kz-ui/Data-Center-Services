from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.core.security import AppRole, require_roles
from app.db.session import get_db
from app.schemas.parcels import ParcelSearchPageResponse
from app.services.parcels import search_parcels

router = APIRouter(
    dependencies=[Depends(require_roles(AppRole.ADMIN, AppRole.OPERATOR, AppRole.READER))]
)
DbSession = Annotated[Session, Depends(get_db)]


@router.get("/parcels/search", response_model=ParcelSearchPageResponse)
def search_canonical_parcels(
    db: DbSession,
    metro_id: Annotated[str | None, Query()] = None,
    county_fips: Annotated[str | None, Query()] = None,
    parcel_id: Annotated[str | None, Query()] = None,
    apn: Annotated[str | None, Query()] = None,
    min_acreage: Annotated[Decimal | None, Query(ge=0)] = None,
    max_acreage: Annotated[Decimal | None, Query(ge=0)] = None,
    zoning_code: Annotated[str | None, Query()] = None,
    land_use_code: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 25,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> ParcelSearchPageResponse:
    page = search_parcels(
        db,
        metro_id=metro_id,
        county_fips=county_fips,
        parcel_id=parcel_id,
        apn=apn,
        min_acreage=min_acreage,
        max_acreage=max_acreage,
        zoning_code=zoning_code,
        land_use_code=land_use_code,
        limit=limit,
        offset=offset,
    )
    return ParcelSearchPageResponse.model_validate(page)
