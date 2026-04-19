from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db.models.source_data import RawZoning, SourceEvidence
from app.db.models.territory import ParcelRepPoint, RawParcel


@dataclass(slots=True)
class ParcelSearchResult:
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


@dataclass(slots=True)
class ParcelSearchPage:
    total_count: int
    limit: int
    offset: int
    items: list[ParcelSearchResult] = field(default_factory=list)


def search_parcels(
    session: Session,
    *,
    metro_id: str | None = None,
    county_fips: str | None = None,
    parcel_id: str | None = None,
    apn: str | None = None,
    min_acreage: Decimal | None = None,
    max_acreage: Decimal | None = None,
    zoning_code: str | None = None,
    land_use_code: str | None = None,
    limit: int = 25,
    offset: int = 0,
) -> ParcelSearchPage:
    zoning_subquery = (
        select(
            RawZoning.parcel_id.label("parcel_id"),
            RawZoning.zoning_code.label("zoning_code"),
            RawZoning.land_use_code.label("land_use_code"),
        )
        .where(RawZoning.is_active.is_(True))
        .subquery()
    )
    evidence_subquery = (
        select(
            SourceEvidence.parcel_id.label("parcel_id"),
            func.count(SourceEvidence.evidence_id).label("evidence_count"),
        )
        .where(
            SourceEvidence.is_active.is_(True),
            SourceEvidence.parcel_id.is_not(None),
        )
        .group_by(SourceEvidence.parcel_id)
        .subquery()
    )

    statement = (
        select(
            RawParcel.parcel_id,
            RawParcel.metro_id,
            RawParcel.county_fips,
            RawParcel.apn,
            RawParcel.acreage,
            RawParcel.lineage_key,
            ParcelRepPoint.rep_point_wkt,
            zoning_subquery.c.zoning_code,
            zoning_subquery.c.land_use_code,
            func.coalesce(evidence_subquery.c.evidence_count, 0).label("evidence_count"),
        )
        .select_from(RawParcel)
        .outerjoin(ParcelRepPoint, ParcelRepPoint.parcel_id == RawParcel.parcel_id)
        .outerjoin(zoning_subquery, zoning_subquery.c.parcel_id == RawParcel.parcel_id)
        .outerjoin(evidence_subquery, evidence_subquery.c.parcel_id == RawParcel.parcel_id)
        .where(RawParcel.is_active.is_(True))
    )

    if metro_id:
        statement = statement.where(RawParcel.metro_id == metro_id.strip().upper())
    if county_fips:
        statement = statement.where(RawParcel.county_fips == county_fips.strip())
    if parcel_id:
        statement = statement.where(RawParcel.parcel_id.ilike(f"%{parcel_id.strip()}%"))
    if apn:
        statement = statement.where(RawParcel.apn.ilike(f"%{apn.strip()}%"))
    if min_acreage is not None:
        statement = statement.where(RawParcel.acreage >= min_acreage)
    if max_acreage is not None:
        statement = statement.where(RawParcel.acreage <= max_acreage)
    if zoning_code:
        statement = statement.where(zoning_subquery.c.zoning_code == zoning_code.strip().upper())
    if land_use_code:
        statement = statement.where(
            zoning_subquery.c.land_use_code == land_use_code.strip().upper()
        )

    total_count = int(
        session.scalar(select(func.count()).select_from(statement.order_by(None).subquery())) or 0
    )
    rows = session.execute(
        statement.order_by(RawParcel.acreage.desc(), RawParcel.parcel_id)
        .limit(limit)
        .offset(offset)
    ).all()

    return ParcelSearchPage(
        total_count=total_count,
        limit=limit,
        offset=offset,
        items=[
            ParcelSearchResult(
                parcel_id=row.parcel_id,
                metro_id=row.metro_id,
                county_fips=row.county_fips,
                apn=row.apn,
                acreage=row.acreage,
                zoning_code=row.zoning_code,
                land_use_code=row.land_use_code,
                rep_point_wkt=row.rep_point_wkt,
                evidence_count=int(row.evidence_count or 0),
                lineage_key=row.lineage_key,
            )
            for row in rows
        ],
    )
