from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from uuid import UUID

from shapely import wkt
from shapely.errors import ShapelyError
from shapely.geometry import Point
from shapely.strtree import STRtree
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models.market import MarketListing
from app.db.models.territory import ParcelRepPoint, RawParcel
from app.services.live_candidate_scoring import build_ranked_live_candidate_pool

_TOP_CANDIDATE_LIMIT = 1000
_POINT_QUERY_RADII_DEGREES = (0.0005, 0.001, 0.0025, 0.005, 0.01, 0.02)
_POINT_LINK_BUFFER_DEGREES = 0.00035
_MAX_PARCEL_CANDIDATES_PER_LISTING = 32
_METRO_NAME_TO_ID = {
    "Dallas-Fort Worth": "DFW",
    "Houston": "HOU",
    "Austin": "AUS",
    "San Antonio": "SAT",
    "Rio Grande Valley": "MFE",
    "El Paso": "ELP",
    "Brazos Valley": "BRV",
}


@dataclass(frozen=True, slots=True)
class CandidateParcelTarget:
    market_listing_id: str
    metro: str
    lat: float
    lon: float
    site_name: str
    source_listing_key: str
    source_url: str


@dataclass(frozen=True, slots=True)
class ParcelLinkRecord:
    market_listing_id: str
    source_listing_key: str
    site_name: str
    metro: str
    metro_id: str | None
    matched: bool
    county_fips: str | None
    parcel_id: str | None
    candidate_parcel_count: int
    match_strategy: str | None
    source_url: str


@dataclass(frozen=True, slots=True)
class ParcelLinkReport:
    requested_limit: int
    candidate_count: int
    matched_count: int
    unmatched_count: int
    coverage_rate: float
    metro_counts: dict[str, int]
    matched_by_metro: dict[str, int]
    coverage_gaps: dict[str, int]
    records: list[ParcelLinkRecord] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "requested_limit": self.requested_limit,
            "candidate_count": self.candidate_count,
            "matched_count": self.matched_count,
            "unmatched_count": self.unmatched_count,
            "coverage_rate": self.coverage_rate,
            "metro_counts": dict(self.metro_counts),
            "matched_by_metro": dict(self.matched_by_metro),
            "coverage_gaps": dict(self.coverage_gaps),
            "records": [asdict(record) for record in self.records],
        }


@dataclass(frozen=True, slots=True)
class _ParcelRepPointRecord:
    parcel_id: str
    county_fips: str
    point: Point


def build_top_live_candidate_targets(
    session: Session,
    *,
    limit: int = _TOP_CANDIDATE_LIMIT,
) -> list[CandidateParcelTarget]:
    ranked = build_ranked_live_candidate_pool(
        session,
        limit=limit,
        per_metro_cap=None,
    )
    return [
        CandidateParcelTarget(
            market_listing_id=str(record["market_listing_id"]),
            metro=str(record["metro"]),
            lat=float(record["lat"]),
            lon=float(record["lon"]),
            site_name=str(record["site_name"]),
            source_listing_key=str(record["source_listing_key"]),
            source_url=str(record["source_url"]),
        )
        for record in ranked
    ]


def link_top_live_candidates_to_parcels(
    session: Session,
    *,
    limit: int = _TOP_CANDIDATE_LIMIT,
    write_changes: bool = True,
) -> ParcelLinkReport:
    targets = build_top_live_candidate_targets(session, limit=limit)
    return link_candidate_targets_to_parcels(
        session,
        targets=targets,
        requested_limit=limit,
        write_changes=write_changes,
    )


def link_candidate_targets_to_parcels(
    session: Session,
    *,
    targets: list[CandidateParcelTarget],
    requested_limit: int,
    write_changes: bool = True,
) -> ParcelLinkReport:
    if not targets:
        return ParcelLinkReport(
            requested_limit=requested_limit,
            candidate_count=0,
            matched_count=0,
            unmatched_count=0,
            coverage_rate=0.0,
            metro_counts={},
            matched_by_metro={},
            coverage_gaps={},
            records=[],
        )

    metro_counts: dict[str, int] = {}
    matched_by_metro: dict[str, int] = {}
    coverage_gaps: dict[str, int] = {}
    records: list[ParcelLinkRecord] = []
    targets_by_metro_id: dict[str, list[CandidateParcelTarget]] = {}

    for target in targets:
        metro_counts[target.metro] = metro_counts.get(target.metro, 0) + 1
        metro_id = _METRO_NAME_TO_ID.get(target.metro)
        if metro_id is None:
            coverage_gaps[target.metro] = coverage_gaps.get(target.metro, 0) + 1
            records.append(
                ParcelLinkRecord(
                    market_listing_id=target.market_listing_id,
                    source_listing_key=target.source_listing_key,
                    site_name=target.site_name,
                    metro=target.metro,
                    metro_id=None,
                    matched=False,
                    county_fips=None,
                    parcel_id=None,
                    candidate_parcel_count=0,
                    match_strategy=None,
                    source_url=target.source_url,
                )
            )
            continue
        targets_by_metro_id.setdefault(metro_id, []).append(target)

    listing_updates: dict[str, tuple[str, str]] = {}
    for metro_id, metro_targets in targets_by_metro_id.items():
        metro_name = metro_targets[0].metro
        metro_records = _link_targets_in_metro(session, metro_id=metro_id, targets=metro_targets)
        if not metro_records:
            coverage_gaps[metro_name] = coverage_gaps.get(metro_name, 0) + len(metro_targets)
        for record in metro_records:
            records.append(record)
            if record.matched and record.parcel_id is not None and record.county_fips is not None:
                listing_updates[record.market_listing_id] = (record.parcel_id, record.county_fips)
                matched_by_metro[record.metro] = matched_by_metro.get(record.metro, 0) + 1
            elif record.matched is False and record.candidate_parcel_count == 0:
                coverage_gaps[record.metro] = coverage_gaps.get(record.metro, 0) + 1

    if write_changes and listing_updates:
        listing_ids = [UUID(market_listing_id) for market_listing_id in listing_updates]
        listings = session.scalars(
            select(MarketListing).where(MarketListing.market_listing_id.in_(listing_ids))
        ).all()
        for listing in listings:
            parcel_id, county_fips = listing_updates[str(listing.market_listing_id)]
            listing.parcel_id = parcel_id
            listing.county_fips = county_fips

    matched_count = len(listing_updates)
    candidate_count = len(targets)
    unmatched_count = candidate_count - matched_count
    coverage_rate = round(matched_count / candidate_count, 4) if candidate_count else 0.0
    ordered_records = sorted(
        records,
        key=lambda record: (
            record.metro,
            record.site_name,
            record.source_listing_key,
        ),
    )
    return ParcelLinkReport(
        requested_limit=requested_limit,
        candidate_count=candidate_count,
        matched_count=matched_count,
        unmatched_count=unmatched_count,
        coverage_rate=coverage_rate,
        metro_counts=metro_counts,
        matched_by_metro=matched_by_metro,
        coverage_gaps=coverage_gaps,
        records=ordered_records,
    )


def write_parcel_link_report(
    report: ParcelLinkReport,
    *,
    output_path: str | Path,
) -> Path:
    import json

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")
    return path


def _link_targets_in_metro(
    session: Session,
    *,
    metro_id: str,
    targets: list[CandidateParcelTarget],
) -> list[ParcelLinkRecord]:
    parcel_rows = session.execute(
        select(RawParcel.parcel_id, RawParcel.county_fips, ParcelRepPoint.rep_point_wkt)
        .join(ParcelRepPoint, ParcelRepPoint.parcel_id == RawParcel.parcel_id)
        .where(
            RawParcel.metro_id == metro_id,
            RawParcel.is_active.is_(True),
        )
    ).all()
    if not parcel_rows:
        return [
            ParcelLinkRecord(
                market_listing_id=target.market_listing_id,
                source_listing_key=target.source_listing_key,
                site_name=target.site_name,
                metro=target.metro,
                metro_id=metro_id,
                matched=False,
                county_fips=None,
                parcel_id=None,
                candidate_parcel_count=0,
                match_strategy=None,
                source_url=target.source_url,
            )
            for target in targets
        ]

    rep_points: list[_ParcelRepPointRecord] = []
    geometries: list[Point] = []
    for parcel_id, county_fips, rep_point_wkt in parcel_rows:
        try:
            point = wkt.loads(rep_point_wkt)
        except (TypeError, ShapelyError):
            continue
        if point.is_empty or not isinstance(point, Point):
            continue
        rep_points.append(
            _ParcelRepPointRecord(
                parcel_id=parcel_id,
                county_fips=county_fips,
                point=point,
            )
        )
        geometries.append(point)

    if not rep_points:
        return [
            ParcelLinkRecord(
                market_listing_id=target.market_listing_id,
                source_listing_key=target.source_listing_key,
                site_name=target.site_name,
                metro=target.metro,
                metro_id=metro_id,
                matched=False,
                county_fips=None,
                parcel_id=None,
                candidate_parcel_count=0,
                match_strategy=None,
                source_url=target.source_url,
            )
            for target in targets
        ]

    tree = STRtree(geometries)
    candidate_parcel_ids_by_listing: dict[str, list[_ParcelRepPointRecord]] = {}
    needed_parcel_ids: set[str] = set()
    for target in targets:
        listing_point = Point(target.lon, target.lat)
        candidate_records = _candidate_parcels_for_point(
            tree=tree,
            rep_points=rep_points,
            listing_point=listing_point,
        )
        candidate_parcel_ids_by_listing[target.market_listing_id] = candidate_records
        for record in candidate_records:
            needed_parcel_ids.add(record.parcel_id)

    polygons = _load_parcel_geometries(session, parcel_ids=needed_parcel_ids)

    linked_records: list[ParcelLinkRecord] = []
    for target in targets:
        listing_point = Point(target.lon, target.lat)
        candidate_records = candidate_parcel_ids_by_listing[target.market_listing_id]
        match: tuple[str, str, str] | None = None
        for rep_record in candidate_records:
            polygon = polygons.get(rep_record.parcel_id)
            if polygon is None or polygon.is_empty:
                continue
            if polygon.covers(listing_point):
                match = (rep_record.parcel_id, rep_record.county_fips, "polygon_cover")
                break
            if polygon.buffer(_POINT_LINK_BUFFER_DEGREES).covers(listing_point):
                match = (rep_record.parcel_id, rep_record.county_fips, "buffered_cover")
                break

        if match is None:
            linked_records.append(
                ParcelLinkRecord(
                    market_listing_id=target.market_listing_id,
                    source_listing_key=target.source_listing_key,
                    site_name=target.site_name,
                    metro=target.metro,
                    metro_id=metro_id,
                    matched=False,
                    county_fips=None,
                    parcel_id=None,
                    candidate_parcel_count=len(candidate_records),
                    match_strategy=None,
                    source_url=target.source_url,
                )
            )
            continue

        parcel_id, county_fips, match_strategy = match
        linked_records.append(
            ParcelLinkRecord(
                market_listing_id=target.market_listing_id,
                source_listing_key=target.source_listing_key,
                site_name=target.site_name,
                metro=target.metro,
                metro_id=metro_id,
                matched=True,
                county_fips=county_fips,
                parcel_id=parcel_id,
                candidate_parcel_count=len(candidate_records),
                match_strategy=match_strategy,
                source_url=target.source_url,
            )
        )

    return linked_records


def _candidate_parcels_for_point(
    *,
    tree: STRtree,
    rep_points: list[_ParcelRepPointRecord],
    listing_point: Point,
) -> list[_ParcelRepPointRecord]:
    seen_indexes: set[int] = set()
    candidate_indexes: list[int] = []
    for radius in _POINT_QUERY_RADII_DEGREES:
        query_geometry = listing_point.buffer(radius)
        indexes = tree.query(query_geometry)
        for raw_index in indexes:
            index = int(raw_index)
            if index in seen_indexes:
                continue
            seen_indexes.add(index)
            candidate_indexes.append(index)
        if len(candidate_indexes) >= _MAX_PARCEL_CANDIDATES_PER_LISTING:
            break

    if not candidate_indexes:
        nearest_index = tree.nearest(listing_point)
        if nearest_index is not None:
            candidate_indexes.append(int(nearest_index))

    ordered_indexes = sorted(
        candidate_indexes,
        key=lambda index: listing_point.distance(rep_points[index].point),
    )
    return [
        rep_points[index]
        for index in ordered_indexes[:_MAX_PARCEL_CANDIDATES_PER_LISTING]
    ]


def _load_parcel_geometries(
    session: Session,
    *,
    parcel_ids: set[str],
) -> dict[str, Any]:
    if not parcel_ids:
        return {}

    geometries: dict[str, Any] = {}
    parcel_id_list = sorted(parcel_ids)
    batch_size = 500
    for offset in range(0, len(parcel_id_list), batch_size):
        batch = parcel_id_list[offset : offset + batch_size]
        rows = session.execute(
            select(RawParcel.parcel_id, RawParcel.geometry_wkt).where(RawParcel.parcel_id.in_(batch))
        ).all()
        for parcel_id, geometry_wkt in rows:
            try:
                polygon = wkt.loads(geometry_wkt)
            except (TypeError, ShapelyError):
                continue
            if polygon.is_empty:
                continue
            geometries[parcel_id] = polygon
    return geometries
