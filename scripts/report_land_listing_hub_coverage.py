from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.request import urlopen

REPO_ROOT = Path(__file__).resolve().parents[1]
APP_SRC = REPO_ROOT / "apps" / "api" / "src"
if str(APP_SRC) not in sys.path:
    sys.path.insert(0, str(APP_SRC))

from sqlalchemy import create_engine, text  # noqa: E402

from app.core.settings import get_settings  # noqa: E402

CENSUS_ACS_POPULATION_URL = (
    "https://api.census.gov/data/2024/acs/acs5"
    "?get=NAME,B01003_001E&for=place:*&in=state:48"
)
CENSUS_GEOINFO_URL = (
    "https://api.census.gov/data/2024/geoinfo"
    "?get=NAME,INTPTLAT,INTPTLON&for=place:*&in=state:48"
)
EARTH_RADIUS_MILES = 3958.7613
LAND_TEXT_PATTERN = re.compile(
    r"\b(land|acreage|acres?|ranch|farm|parcel|tract|lots?)\b",
    flags=re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class PopulationHub:
    name: str
    place_geoid: str
    population: int
    latitude: float
    longitude: float
    census_name: str


@dataclass(frozen=True, slots=True)
class LandListing:
    market_listing_id: str
    listing_source_id: str
    listing_title: str
    asset_type: str | None
    city: str | None
    state_code: str | None
    postal_code: str | None
    acreage: float | None
    asking_price: float | None
    latitude: float | None
    longitude: float | None
    source_url: str
    last_verified_at: str | None


@dataclass(frozen=True, slots=True)
class MatchedListing:
    listing: LandListing
    hub: PopulationHub
    distance_miles: float
    dedupe_key: str


def _read_json_url(url: str) -> list[list[str]]:
    with urlopen(url, timeout=45) as response:
        payload = response.read().decode("utf-8")
    rows = json.loads(payload)
    if not rows or not isinstance(rows, list):
        raise RuntimeError(f"Census API returned an empty response for {url}")
    return rows


def _rows_by_header(rows: list[list[str]]) -> list[dict[str, str]]:
    header = rows[0]
    return [dict(zip(header, row, strict=False)) for row in rows[1:]]


def _clean_census_place_name(value: str) -> str:
    name = value.removesuffix(", Texas")
    for suffix in (" city", " town", " village", " CDP"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def fetch_population_hubs(population_threshold: int) -> list[PopulationHub]:
    population_rows = _rows_by_header(_read_json_url(CENSUS_ACS_POPULATION_URL))
    geoinfo_rows = _rows_by_header(_read_json_url(CENSUS_GEOINFO_URL))
    geoinfo_by_place = {row["place"]: row for row in geoinfo_rows}

    hubs: list[PopulationHub] = []
    for row in population_rows:
        population = int(row["B01003_001E"])
        if population < population_threshold:
            continue
        geoinfo = geoinfo_by_place.get(row["place"])
        if geoinfo is None:
            continue
        hubs.append(
            PopulationHub(
                name=_clean_census_place_name(row["NAME"]),
                place_geoid=f"{row['state']}{row['place']}",
                population=population,
                latitude=float(geoinfo["INTPTLAT"]),
                longitude=float(geoinfo["INTPTLON"]),
                census_name=row["NAME"],
            )
        )

    return sorted(hubs, key=lambda hub: (-hub.population, hub.name))


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def _is_land_sale_listing(row: dict[str, Any]) -> bool:
    status = str(row.get("listing_status") or "").strip().lower()
    if status not in {"sale", "for_sale", "for sale"}:
        return False
    asset_type = str(row.get("asset_type") or "")
    listing_title = str(row.get("listing_title") or "")
    return "land" in asset_type.lower() or bool(LAND_TEXT_PATTERN.search(listing_title))


def fetch_land_sale_listings() -> list[LandListing]:
    engine = create_engine(get_settings().database_url, future=True)
    statement = text(
        """
        select
          market_listing_id::text as market_listing_id,
          listing_source_id,
          listing_title,
          asset_type,
          listing_status,
          city,
          state_code,
          postal_code,
          acreage,
          asking_price,
          latitude,
          longitude,
          source_url,
          last_verified_at
        from market_listing
        where is_active
          and (state_code = 'TX' or metro_id = 'TX')
        """
    )
    listings: list[LandListing] = []
    with engine.connect() as conn:
        rows = conn.execute(statement).mappings().all()
    for row in rows:
        row_dict = dict(row)
        if not _is_land_sale_listing(row_dict):
            continue
        verified_at = row_dict["last_verified_at"]
        listings.append(
            LandListing(
                market_listing_id=row_dict["market_listing_id"],
                listing_source_id=row_dict["listing_source_id"],
                listing_title=row_dict["listing_title"],
                asset_type=row_dict["asset_type"],
                city=row_dict["city"],
                state_code=row_dict["state_code"],
                postal_code=row_dict["postal_code"],
                acreage=_as_float(row_dict["acreage"]),
                asking_price=_as_float(row_dict["asking_price"]),
                latitude=_as_float(row_dict["latitude"]),
                longitude=_as_float(row_dict["longitude"]),
                source_url=row_dict["source_url"],
                last_verified_at=verified_at.isoformat() if verified_at is not None else None,
            )
        )
    return listings


def distance_miles(lat_a: float, lon_a: float, lat_b: float, lon_b: float) -> float:
    lat_a_r = math.radians(lat_a)
    lon_a_r = math.radians(lon_a)
    lat_b_r = math.radians(lat_b)
    lon_b_r = math.radians(lon_b)
    delta_lat = lat_b_r - lat_a_r
    delta_lon = lon_b_r - lon_a_r
    haversine = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat_a_r) * math.cos(lat_b_r) * math.sin(delta_lon / 2) ** 2
    )
    return 2 * EARTH_RADIUS_MILES * math.asin(min(1.0, math.sqrt(haversine)))


def _norm_text(value: str | None) -> str:
    return " ".join(str(value or "").strip().lower().split())


def build_dedupe_key(listing: LandListing) -> str:
    lat_key = round(float(listing.latitude or 0), 4)
    lon_key = round(float(listing.longitude or 0), 4)
    acreage_key = round(float(listing.acreage or 0), 2)
    price_key = round(float(listing.asking_price or 0), 0)
    location_key = "|".join(
        [
            _norm_text(listing.listing_title),
            _norm_text(listing.city),
            _norm_text(listing.postal_code),
        ]
    )
    return f"{location_key}|{lat_key}|{lon_key}|{acreage_key}|{price_key}"


def match_listings_to_hubs(
    listings: list[LandListing],
    hubs: list[PopulationHub],
    *,
    radius_miles: float,
) -> tuple[list[MatchedListing], list[MatchedListing], list[LandListing], list[LandListing]]:
    nearest_matches: list[MatchedListing] = []
    hub_radius_matches: list[MatchedListing] = []
    outside_radius: list[LandListing] = []
    ungeocoded: list[LandListing] = []

    for listing in listings:
        if listing.latitude is None or listing.longitude is None:
            ungeocoded.append(listing)
            continue

        dedupe_key = build_dedupe_key(listing)
        listing_radius_matches: list[MatchedListing] = []
        for hub in hubs:
            distance = distance_miles(
                float(listing.latitude),
                float(listing.longitude),
                hub.latitude,
                hub.longitude,
            )
            if distance <= radius_miles:
                listing_radius_matches.append(
                    MatchedListing(
                        listing=listing,
                        hub=hub,
                        distance_miles=distance,
                        dedupe_key=dedupe_key,
                    )
                )

        if not listing_radius_matches:
            outside_radius.append(listing)
            continue

        hub_radius_matches.extend(listing_radius_matches)
        nearest_matches.append(
            min(
                listing_radius_matches,
                key=lambda item: (item.distance_miles, -item.hub.population),
            )
        )

    return nearest_matches, hub_radius_matches, outside_radius, ungeocoded


def build_report(
    hubs: list[PopulationHub],
    listings: list[LandListing],
    nearest_matches: list[MatchedListing],
    hub_radius_matches: list[MatchedListing],
    outside_radius: list[LandListing],
    ungeocoded: list[LandListing],
    *,
    radius_miles: float,
    population_threshold: int,
    sample_limit: int,
) -> dict[str, Any]:
    matches_by_hub: dict[str, list[MatchedListing]] = defaultdict(list)
    nearest_matches_by_hub: dict[str, list[MatchedListing]] = defaultdict(list)
    for item in hub_radius_matches:
        matches_by_hub[item.hub.place_geoid].append(item)
    for item in nearest_matches:
        nearest_matches_by_hub[item.hub.place_geoid].append(item)

    hub_rows: list[dict[str, Any]] = []
    for hub in hubs:
        hub_matches = matches_by_hub.get(hub.place_geoid, [])
        nearest_hub_matches = nearest_matches_by_hub.get(hub.place_geoid, [])
        source_counts = Counter(item.listing.listing_source_id for item in hub_matches)
        dedupe_count = len({item.dedupe_key for item in hub_matches})
        closest_samples = sorted(hub_matches, key=lambda item: item.distance_miles)[
            :sample_limit
        ]
        hub_rows.append(
            {
                "hub_name": hub.name,
                "place_geoid": hub.place_geoid,
                "population": hub.population,
                "latitude": hub.latitude,
                "longitude": hub.longitude,
                "matched_listing_count": len(hub_matches),
                "nearest_assigned_listing_count": len(nearest_hub_matches),
                "deduped_listing_count": dedupe_count,
                "source_counts": dict(sorted(source_counts.items())),
                "closest_samples": [
                    {
                        "listing_title": item.listing.listing_title,
                        "listing_source_id": item.listing.listing_source_id,
                        "city": item.listing.city,
                        "acreage": item.listing.acreage,
                        "asking_price": item.listing.asking_price,
                        "distance_miles": round(item.distance_miles, 2),
                        "source_url": item.listing.source_url,
                    }
                    for item in closest_samples
                ],
            }
        )

    source_counts = Counter(item.listing_source_id for item in listings)
    matched_source_counts = Counter(item.listing.listing_source_id for item in nearest_matches)

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "radius_miles": radius_miles,
        "hub_population_threshold": population_threshold,
        "hub_source": {
            "population_api": CENSUS_ACS_POPULATION_URL,
            "geography_api": CENSUS_GEOINFO_URL,
            "population_field": "2024 ACS 5-year B01003_001E total population",
            "geography_fields": "2024 Census geoinfo INTPTLAT/INTPTLON",
        },
        "listing_filter": (
            "active Texas market_listing rows with sale status and land-like asset_type "
            "or land-like title terms"
        ),
        "hub_count": len(hubs),
        "active_sale_land_listing_count": len(listings),
        "geocoded_sale_land_listing_count": len(listings) - len(ungeocoded),
        "within_radius_listing_count": len(nearest_matches),
        "within_radius_deduped_listing_count": len(
            {item.dedupe_key for item in nearest_matches}
        ),
        "hub_listing_pair_count": len(hub_radius_matches),
        "outside_radius_listing_count": len(outside_radius),
        "ungeocoded_listing_count": len(ungeocoded),
        "source_counts": dict(sorted(source_counts.items())),
        "matched_source_counts": dict(sorted(matched_source_counts.items())),
        "zero_listing_hubs": [
            row["hub_name"] for row in hub_rows if row["matched_listing_count"] == 0
        ],
        "coverage_caveats": [
            (
                "This proves coverage for active, geocoded listings already loaded "
                "into market_listing."
            ),
            "A single public marketplace cannot guarantee every land listing for sale in Texas.",
            (
                "To approach complete market coverage, add authorized MLS/RESO, "
                "ListHub, Crexi partnership/API, Land.com or broker feeds, and "
                "public surplus/auction sources."
            ),
        ],
        "hubs": sorted(
            hub_rows,
            key=lambda row: (
                row["matched_listing_count"] == 0,
                -int(row["matched_listing_count"]),
                str(row["hub_name"]),
            ),
        ),
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_hub_csv(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "hub_name",
        "place_geoid",
        "population",
        "latitude",
        "longitude",
        "matched_listing_count",
        "nearest_assigned_listing_count",
        "deduped_listing_count",
        "source_counts",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in report["hubs"]:
            writer.writerow({field: row[field] for field in fieldnames})


def write_matches_csv(path: Path, matched: list[MatchedListing]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "hub_name",
        "hub_population",
        "distance_miles",
        "market_listing_id",
        "listing_source_id",
        "listing_title",
        "asset_type",
        "city",
        "state_code",
        "postal_code",
        "acreage",
        "asking_price",
        "latitude",
        "longitude",
        "source_url",
        "last_verified_at",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for item in sorted(
            matched,
            key=lambda match: (
                match.hub.name,
                round(match.distance_miles, 4),
                match.listing.listing_title,
            ),
        ):
            writer.writerow(
                {
                    "hub_name": item.hub.name,
                    "hub_population": item.hub.population,
                    "distance_miles": round(item.distance_miles, 2),
                    **asdict(item.listing),
                }
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Audit active Texas land-for-sale listing coverage within a radius of "
            "major Census population hubs."
        )
    )
    parser.add_argument("--population-threshold", type=int, default=100_000)
    parser.add_argument("--radius-miles", type=float, default=30.0)
    parser.add_argument(
        "--json-output",
        type=Path,
        default=Path("temp/land_listing_hub_coverage.json"),
    )
    parser.add_argument(
        "--hub-csv-output",
        type=Path,
        default=Path("temp/land_listing_hub_coverage.csv"),
    )
    parser.add_argument(
        "--matches-csv-output",
        type=Path,
        default=Path("temp/land_listing_hub_matches.csv"),
    )
    parser.add_argument("--sample-limit", type=int, default=5)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    hubs = fetch_population_hubs(args.population_threshold)
    listings = fetch_land_sale_listings()
    nearest_matches, hub_radius_matches, outside_radius, ungeocoded = match_listings_to_hubs(
        listings,
        hubs,
        radius_miles=args.radius_miles,
    )
    report = build_report(
        hubs,
        listings,
        nearest_matches,
        hub_radius_matches,
        outside_radius,
        ungeocoded,
        radius_miles=args.radius_miles,
        population_threshold=args.population_threshold,
        sample_limit=args.sample_limit,
    )

    write_json(args.json_output, report)
    write_hub_csv(args.hub_csv_output, report)
    write_matches_csv(args.matches_csv_output, hub_radius_matches)

    print(
        json.dumps(
            {
                "hub_count": report["hub_count"],
                "active_sale_land_listing_count": report[
                    "active_sale_land_listing_count"
                ],
                "within_radius_listing_count": report["within_radius_listing_count"],
                "within_radius_deduped_listing_count": report[
                    "within_radius_deduped_listing_count"
                ],
                "hub_listing_pair_count": report["hub_listing_pair_count"],
                "outside_radius_listing_count": report["outside_radius_listing_count"],
                "ungeocoded_listing_count": report["ungeocoded_listing_count"],
                "zero_listing_hub_count": len(report["zero_listing_hubs"]),
                "json_output": str(args.json_output),
                "hub_csv_output": str(args.hub_csv_output),
                "matches_csv_output": str(args.matches_csv_output),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
