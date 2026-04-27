from __future__ import annotations

from dataclasses import dataclass, field
from math import asin, ceil, cos, floor, radians, sin, sqrt
from statistics import median
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models.market import MarketListing
from app.db.models.source_data import SourceEvidence
from app.db.session import SessionLocal

_MAX_CITY_DISTANCE_MILES = 50.0
_MAX_UNIVERSITY_DISTANCE_MILES = 75.0
_MAX_POPULATION_ACCESS_DISTANCE_MILES = 30.0
_MAX_SUBSTATION_DISTANCE_MILES = 50.0
_MAX_POWER_PLANT_DISTANCE_MILES = 80.0
_MAX_PEERING_DISTANCE_MILES = 120.0
_MAX_HIGHWAY_DISTANCE_MILES = 25.0
_MAX_WATER_DISTANCE_MILES = 40.0
_DEFAULT_SHORTLIST_LIMIT = 136
_MAX_OPPORTUNITIES_PER_METRO = 30
_SWEET_SPOT_ACREAGE_MIN = 1.0
_SWEET_SPOT_ACREAGE_MAX = 2.0
_SHORTLIST_METRO_ALLOWLIST = frozenset(
    {
        "Dallas-Fort Worth",
        "Houston",
        "Austin",
        "San Antonio",
        "Brazos Valley",
        "El Paso",
        "Rio Grande Valley",
    }
)
_PRIMARY_FEATURED_METRO_MINIMUMS = {
    "Dallas-Fort Worth": 30,
    "Houston": 25,
    "Austin": 12,
    "San Antonio": 25,
    "Brazos Valley": 12,
    "El Paso": 12,
    "Rio Grande Valley": 12,
}
_PRIMARY_FEATURED_METRO_CAPS = {
    "Dallas-Fort Worth": 30,
    "Houston": 25,
    "Austin": 20,
    "San Antonio": 25,
}

_QUALITY_WEIGHT = {
    "measured": 1.0,
    "proxy": 0.6,
    "heuristic": 0.3,
}

_ASSET_TYPE_BASE_SCORE = {
    "commercial land": 1.00,
    "industrial properties": 0.88,
    "flex-office-warehouse": 0.82,
    "specialty real estate": 0.68,
    "office space": 0.28,
    "retail space": 0.10,
    "multifamily housing": 0.05,
    "health care and medical properties": 0.05,
    "- for sale": 0.10,
}

_KEYWORD_BONUSES = {
    "powered": 0.08,
    "industrial": 0.06,
    "distribution": 0.05,
    "warehouse": 0.05,
    "land": 0.05,
    "tract": 0.04,
    "campus": 0.04,
    "interstate": 0.03,
    "business park": 0.03,
}

_KEYWORD_PENALTIES = {
    "retail": 0.30,
    "office": 0.18,
    "medical": 0.30,
    "multifamily": 0.40,
    "restaurant": 0.35,
    "hotel": 0.25,
}


@dataclass(frozen=True, slots=True)
class CityAnchor:
    city: str
    metro: str
    county: str
    region: str
    corridor_name: str
    lat: float
    lon: float
    market_weight: float


@dataclass(frozen=True, slots=True)
class UniversityAnchor:
    name: str
    metro: str
    lat: float
    lon: float
    talent_weight: float


@dataclass(frozen=True, slots=True)
class PointAsset:
    record_key: str
    lat: float
    lon: float
    label: str
    attrs: dict[str, str]


@dataclass(frozen=True, slots=True)
class ListingCandidate:
    market_listing_id: str
    listing_source_id: str
    source_listing_key: str
    listing_title: str
    listing_status: str | None
    asset_type: str | None
    asking_price: float | None
    acreage: float | None
    building_sqft: float | None
    city: str | None
    state_code: str | None
    latitude: float
    longitude: float
    source_url: str
    broker_name: str | None


@dataclass(frozen=True, slots=True)
class ScoredCandidate:
    opportunity: dict[str, Any]
    confidence_score: int
    viability_score: int
    market_weight: float
    factor_scores: dict[str, float] = field(default_factory=dict)
    bonus_points: float = 0.0
    candidate: ListingCandidate | None = None
    city_anchor: CityAnchor | None = None
    city_distance: float | None = None
    university_anchor: UniversityAnchor | None = None
    university_distance: float | None = None
    substation_distance: float | None = None
    peering_distance: float | None = None


@dataclass(frozen=True, slots=True)
class ApprovalProfile:
    metro: str
    social_base: int
    political_base: int
    summary: str
    headwinds: tuple[str, ...]
    community_water_pressure: int
    political_water_pressure: int
    environment_pressure: int
    proximity_sensitivity: int
    scale_sensitivity: int


@dataclass(frozen=True, slots=True)
class ApprovalAssessment:
    social_score: int
    political_score: int
    approval_score: int
    social_category: str
    political_category: str
    approval_stage: str
    headwinds: tuple[str, ...]
    summary: str


_DEFAULT_APPROVAL_PROFILE = ApprovalProfile(
    metro="Texas",
    social_base=60,
    political_base=60,
    summary=(
        "Texas approval risk now hinges on written utility commitments, ERCOT large-load "
        "scrutiny, and how credibly a project limits potable-water use."
    ),
    headwinds=(
        "written resource commitments",
        "ERCOT large-load scrutiny",
        "community transparency demands",
    ),
    community_water_pressure=2,
    political_water_pressure=2,
    environment_pressure=2,
    proximity_sensitivity=1,
    scale_sensitivity=1,
)

_APPROVAL_PROFILES = {
    "Austin": ApprovalProfile(
        metro="Austin",
        social_base=55,
        political_base=52,
        summary=(
            "Central Texas is a water-first approval market where reclaimed-water access, quiet "
            "cooling, and buffers from homes and waterways matter early."
        ),
        headwinds=(
            "potable-water scrutiny",
            "transparency demands",
            "creek and flood-sensitive siting",
        ),
        community_water_pressure=3,
        political_water_pressure=4,
        environment_pressure=2,
        proximity_sensitivity=2,
        scale_sensitivity=2,
    ),
    "Dallas-Fort Worth": ApprovalProfile(
        metro="Dallas-Fort Worth",
        social_base=66,
        political_base=68,
        summary=(
            "DFW remains the best pure-market fit, but ozone, neighborhood-edge conflicts, and "
            "abatement politics can still reshape approvals."
        ),
        headwinds=(
            "ozone permitting scrutiny",
            "noise and light near homes",
            "abatement backlash",
        ),
        community_water_pressure=2,
        political_water_pressure=2,
        environment_pressure=3,
        proximity_sensitivity=2,
        scale_sensitivity=1,
    ),
    "Brazos Valley": ApprovalProfile(
        metro="Brazos Valley",
        social_base=46,
        political_base=44,
        summary=(
            "Bryan-College Station is highly trust-sensitive, with groundwater dependence and "
            "recent public backlash making approval very site-specific."
        ),
        headwinds=(
            "groundwater dependence",
            "trust and secrecy backlash",
            "farmland and neighborhood fit",
        ),
        community_water_pressure=4,
        political_water_pressure=4,
        environment_pressure=1,
        proximity_sensitivity=3,
        scale_sensitivity=2,
    ),
    "Houston": ApprovalProfile(
        metro="Houston",
        social_base=62,
        political_base=64,
        summary=(
            "Houston is more industrially comfortable than most metros, but flood hardening, "
            "abatement asks, and gas-generation pairings drive political risk."
        ),
        headwinds=(
            "flood hardening",
            "abatement backlash",
            "power-plant pairing scrutiny",
        ),
        community_water_pressure=1,
        political_water_pressure=2,
        environment_pressure=3,
        proximity_sensitivity=2,
        scale_sensitivity=1,
    ),
    "San Antonio": ApprovalProfile(
        metro="San Antonio",
        social_base=59,
        political_base=60,
        summary=(
            "San Antonio has stronger utility planning than its politics suggest, but aquifer "
            "sensitivity and utility-bill anxiety quickly become public issues."
        ),
        headwinds=(
            "aquifer sensitivity",
            "utility-bill anxiety",
            "ozone backup-power scrutiny",
        ),
        community_water_pressure=3,
        political_water_pressure=3,
        environment_pressure=3,
        proximity_sensitivity=2,
        scale_sensitivity=2,
    ),
    "El Paso": ApprovalProfile(
        metro="El Paso",
        social_base=47,
        political_base=45,
        summary=(
            "El Paso is technically innovative on water, but desert scarcity, air quality, and "
            "recent resilience shocks make large new loads politically symbolic."
        ),
        headwinds=(
            "desert water symbolism",
            "air quality scrutiny",
            "infrastructure resilience anxiety",
        ),
        community_water_pressure=4,
        political_water_pressure=4,
        environment_pressure=3,
        proximity_sensitivity=2,
        scale_sensitivity=2,
    ),
    "Rio Grande Valley": ApprovalProfile(
        metro="Rio Grande Valley",
        social_base=42,
        political_base=40,
        summary=(
            "The Valley is one of the hardest Texas markets for potable-water-intensive designs, "
            "with drought, flood risk, and cross-border delivery politics all active at once."
        ),
        headwinds=(
            "potable-water scarcity",
            "flood and drainage exposure",
            "cross-border water politics",
        ),
        community_water_pressure=4,
        political_water_pressure=4,
        environment_pressure=2,
        proximity_sensitivity=2,
        scale_sensitivity=2,
    ),
    "Corpus Christi": ApprovalProfile(
        metro="Corpus Christi",
        social_base=58,
        political_base=59,
        summary=(
            "Corpus Christi is industrially oriented, but coastal flooding and drainage resilience "
            "still matter materially for large utility loads."
        ),
        headwinds=(
            "coastal flooding",
            "industrial emissions scrutiny",
            "drainage resilience",
        ),
        community_water_pressure=2,
        political_water_pressure=2,
        environment_pressure=2,
        proximity_sensitivity=1,
        scale_sensitivity=1,
    ),
    "Laredo": ApprovalProfile(
        metro="Laredo",
        social_base=60,
        political_base=61,
        summary=(
            "Laredo is logistics-friendly, but cross-border infrastructure politics and written "
            "resource commitments still shape approval comfort."
        ),
        headwinds=(
            "cross-border infrastructure politics",
            "written water commitments",
            "industrial edge compatibility",
        ),
        community_water_pressure=2,
        political_water_pressure=2,
        environment_pressure=1,
        proximity_sensitivity=1,
        scale_sensitivity=1,
    ),
    "Midland": ApprovalProfile(
        metro="Midland",
        social_base=63,
        political_base=65,
        summary=(
            "Midland is comparatively industry-tolerant, but water sourcing transparency and grid "
            "study cost exposure still matter for data-center politics."
        ),
        headwinds=(
            "water sourcing transparency",
            "grid study cost exposure",
            "energy-transition politics",
        ),
        community_water_pressure=2,
        political_water_pressure=2,
        environment_pressure=1,
        proximity_sensitivity=1,
        scale_sensitivity=1,
    ),
    "Central Texas": ApprovalProfile(
        metro="Central Texas",
        social_base=56,
        political_base=57,
        summary=(
            "Central Texas secondary markets usually inherit Austin-style water sensitivity, but "
            "with more emphasis on community fit and growth transparency."
        ),
        headwinds=(
            "water-use scrutiny",
            "growth transparency",
            "neighborhood compatibility",
        ),
        community_water_pressure=3,
        political_water_pressure=3,
        environment_pressure=2,
        proximity_sensitivity=2,
        scale_sensitivity=2,
    ),
    "West Texas": ApprovalProfile(
        metro="West Texas",
        social_base=60,
        political_base=61,
        summary=(
            "West Texas markets are generally more industry-friendly, but water sourcing and long "
            "lead-time infrastructure commitments still affect comfort."
        ),
        headwinds=(
            "water sourcing transparency",
            "long-lead utility commitments",
            "infrastructure fragility",
        ),
        community_water_pressure=2,
        political_water_pressure=2,
        environment_pressure=1,
        proximity_sensitivity=1,
        scale_sensitivity=1,
    ),
}


_CITY_ANCHORS: tuple[CityAnchor, ...] = (
    CityAnchor(
        city="Dallas",
        metro="Dallas-Fort Worth",
        county="Dallas",
        region="North Texas",
        corridor_name="Dallas-Fort Worth Compute Crescent",
        lat=32.7767,
        lon=-96.7970,
        market_weight=1.00,
    ),
    CityAnchor(
        city="Fort Worth",
        metro="Dallas-Fort Worth",
        county="Tarrant",
        region="North Texas",
        corridor_name="Dallas-Fort Worth Compute Crescent",
        lat=32.7555,
        lon=-97.3308,
        market_weight=0.97,
    ),
    CityAnchor(
        city="Houston",
        metro="Houston",
        county="Harris",
        region="Gulf Coast",
        corridor_name="Houston Capacity Ring",
        lat=29.7604,
        lon=-95.3698,
        market_weight=1.00,
    ),
    CityAnchor(
        city="Austin",
        metro="Austin",
        county="Travis",
        region="Central Texas",
        corridor_name="Austin Research Power Belt",
        lat=30.2672,
        lon=-97.7431,
        market_weight=0.96,
    ),
    CityAnchor(
        city="San Antonio",
        metro="San Antonio",
        county="Bexar",
        region="South Central Texas",
        corridor_name="San Antonio Innovation Loop",
        lat=29.4241,
        lon=-98.4936,
        market_weight=0.95,
    ),
    CityAnchor(
        city="College Station",
        metro="Brazos Valley",
        county="Brazos",
        region="Central Texas",
        corridor_name="Brazos Valley Research Triangle",
        lat=30.6279,
        lon=-96.3344,
        market_weight=0.78,
    ),
    CityAnchor(
        city="El Paso",
        metro="El Paso",
        county="El Paso",
        region="West Texas",
        corridor_name="El Paso Border Compute Belt",
        lat=31.7619,
        lon=-106.4850,
        market_weight=0.82,
    ),
    CityAnchor(
        city="Corpus Christi",
        metro="Corpus Christi",
        county="Nueces",
        region="Gulf Coast",
        corridor_name="Corpus Christi Coastal Industrial Ring",
        lat=27.8006,
        lon=-97.3964,
        market_weight=0.70,
    ),
    CityAnchor(
        city="McAllen",
        metro="Rio Grande Valley",
        county="Hidalgo",
        region="South Texas",
        corridor_name="Rio Grande Valley Cross-Border Belt",
        lat=26.2034,
        lon=-98.2300,
        market_weight=0.71,
    ),
    CityAnchor(
        city="Brownsville",
        metro="Rio Grande Valley",
        county="Cameron",
        region="South Texas",
        corridor_name="Rio Grande Valley Cross-Border Belt",
        lat=25.9017,
        lon=-97.4975,
        market_weight=0.68,
    ),
    CityAnchor(
        city="Laredo",
        metro="Laredo",
        county="Webb",
        region="South Texas",
        corridor_name="Laredo Trade Infrastructure Ring",
        lat=27.5306,
        lon=-99.4803,
        market_weight=0.69,
    ),
    CityAnchor(
        city="Midland",
        metro="Midland",
        county="Midland",
        region="West Texas",
        corridor_name="Midland Energy Transition Corridor",
        lat=31.9974,
        lon=-102.0779,
        market_weight=0.67,
    ),
)

_UNIVERSITY_ANCHORS: tuple[UniversityAnchor, ...] = (
    UniversityAnchor(
        name="University of Texas at Dallas",
        metro="Dallas-Fort Worth",
        lat=32.9858,
        lon=-96.7501,
        talent_weight=1.00,
    ),
    UniversityAnchor(
        name="University of Texas at Arlington",
        metro="Dallas-Fort Worth",
        lat=32.7298,
        lon=-97.1153,
        talent_weight=0.92,
    ),
    UniversityAnchor(
        name="University of North Texas",
        metro="Dallas-Fort Worth",
        lat=33.2107,
        lon=-97.1467,
        talent_weight=0.92,
    ),
    UniversityAnchor(
        name="Southern Methodist University",
        metro="Dallas-Fort Worth",
        lat=32.8411,
        lon=-96.7845,
        talent_weight=0.90,
    ),
    UniversityAnchor(
        name="University of Texas at Austin",
        metro="Austin",
        lat=30.2849,
        lon=-97.7341,
        talent_weight=1.00,
    ),
    UniversityAnchor(
        name="Texas A&M University",
        metro="Brazos Valley",
        lat=30.6187,
        lon=-96.3365,
        talent_weight=1.00,
    ),
    UniversityAnchor(
        name="Rice University",
        metro="Houston",
        lat=29.7174,
        lon=-95.4018,
        talent_weight=0.98,
    ),
    UniversityAnchor(
        name="University of Houston",
        metro="Houston",
        lat=29.7199,
        lon=-95.3422,
        talent_weight=0.95,
    ),
    UniversityAnchor(
        name="University of Texas at San Antonio",
        metro="San Antonio",
        lat=29.5845,
        lon=-98.6215,
        talent_weight=0.92,
    ),
    UniversityAnchor(
        name="Baylor University",
        metro="Central Texas",
        lat=31.5493,
        lon=-97.1147,
        talent_weight=0.78,
    ),
    UniversityAnchor(
        name="University of Texas at El Paso",
        metro="El Paso",
        lat=31.7717,
        lon=-106.5048,
        talent_weight=0.88,
    ),
    UniversityAnchor(
        name="University of Texas Rio Grande Valley",
        metro="Rio Grande Valley",
        lat=26.3073,
        lon=-98.1740,
        talent_weight=0.84,
    ),
    UniversityAnchor(
        name="Texas Tech University",
        metro="West Texas",
        lat=33.5843,
        lon=-101.8783,
        talent_weight=0.82,
    ),
)


class PointGridIndex:
    def __init__(self, points: list[PointAsset], *, cell_size_degrees: float = 0.35) -> None:
        self._cell_size = cell_size_degrees
        self._cells: dict[tuple[int, int], list[PointAsset]] = {}
        for point in points:
            self._cells.setdefault(self._key(point.lat, point.lon), []).append(point)

    def nearest(
        self,
        lat: float,
        lon: float,
        *,
        max_distance_miles: float,
    ) -> tuple[PointAsset | None, float | None]:
        origin = self._key(lat, lon)
        max_ring = max(1, ceil(max_distance_miles / (69.0 * self._cell_size)))
        best_point: PointAsset | None = None
        best_distance = max_distance_miles

        for ring in range(max_ring + 1):
            for cell in self._ring(origin, ring):
                for point in self._cells.get(cell, ()):
                    distance = _haversine_miles(lat, lon, point.lat, point.lon)
                    if distance <= best_distance:
                        best_point = point
                        best_distance = distance
            if best_point is not None and (ring * self._cell_size * 69.0) > best_distance:
                break

        if best_point is None:
            return None, None
        return best_point, round(best_distance, 2)

    def _key(self, lat: float, lon: float) -> tuple[int, int]:
        return (floor(lat / self._cell_size), floor(lon / self._cell_size))

    @staticmethod
    def _ring(origin: tuple[int, int], ring: int) -> list[tuple[int, int]]:
        x0, y0 = origin
        if ring == 0:
            return [(x0, y0)]
        cells: list[tuple[int, int]] = []
        for dx in range(-ring, ring + 1):
            cells.append((x0 + dx, y0 - ring))
            cells.append((x0 + dx, y0 + ring))
        for dy in range(-ring + 1, ring):
            cells.append((x0 - ring, y0 + dy))
            cells.append((x0 + ring, y0 + dy))
        return cells


def load_live_candidate_opportunities(
    limit: int = _DEFAULT_SHORTLIST_LIMIT,
) -> list[dict[str, Any]]:
    with SessionLocal() as session:
        return build_live_candidate_opportunities(session, limit=limit)


def build_live_candidate_opportunities(
    session: Session,
    limit: int = _DEFAULT_SHORTLIST_LIMIT,
) -> list[dict[str, Any]]:
    return build_ranked_live_candidate_pool(
        session,
        limit=limit,
        per_metro_cap=_MAX_OPPORTUNITIES_PER_METRO,
        major_metro_minimums=_PRIMARY_FEATURED_METRO_MINIMUMS,
        metro_caps=_PRIMARY_FEATURED_METRO_CAPS,
    )


def build_ranked_live_candidate_records(
    session: Session,
    *,
    limit: int | None = None,
    per_metro_cap: int | None = None,
    major_metro_minimums: dict[str, int] | None = None,
    metro_caps: dict[str, int] | None = None,
) -> list[ScoredCandidate]:
    candidates = _load_listing_candidates(session)
    if not candidates:
        return []

    substations = PointGridIndex(
        _load_point_assets(
            session,
            source_id="IF-001",
            label_attribute="facility_name",
        ),
        cell_size_degrees=0.5,
    )
    peering = PointGridIndex(
        _load_point_assets(
            session,
            source_id="IF-009",
            label_attribute="facility_name",
        ),
        cell_size_degrees=0.5,
    )
    power_plants = PointGridIndex(
        _load_point_assets(
            session,
            source_id="IF-025",
            label_attribute="plant_name",
        ),
        cell_size_degrees=0.5,
    )
    highways = PointGridIndex(
        _load_point_assets(
            session,
            source_id="IF-026",
            label_attribute="ref",
            fallback_label_attribute="name",
        ),
        cell_size_degrees=0.25,
    )
    water_sites = PointGridIndex(
        _load_point_assets(
            session,
            source_id="IF-021",
            label_attribute="site_name",
        ),
        cell_size_degrees=0.4,
    )
    hazard_lookup = _load_anchor_hazard_scores(session)
    superfund_cities = _load_superfund_city_set(session)

    anchored_candidates: list[
        tuple[ListingCandidate, CityAnchor, float, UniversityAnchor, float]
    ] = []
    for candidate in candidates:
        city_anchor, city_distance = _nearest_city_anchor(candidate.latitude, candidate.longitude)
        university_anchor, university_distance = _nearest_university_anchor(
            candidate.latitude,
            candidate.longitude,
        )
        if (
            _population_access_distance(city_distance, university_distance)
            > _MAX_POPULATION_ACCESS_DISTANCE_MILES
        ):
            continue
        if city_anchor.metro not in _SHORTLIST_METRO_ALLOWLIST:
            continue
        anchored_candidates.append(
            (candidate, city_anchor, city_distance, university_anchor, university_distance)
        )

    if not anchored_candidates:
        return []

    price_per_acre_by_metro: dict[str, list[float]] = {}
    for candidate, city_anchor, *_ in anchored_candidates:
        price_per_acre = _price_per_acre(candidate)
        if price_per_acre is not None:
            price_per_acre_by_metro.setdefault(city_anchor.metro, []).append(price_per_acre)
    metro_price_medians = {
        metro: median(values)
        for metro, values in price_per_acre_by_metro.items()
        if values
    }

    scored: list[ScoredCandidate] = []
    for (
        candidate,
        city_anchor,
        city_distance,
        university_anchor,
        university_distance,
    ) in anchored_candidates:
        if _asset_suitability(candidate) < 0.6:
            continue

        substation, substation_distance = substations.nearest(
            candidate.latitude,
            candidate.longitude,
            max_distance_miles=_MAX_SUBSTATION_DISTANCE_MILES,
        )
        peering_facility, peering_distance = peering.nearest(
            candidate.latitude,
            candidate.longitude,
            max_distance_miles=_MAX_PEERING_DISTANCE_MILES,
        )
        power_plant, power_plant_distance = power_plants.nearest(
            candidate.latitude,
            candidate.longitude,
            max_distance_miles=_MAX_POWER_PLANT_DISTANCE_MILES,
        )
        highway, highway_distance = highways.nearest(
            candidate.latitude,
            candidate.longitude,
            max_distance_miles=_MAX_HIGHWAY_DISTANCE_MILES,
        )
        water_site, water_distance = water_sites.nearest(
            candidate.latitude,
            candidate.longitude,
            max_distance_miles=_MAX_WATER_DISTANCE_MILES,
        )

        factor_scores = _score_factors(
            candidate=candidate,
            city_anchor=city_anchor,
            city_distance=city_distance,
            university_anchor=university_anchor,
            university_distance=university_distance,
            substation=substation,
            substation_distance=substation_distance,
            peering_facility=peering_facility,
            peering_distance=peering_distance,
            power_plant=power_plant,
            power_plant_distance=power_plant_distance,
            highway=highway,
            highway_distance=highway_distance,
            water_site=water_site,
            water_distance=water_distance,
            hazard_lookup=hazard_lookup,
            superfund_cities=superfund_cities,
            metro_price_medians=metro_price_medians,
        )
        bonus_points = _score_bonuses(
            candidate=candidate,
            city_distance=city_distance,
            university_distance=university_distance,
            substation_distance=substation_distance,
            peering_distance=peering_distance,
            metro_price_medians=metro_price_medians,
            metro_name=city_anchor.metro,
            market_weight=city_anchor.market_weight,
        )
        viability_score = min(round(sum(factor_scores.values()) + bonus_points), 100)
        confidence_score = _confidence_score()
        scored.append(
            ScoredCandidate(
                opportunity=_build_opportunity_record(
                    candidate=candidate,
                    city_anchor=city_anchor,
                    city_distance=city_distance,
                    university_anchor=university_anchor,
                    university_distance=university_distance,
                    substation=substation,
                    substation_distance=substation_distance,
                    peering_facility=peering_facility,
                    peering_distance=peering_distance,
                    power_plant=power_plant,
                    power_plant_distance=power_plant_distance,
                    highway=highway,
                    highway_distance=highway_distance,
                    water_site=water_site,
                    water_distance=water_distance,
                    factor_scores=factor_scores,
                    viability_score=viability_score,
                    confidence_score=confidence_score,
                ),
                confidence_score=confidence_score,
                viability_score=viability_score,
                market_weight=city_anchor.market_weight,
                factor_scores=dict(factor_scores),
                bonus_points=bonus_points,
                candidate=candidate,
                city_anchor=city_anchor,
                city_distance=city_distance,
                university_anchor=university_anchor,
                university_distance=university_distance,
                substation_distance=substation_distance,
                peering_distance=peering_distance,
            )
        )

    ranked = sorted(
        scored,
        key=lambda item: (
            -item.viability_score,
            -item.confidence_score,
            -item.market_weight,
            -(item.opportunity.get("power_score") or 0),
            item.opportunity["site_name"],
        ),
    )

    return _select_balanced_candidates(
        ranked,
        limit=limit,
        per_metro_cap=per_metro_cap,
        major_metro_minimums=major_metro_minimums,
        metro_caps=metro_caps,
    )


def build_ranked_live_candidate_pool(
    session: Session,
    *,
    limit: int | None = None,
    per_metro_cap: int | None = None,
    major_metro_minimums: dict[str, int] | None = None,
    metro_caps: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    selected = build_ranked_live_candidate_records(
        session,
        limit=limit,
        per_metro_cap=per_metro_cap,
        major_metro_minimums=major_metro_minimums,
        metro_caps=metro_caps,
    )
    opportunities = [dict(item.opportunity) for item in selected]
    for rank, record in enumerate(opportunities, start=1):
        record["rank"] = rank
        record["site_id"] = (
            f"tx-live-{rank:02d}-{record['listing_source_id']}-{record['source_listing_key']}"
        )
    return opportunities


def _select_balanced_candidates(
    ranked: list[ScoredCandidate],
    *,
    limit: int | None,
    per_metro_cap: int | None,
    major_metro_minimums: dict[str, int] | None,
    metro_caps: dict[str, int] | None,
) -> list[ScoredCandidate]:
    if not ranked:
        return []

    effective_limit = limit if limit is not None else len(ranked)
    selected_indices: list[int] = []
    selected_lookup: set[int] = set()
    metro_counts: dict[str, int] = {}

    if major_metro_minimums:
        for metro_name, minimum in major_metro_minimums.items():
            if len(selected_indices) >= effective_limit:
                break
            for index, item in enumerate(ranked):
                if index in selected_lookup or item.opportunity["metro"] != metro_name:
                    continue
                metro_cap = _metro_cap_for(
                    metro_name,
                    default_cap=per_metro_cap,
                    metro_caps=metro_caps,
                )
                if metro_cap is not None and metro_counts.get(metro_name, 0) >= metro_cap:
                    break
                selected_indices.append(index)
                selected_lookup.add(index)
                metro_counts[metro_name] = metro_counts.get(metro_name, 0) + 1
                if metro_counts[metro_name] >= minimum or len(selected_indices) >= effective_limit:
                    break

    for index, item in enumerate(ranked):
        if index in selected_lookup:
            continue
        metro_name = item.opportunity["metro"]
        metro_cap = _metro_cap_for(
            metro_name,
            default_cap=per_metro_cap,
            metro_caps=metro_caps,
        )
        if metro_cap is not None and metro_counts.get(metro_name, 0) >= metro_cap:
            continue
        selected_indices.append(index)
        selected_lookup.add(index)
        metro_counts[metro_name] = metro_counts.get(metro_name, 0) + 1
        if len(selected_indices) >= effective_limit:
            break

    return [ranked[index] for index in sorted(selected_indices)]


def _select_balanced_opportunities(
    ranked: list[ScoredCandidate],
    *,
    limit: int | None,
    per_metro_cap: int | None,
    major_metro_minimums: dict[str, int] | None,
    metro_caps: dict[str, int] | None,
) -> list[dict[str, Any]]:
    return [
        dict(item.opportunity)
        for item in _select_balanced_candidates(
            ranked,
            limit=limit,
            per_metro_cap=per_metro_cap,
            major_metro_minimums=major_metro_minimums,
            metro_caps=metro_caps,
        )
    ]


def _metro_cap_for(
    metro_name: str,
    *,
    default_cap: int | None,
    metro_caps: dict[str, int] | None,
) -> int | None:
    if metro_caps and metro_name in metro_caps:
        return metro_caps[metro_name]
    return default_cap


def _load_listing_candidates(session: Session) -> list[ListingCandidate]:
    rows = session.scalars(
        select(MarketListing).where(
            MarketListing.is_active.is_(True),
            MarketListing.latitude.is_not(None),
            MarketListing.longitude.is_not(None),
            MarketListing.acreage.is_not(None),
            MarketListing.acreage >= _SWEET_SPOT_ACREAGE_MIN,
            MarketListing.acreage <= _SWEET_SPOT_ACREAGE_MAX,
        )
    ).all()
    candidates: list[ListingCandidate] = []
    for row in rows:
        if (row.state_code or "TX").upper() != "TX":
            continue
        acreage = float(row.acreage)
        candidates.append(
            ListingCandidate(
                market_listing_id=str(UUID(str(row.market_listing_id))),
                listing_source_id=row.listing_source_id,
                source_listing_key=row.source_listing_key,
                listing_title=row.listing_title,
                listing_status=row.listing_status,
                asset_type=row.asset_type,
                asking_price=float(row.asking_price) if row.asking_price is not None else None,
                acreage=acreage,
                building_sqft=float(row.building_sqft) if row.building_sqft is not None else None,
                city=row.city,
                state_code=row.state_code,
                latitude=float(row.latitude),
                longitude=float(row.longitude),
                source_url=row.source_url,
                broker_name=row.broker_name,
            )
        )
    return candidates


def _load_point_assets(
    session: Session,
    *,
    source_id: str,
    label_attribute: str,
    fallback_label_attribute: str | None = None,
) -> list[PointAsset]:
    rows = session.execute(
        select(
            SourceEvidence.record_key,
            SourceEvidence.attribute_name,
            SourceEvidence.attribute_value,
        ).where(
            SourceEvidence.source_id == source_id,
            SourceEvidence.is_active.is_(True),
        )
    ).all()

    grouped: dict[str, dict[str, str]] = {}
    for record_key, attribute_name, attribute_value in rows:
        grouped.setdefault(record_key, {})[attribute_name.lower()] = attribute_value

    assets: list[PointAsset] = []
    for record_key, attrs in grouped.items():
        lat = _safe_float(attrs.get("latitude"))
        lon = _safe_float(attrs.get("longitude"))
        if lat is None or lon is None:
            continue
        label = (
            attrs.get(label_attribute.lower())
            or (attrs.get(fallback_label_attribute.lower()) if fallback_label_attribute else None)
            or record_key
        )
        assets.append(
            PointAsset(
                record_key=record_key,
                lat=lat,
                lon=lon,
                label=label,
                attrs=attrs,
            )
        )
    return assets


def _load_anchor_hazard_scores(session: Session) -> dict[str, float]:
    rows = session.execute(
        select(
            SourceEvidence.record_key,
            SourceEvidence.attribute_name,
            SourceEvidence.attribute_value,
        ).where(
            SourceEvidence.source_id == "IF-007",
            SourceEvidence.is_active.is_(True),
        )
    ).all()
    grouped: dict[str, dict[str, str]] = {}
    for record_key, attribute_name, attribute_value in rows:
        grouped.setdefault(record_key, {})[attribute_name.lower()] = attribute_value

    county_scores: dict[str, float] = {}
    for attrs in grouped.values():
        county_name = (attrs.get("county") or "").strip().lower()
        if not county_name:
            continue
        loss_percentile = _safe_float(attrs.get("expected_annual_loss_percentile"))
        resilience_percentile = _safe_float(attrs.get("community_resilience_percentile"))
        if loss_percentile is None and resilience_percentile is None:
            continue
        inverse_loss = 1.0 - ((loss_percentile or 50.0) / 100.0)
        resilience = (resilience_percentile or 50.0) / 100.0
        county_scores[county_name] = _clamp((inverse_loss * 0.75) + (resilience * 0.25))
    return county_scores


def _load_superfund_city_set(session: Session) -> set[str]:
    rows = session.execute(
        select(SourceEvidence.attribute_value).where(
            SourceEvidence.source_id == "IF-023",
            SourceEvidence.attribute_name == "city",
            SourceEvidence.is_active.is_(True),
        )
    ).all()
    return {
        str(value).strip().lower()
        for (value,) in rows
        if value
    }


def _nearest_city_anchor(lat: float, lon: float) -> tuple[CityAnchor, float]:
    best_anchor = _CITY_ANCHORS[0]
    best_distance = _haversine_miles(lat, lon, best_anchor.lat, best_anchor.lon)
    for anchor in _CITY_ANCHORS[1:]:
        distance = _haversine_miles(lat, lon, anchor.lat, anchor.lon)
        if distance < best_distance:
            best_anchor = anchor
            best_distance = distance
    return best_anchor, round(best_distance, 2)


def _nearest_university_anchor(lat: float, lon: float) -> tuple[UniversityAnchor, float]:
    best_anchor = _UNIVERSITY_ANCHORS[0]
    best_distance = _haversine_miles(lat, lon, best_anchor.lat, best_anchor.lon)
    for anchor in _UNIVERSITY_ANCHORS[1:]:
        distance = _haversine_miles(lat, lon, anchor.lat, anchor.lon)
        if distance < best_distance:
            best_anchor = anchor
            best_distance = distance
    return best_anchor, round(best_distance, 2)


def _population_access_distance(city_distance: float, university_distance: float) -> float:
    return min(city_distance, university_distance)


def _score_factors(
    *,
    candidate: ListingCandidate,
    city_anchor: CityAnchor,
    city_distance: float,
    university_anchor: UniversityAnchor,
    university_distance: float,
    substation: PointAsset | None,
    substation_distance: float | None,
    peering_facility: PointAsset | None,
    peering_distance: float | None,
    power_plant: PointAsset | None,
    power_plant_distance: float | None,
    highway: PointAsset | None,
    highway_distance: float | None,
    water_site: PointAsset | None,
    water_distance: float | None,
    hazard_lookup: dict[str, float],
    superfund_cities: set[str],
    metro_price_medians: dict[str, float],
) -> dict[str, float]:
    power_score = 20.0 * _power_normalized(
        substation=substation,
        substation_distance=substation_distance,
        power_plant=power_plant,
        power_plant_distance=power_plant_distance,
    )
    fiber_score = 12.0 * _fiber_normalized(
        peering_facility=peering_facility,
        peering_distance=peering_distance,
    )
    hazard_score = 12.0 * _hazard_normalized(city_anchor, hazard_lookup)
    scale_score = 10.0 * _scale_normalized(candidate)
    land_use_score = 10.0 * _asset_suitability(candidate)
    water_score = 8.0 * _water_normalized(
        water_site=water_site,
        water_distance=water_distance,
        city_distance=city_distance,
    )
    environment_score = 8.0 * _environment_normalized(candidate, superfund_cities)
    talent_score = 8.0 * _talent_normalized(
        city_anchor=city_anchor,
        city_distance=city_distance,
        university_anchor=university_anchor,
        university_distance=university_distance,
    )
    logistics_score = 6.0 * _logistics_normalized(
        highway=highway,
        highway_distance=highway_distance,
    )
    market_score = 6.0 * _market_economics_normalized(
        candidate=candidate,
        metro_name=city_anchor.metro,
        market_weight=city_anchor.market_weight,
        metro_price_medians=metro_price_medians,
    )
    return {
        "power": power_score,
        "fiber": fiber_score,
        "hazard": hazard_score,
        "scale": scale_score,
        "land_use": land_use_score,
        "water": water_score,
        "environment": environment_score,
        "talent": talent_score,
        "logistics": logistics_score,
        "market": market_score,
    }


def _score_bonuses(
    *,
    candidate: ListingCandidate,
    city_distance: float,
    university_distance: float,
    substation_distance: float | None,
    peering_distance: float | None,
    metro_price_medians: dict[str, float],
    metro_name: str,
    market_weight: float,
) -> float:
    points = 0.0
    acreage = candidate.acreage or 0.0
    building_sqft = candidate.building_sqft or 0.0
    points += 1.0
    if (
        _SWEET_SPOT_ACREAGE_MIN <= acreage <= _SWEET_SPOT_ACREAGE_MAX
        or 15_000 <= building_sqft <= 75_000
    ):
        points += 1.0
    if (substation_distance or 999.0) <= 10.0 and (peering_distance or 999.0) <= 20.0:
        points += 1.0
    if _population_access_distance(city_distance, university_distance) <= 10.0:
        points += 1.0
    if _market_economics_normalized(
        candidate=candidate,
        metro_name=metro_name,
        metro_price_medians=metro_price_medians,
        market_weight=market_weight,
    ) >= 0.7:
        points += 1.0
    return points


def build_social_political_overlay(
    *,
    metro_name: str,
    region: str,
    city_distance: float,
    university_distance: float,
    acreage: float | None,
    building_sqft: float | None,
    water_score: int,
    environment_score: int,
    hazard_score: int,
) -> dict[str, Any]:
    profile = _approval_profile_for_market(metro_name=metro_name, region=region)
    population_distance = _population_access_distance(city_distance, university_distance)

    social_score = float(profile.social_base)
    political_score = float(profile.political_base)
    headwinds = list(profile.headwinds)

    scale_social_delta, scale_political_delta = _scale_approval_adjustment(
        acreage=acreage,
        building_sqft=building_sqft,
        scale_sensitivity=profile.scale_sensitivity,
    )
    social_score += scale_social_delta
    political_score += scale_political_delta
    if scale_political_delta <= -5 and "ERCOT large-load scrutiny" not in headwinds:
        headwinds.append("ERCOT large-load scrutiny")

    proximity_social_delta, proximity_political_delta = _proximity_approval_adjustment(
        population_distance=population_distance,
        proximity_sensitivity=profile.proximity_sensitivity,
    )
    social_score += proximity_social_delta
    political_score += proximity_political_delta
    if proximity_social_delta <= -4 and "sensitive-use adjacency" not in headwinds:
        headwinds.append("sensitive-use adjacency")

    water_social_delta, water_political_delta = _water_approval_adjustment(
        water_score=water_score,
        community_pressure=profile.community_water_pressure,
        political_pressure=profile.political_water_pressure,
    )
    social_score += water_social_delta
    political_score += water_political_delta

    environment_social_delta, environment_political_delta = _environment_approval_adjustment(
        environment_score=environment_score,
        hazard_score=hazard_score,
        environment_pressure=profile.environment_pressure,
    )
    social_score += environment_social_delta
    political_score += environment_political_delta

    social_score = round(_clamp(social_score, minimum=30.0, maximum=92.0))
    political_score = round(_clamp(political_score, minimum=28.0, maximum=92.0))
    approval_score = round((social_score * 0.48) + (political_score * 0.52))
    social_category = _social_category(social_score)
    political_category = _political_category(political_score)
    approval_stage = _approval_stage(
        approval_score=approval_score,
        social_score=social_score,
        political_score=political_score,
    )
    summary = (
        f"{profile.summary} Approval path currently reads {approval_stage.lower()}, "
        f"with headwinds around {headwinds[0]} and {headwinds[1]}."
    )

    return {
        "social_score": social_score,
        "political_score": political_score,
        "approval_score": approval_score,
        "social_category": social_category,
        "political_category": political_category,
        "approval_stage": approval_stage,
        "approval_headwinds": headwinds[:4],
        "approval_summary": summary,
    }


def _build_opportunity_record(
    *,
    candidate: ListingCandidate,
    city_anchor: CityAnchor,
    city_distance: float,
    university_anchor: UniversityAnchor,
    university_distance: float,
    substation: PointAsset | None,
    substation_distance: float | None,
    peering_facility: PointAsset | None,
    peering_distance: float | None,
    power_plant: PointAsset | None,
    power_plant_distance: float | None,
    highway: PointAsset | None,
    highway_distance: float | None,
    water_site: PointAsset | None,
    water_distance: float | None,
    factor_scores: dict[str, float],
    viability_score: int,
    confidence_score: int,
) -> dict[str, Any]:
    power_score = round((factor_scores["power"] / 20.0) * 100)
    fiber_score = round((factor_scores["fiber"] / 12.0) * 100)
    water_score = round((factor_scores["water"] / 8.0) * 100)
    environment_score = round((factor_scores["environment"] / 8.0) * 100)
    hazard_score = round((factor_scores["hazard"] / 12.0) * 100)
    talent_score = round((factor_scores["talent"] / 8.0) * 100)
    readiness_stage = _readiness_stage(viability_score, confidence_score)
    strengths = _strengths(
        power_score=power_score,
        fiber_score=fiber_score,
        water_score=water_score,
        talent_score=talent_score,
        candidate=candidate,
    )
    approval_overlay = build_social_political_overlay(
        metro_name=city_anchor.metro,
        region=city_anchor.region,
        city_distance=city_distance,
        university_distance=university_distance,
        acreage=candidate.acreage,
        building_sqft=candidate.building_sqft,
        water_score=water_score,
        environment_score=environment_score,
        hazard_score=hazard_score,
    )

    return {
        "site_name": candidate.listing_title,
        "corridor_name": city_anchor.corridor_name,
        "metro": city_anchor.metro,
        "city": candidate.city or city_anchor.city,
        "county": f"{city_anchor.county} County",
        "region": city_anchor.region,
        "university_anchor": university_anchor.name,
        "acreage": round(candidate.acreage, 4) if candidate.acreage is not None else None,
        "acreage_band": _acreage_band(candidate),
        "distance_to_city_miles": round(city_distance),
        "distance_to_university_miles": round(
            min(university_distance, _MAX_UNIVERSITY_DISTANCE_MILES)
        ),
        "viability_score": viability_score,
        "power_score": power_score,
        "fiber_score": fiber_score,
        "water_score": water_score,
        "talent_score": talent_score,
        "readiness_stage": readiness_stage,
        "score_band": _score_band(viability_score),
        "strengths": strengths,
        "summary": (
            f"Live-scored {candidate.listing_status or 'marketed'} candidate near "
            f"{city_anchor.city} with strongest signals in {strengths[0]}, {strengths[1]}, "
            f"and {strengths[2]}."
        ),
        "lat": candidate.latitude,
        "lon": candidate.longitude,
        "confidence_score": confidence_score,
        "listing_source_id": candidate.listing_source_id,
        "listing_status": candidate.listing_status,
        "asking_price": candidate.asking_price,
        "broker_name": candidate.broker_name,
        "price_per_acre": _price_per_acre(candidate),
        "source_url": candidate.source_url,
        "source_listing_key": candidate.source_listing_key,
        "market_listing_id": candidate.market_listing_id,
        "nearest_substation_name": substation.label if substation is not None else None,
        "nearest_substation_distance_miles": _round_optional(substation_distance),
        "nearest_substation_voltage_kv": (
            _safe_float(substation.attrs.get("max_voltage_kv"))
            if substation is not None
            else None
        ),
        "nearest_peering_facility_name": (
            peering_facility.label if peering_facility is not None else None
        ),
        "nearest_peering_distance_miles": _round_optional(peering_distance),
        "nearest_peering_carrier_count": (
            _safe_int(peering_facility.attrs.get("carrier_count"))
            if peering_facility is not None
            else None
        ),
        "nearest_power_plant_name": power_plant.label if power_plant is not None else None,
        "nearest_power_plant_distance_miles": _round_optional(power_plant_distance),
        "nearest_highway_name": highway.label if highway is not None else None,
        "nearest_highway_distance_miles": _round_optional(highway_distance),
        "nearest_water_name": water_site.label if water_site is not None else None,
        "nearest_water_distance_miles": _round_optional(water_distance),
        **approval_overlay,
    }


def _asset_suitability(candidate: ListingCandidate) -> float:
    asset_type = (candidate.asset_type or "").strip().lower()
    score = _ASSET_TYPE_BASE_SCORE.get(asset_type, 0.25)
    title = candidate.listing_title.lower()
    acreage = candidate.acreage or 0.0
    building_sqft = candidate.building_sqft or 0.0
    for token, bonus in _KEYWORD_BONUSES.items():
        if token in title:
            score += bonus
    for token, penalty in _KEYWORD_PENALTIES.items():
        if token in title:
            score -= penalty
    if _SWEET_SPOT_ACREAGE_MIN <= acreage <= _SWEET_SPOT_ACREAGE_MAX:
        score += 0.05
    elif acreage > _SWEET_SPOT_ACREAGE_MAX:
        score -= 0.05
    if 15_000 <= building_sqft <= 75_000:
        score += 0.04
    return _clamp(score)


def _power_normalized(
    *,
    substation: PointAsset | None,
    substation_distance: float | None,
    power_plant: PointAsset | None,
    power_plant_distance: float | None,
) -> float:
    if substation is None and power_plant is None:
        return 0.45

    substation_distance_score = _distance_score(
        substation_distance,
        ideal=2.0,
        acceptable=15.0,
        max_distance=_MAX_SUBSTATION_DISTANCE_MILES,
    )
    voltage = _safe_float(substation.attrs.get("max_voltage_kv")) if substation else None
    voltage_score = 0.35
    if voltage is not None:
        if voltage >= 345:
            voltage_score = 1.0
        elif voltage >= 138:
            voltage_score = 0.82
        elif voltage >= 69:
            voltage_score = 0.62

    plant_distance_score = _distance_score(
        power_plant_distance,
        ideal=5.0,
        acceptable=30.0,
        max_distance=_MAX_POWER_PLANT_DISTANCE_MILES,
    )
    plant_capacity = (
        _safe_float(power_plant.attrs.get("installed_capacity_mw"))
        if power_plant
        else None
    )
    plant_capacity_score = 0.40
    if plant_capacity is not None:
        if plant_capacity >= 1000:
            plant_capacity_score = 1.0
        elif plant_capacity >= 500:
            plant_capacity_score = 0.85
        elif plant_capacity >= 100:
            plant_capacity_score = 0.65
        else:
            plant_capacity_score = 0.45

    return _clamp(
        (substation_distance_score * 0.50)
        + (voltage_score * 0.30)
        + (plant_distance_score * 0.10)
        + (plant_capacity_score * 0.10)
    )


def _fiber_normalized(
    *,
    peering_facility: PointAsset | None,
    peering_distance: float | None,
) -> float:
    if peering_facility is None:
        return 0.35
    distance_score = _distance_score(
        peering_distance,
        ideal=3.0,
        acceptable=20.0,
        max_distance=_MAX_PEERING_DISTANCE_MILES,
    )
    carrier_count = _safe_float(peering_facility.attrs.get("carrier_count")) or 0.0
    ix_count = _safe_float(peering_facility.attrs.get("ix_count")) or 0.0
    net_count = _safe_float(peering_facility.attrs.get("net_count")) or 0.0
    density_score = _clamp(
        (carrier_count / 10.0) * 0.35
        + (ix_count / 3.0) * 0.30
        + (net_count / 50.0) * 0.35
    )
    return _clamp((distance_score * 0.7) + (density_score * 0.3))


def _hazard_normalized(city_anchor: CityAnchor, hazard_lookup: dict[str, float]) -> float:
    return hazard_lookup.get(city_anchor.county.lower(), 0.62)


def _scale_normalized(candidate: ListingCandidate) -> float:
    acreage_score = _acreage_fit_score(candidate.acreage)
    building_score = _building_fit_score(candidate.building_sqft)
    return max(acreage_score, building_score * 0.95)


def _water_normalized(
    *,
    water_site: PointAsset | None,
    water_distance: float | None,
    city_distance: float,
) -> float:
    gauge_score = _distance_score(
        water_distance,
        ideal=3.0,
        acceptable=20.0,
        max_distance=_MAX_WATER_DISTANCE_MILES,
    ) if water_site is not None else 0.40
    municipal_proxy = _distance_score(city_distance, ideal=0.0, acceptable=20.0, max_distance=50.0)
    return _clamp((gauge_score * 0.55) + (municipal_proxy * 0.45))


def _environment_normalized(candidate: ListingCandidate, superfund_cities: set[str]) -> float:
    city_name = (candidate.city or "").strip().lower()
    if city_name and city_name in superfund_cities:
        return 0.42
    return 0.82


def _talent_normalized(
    *,
    city_anchor: CityAnchor,
    city_distance: float,
    university_anchor: UniversityAnchor,
    university_distance: float,
) -> float:
    population_access_score = _distance_score(
        _population_access_distance(city_distance, university_distance),
        ideal=0.0,
        acceptable=10.0,
        max_distance=_MAX_POPULATION_ACCESS_DISTANCE_MILES,
    )
    metro_weight = city_anchor.market_weight
    return _clamp((population_access_score * 0.72) + (metro_weight * 0.28))


def _logistics_normalized(
    *,
    highway: PointAsset | None,
    highway_distance: float | None,
) -> float:
    if highway is None:
        return 0.45
    highway_type = (highway.attrs.get("highway_type") or "").lower()
    type_score = 0.95 if highway_type == "motorway" else 0.75
    distance_score = _distance_score(
        highway_distance,
        ideal=0.5,
        acceptable=8.0,
        max_distance=_MAX_HIGHWAY_DISTANCE_MILES,
    )
    return _clamp((distance_score * 0.75) + (type_score * 0.25))


def _market_economics_normalized(
    *,
    candidate: ListingCandidate,
    metro_name: str,
    market_weight: float,
    metro_price_medians: dict[str, float],
) -> float:
    price_per_acre = _price_per_acre(candidate)
    metro_median = metro_price_medians.get(metro_name)
    if price_per_acre is None or metro_median is None or metro_median <= 0:
        return _clamp((market_weight * 0.6) + 0.22)
    ratio = price_per_acre / metro_median
    if ratio <= 0.65:
        price_score = 0.95
    elif ratio <= 0.85:
        price_score = 0.85
    elif ratio <= 1.0:
        price_score = 0.72
    elif ratio <= 1.2:
        price_score = 0.58
    else:
        price_score = 0.42
    return _clamp((price_score * 0.55) + (market_weight * 0.45))


def _price_per_acre(candidate: ListingCandidate) -> float | None:
    if candidate.asking_price is None or candidate.acreage is None or candidate.acreage <= 0:
        return None
    return round(candidate.asking_price / candidate.acreage, 2)


def _approval_profile_for_market(*, metro_name: str, region: str) -> ApprovalProfile:
    return (
        _APPROVAL_PROFILES.get(metro_name)
        or _APPROVAL_PROFILES.get(region)
        or _DEFAULT_APPROVAL_PROFILE
    )


def _scale_approval_adjustment(
    *,
    acreage: float | None,
    building_sqft: float | None,
    scale_sensitivity: int,
) -> tuple[float, float]:
    social_delta = 0.0
    political_delta = 0.0

    if acreage is not None and acreage > 0:
        if _SWEET_SPOT_ACREAGE_MIN <= acreage <= _SWEET_SPOT_ACREAGE_MAX:
            social_delta += 6.0
            political_delta += 4.0
        elif acreage <= 3.0:
            social_delta -= 1.0 * scale_sensitivity
            political_delta -= 1.0 * scale_sensitivity
        elif acreage <= 10.0:
            social_delta -= 2.0 * scale_sensitivity
            political_delta -= 2.0 * scale_sensitivity
        elif acreage <= 25.0:
            social_delta -= 4.0 * scale_sensitivity
            political_delta -= 3.0 * scale_sensitivity
        else:
            social_delta -= 6.0 * scale_sensitivity
            political_delta -= 5.0 * scale_sensitivity

    if building_sqft is not None and building_sqft > 0:
        if 15_000 <= building_sqft <= 150_000:
            social_delta += 2.0
            political_delta += 2.0
        elif building_sqft > 300_000:
            social_delta -= 3.0 * scale_sensitivity
            political_delta -= 4.0 * scale_sensitivity

    return social_delta, political_delta


def _proximity_approval_adjustment(
    *,
    population_distance: float,
    proximity_sensitivity: int,
) -> tuple[float, float]:
    if population_distance <= 3.0:
        return (-4.0 * proximity_sensitivity, -3.0 * proximity_sensitivity)
    if population_distance <= 12.0:
        return (2.0, 2.0)
    if population_distance <= 30.0:
        return (4.0, 3.0)
    return (-3.0, -2.0)


def _water_approval_adjustment(
    *,
    water_score: int,
    community_pressure: int,
    political_pressure: int,
) -> tuple[float, float]:
    if water_score >= 85:
        return (2.0 * community_pressure, 2.0 * political_pressure)
    if water_score >= 72:
        return (1.0 * community_pressure, 1.0 * political_pressure)
    if water_score >= 60:
        return (-1.0 * community_pressure, -1.0 * political_pressure)
    return (-2.0 * community_pressure, -2.0 * political_pressure)


def _environment_approval_adjustment(
    *,
    environment_score: int,
    hazard_score: int,
    environment_pressure: int,
) -> tuple[float, float]:
    combined_resilience = round((environment_score * 0.55) + (hazard_score * 0.45))
    if combined_resilience >= 78:
        return (1.0 * environment_pressure, 1.0 * environment_pressure)
    if combined_resilience >= 62:
        return (0.0, 0.0)
    if combined_resilience >= 50:
        return (-1.0 * environment_pressure, -1.0 * environment_pressure)
    return (-2.0 * environment_pressure, -2.0 * environment_pressure)


def _social_category(score: int) -> str:
    if score >= 72:
        return "community-aligned"
    if score >= 60:
        return "community-manageable"
    if score >= 50:
        return "community-sensitive"
    return "community-fragile"


def _political_category(score: int) -> str:
    if score >= 72:
        return "permit-forward"
    if score >= 60:
        return "policy-manageable"
    if score >= 50:
        return "hearing-sensitive"
    return "politically fragile"


def _approval_stage(*, approval_score: int, social_score: int, political_score: int) -> str:
    if approval_score >= 72 and min(social_score, political_score) >= 64:
        return "Lower-friction approval path"
    if approval_score >= 60 and min(social_score, political_score) >= 56:
        return "Conditional approval path"
    if approval_score >= 50:
        return "High-touch entitlement path"
    return "Politically fragile path"


def _readiness_stage(viability_score: int, confidence_score: int) -> str:
    if viability_score >= 74 and confidence_score >= 75:
        return "Priority now"
    if viability_score >= 68:
        return "Near-term build"
    return "Strategic reserve"


def _strengths(
    *,
    power_score: int,
    fiber_score: int,
    water_score: int,
    talent_score: int,
    candidate: ListingCandidate,
) -> list[str]:
    strengths = [
        ("power adjacency", power_score),
        ("fiber reach", fiber_score),
        ("water optionality", water_score),
        ("talent access", talent_score),
        ("marketed availability", 70 if candidate.asking_price is not None else 55),
    ]
    return [name for name, _ in sorted(strengths, key=lambda item: (-item[1], item[0]))[:3]]


def _acreage_band(candidate: ListingCandidate) -> str:
    if candidate.acreage is not None and candidate.acreage > 0:
        acreage = round(candidate.acreage, 1)
        if _SWEET_SPOT_ACREAGE_MIN <= acreage <= _SWEET_SPOT_ACREAGE_MAX:
            return f"{acreage:.1f} acres (1-2 acre sweet spot)"
        if acreage < _SWEET_SPOT_ACREAGE_MIN:
            return f"{acreage:.1f} acres (below target footprint)"
        if acreage <= 3:
            return f"{acreage:.1f} acres (above target footprint)"
        if acreage > 25:
            return f"{acreage:.1f} acres (large tract)"
        if acreage > 10:
            return f"{acreage:.1f} acres (expansion tract)"
        return f"{acreage:.1f} acres (oversized tract)"
    if candidate.building_sqft is not None and candidate.building_sqft > 0:
        return f"{int(candidate.building_sqft):,} sqft building"
    return "Acreage not disclosed"


def _confidence_score() -> int:
    weighted = (
        20 * _QUALITY_WEIGHT["measured"]
        + 12 * _QUALITY_WEIGHT["measured"]
        + 12 * _QUALITY_WEIGHT["proxy"]
        + 10 * _QUALITY_WEIGHT["measured"]
        + 10 * _QUALITY_WEIGHT["heuristic"]
        + 8 * _QUALITY_WEIGHT["measured"]
        + 8 * _QUALITY_WEIGHT["proxy"]
        + 8 * _QUALITY_WEIGHT["proxy"]
        + 6 * _QUALITY_WEIGHT["measured"]
        + 6 * _QUALITY_WEIGHT["measured"]
    )
    return round(weighted)


def _distance_score(
    distance: float | None,
    *,
    ideal: float,
    acceptable: float,
    max_distance: float,
) -> float:
    if distance is None:
        return 0.0
    if distance <= ideal:
        return 1.0
    if distance <= acceptable:
        span = max(acceptable - ideal, 1.0)
        return _clamp(1.0 - ((distance - ideal) / span) * 0.4)
    if distance >= max_distance:
        return 0.0
    span = max(max_distance - acceptable, 1.0)
    return _clamp(0.6 - ((distance - acceptable) / span) * 0.6)


def _normalize_range(value: float | None, *, minimum: float, target: float) -> float:
    if value is None or value <= 0:
        return 0.0
    if value <= minimum:
        return 0.15
    if value >= target:
        return 1.0
    return _clamp((value - minimum) / (target - minimum))


def _acreage_fit_score(acreage: float | None) -> float:
    if acreage is None or acreage <= 0:
        return 0.0
    if acreage < 0.5:
        return 0.12
    if acreage < 1.0:
        return _interpolate(acreage, start=0.5, end=1.0, low=0.25, high=0.78)
    if acreage <= 1.5:
        return _interpolate(acreage, start=1.0, end=1.5, low=0.95, high=1.0)
    if acreage <= _SWEET_SPOT_ACREAGE_MAX:
        return _interpolate(
            acreage,
            start=1.5,
            end=_SWEET_SPOT_ACREAGE_MAX,
            low=1.0,
            high=0.96,
        )
    if acreage <= 3.0:
        return _interpolate(
            acreage,
            start=_SWEET_SPOT_ACREAGE_MAX,
            end=3.0,
            low=0.65,
            high=0.35,
        )
    if acreage <= 10.0:
        return _interpolate(acreage, start=3.0, end=10.0, low=0.30, high=0.12)
    if acreage <= 25.0:
        return _interpolate(acreage, start=10.0, end=25.0, low=0.12, high=0.06)
    return 0.03


def _building_fit_score(building_sqft: float | None) -> float:
    if building_sqft is None or building_sqft <= 0:
        return 0.0
    if building_sqft < 10_000:
        return 0.30
    if building_sqft < 25_000:
        return _interpolate(
            building_sqft,
            start=10_000.0,
            end=25_000.0,
            low=0.45,
            high=0.75,
        )
    if building_sqft <= 150_000:
        return _interpolate(
            building_sqft,
            start=25_000.0,
            end=150_000.0,
            low=0.78,
            high=1.0,
        )
    if building_sqft <= 300_000:
        return _interpolate(
            building_sqft,
            start=150_000.0,
            end=300_000.0,
            low=0.90,
            high=0.72,
        )
    return 0.65


def _safe_float(value: str | None) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(str(value).replace(",", "").replace("$", "").strip())
    except ValueError:
        return None


def _safe_int(value: str | None) -> int | None:
    numeric = _safe_float(value)
    if numeric is None:
        return None
    return round(numeric)


def _round_optional(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 2)


def _score_band(score: int) -> str:
    if score >= 72:
        return "Tier 1"
    if score >= 68:
        return "Tier 2"
    return "Tier 3"


def _clamp(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
    return max(minimum, min(maximum, value))


def _interpolate(
    value: float,
    *,
    start: float,
    end: float,
    low: float,
    high: float,
) -> float:
    if end <= start:
        return high
    ratio = (value - start) / (end - start)
    return _clamp(low + ((high - low) * ratio))


def _haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lat1_rad, lon1_rad, lat2_rad, lon2_rad = map(radians, (lat1, lon1, lat2, lon2))
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = sin(dlat / 2) ** 2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2) ** 2
    return 3958.7613 * 2 * asin(sqrt(a))
