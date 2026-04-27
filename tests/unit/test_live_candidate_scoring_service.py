from __future__ import annotations

from decimal import Decimal

from sqlalchemy.orm import Session

from app.db.models.catalogs import SourceCatalog
from app.db.models.market import ListingSourceCatalog, MarketListing
from app.db.models.source_data import SourceEvidence
from app.db.models.territory import MetroCatalog
from app.services.live_candidate_scoring import (
    _PRIMARY_FEATURED_METRO_CAPS,
    _PRIMARY_FEATURED_METRO_MINIMUMS,
    ScoredCandidate,
    _select_balanced_opportunities,
    build_live_candidate_opportunities,
)


def test_build_live_candidate_opportunities_prefers_right_sized_site_over_large_tract(
    db_session: Session,
) -> None:
    _seed_live_candidate_context(db_session)

    opportunities = build_live_candidate_opportunities(db_session, limit=10)

    assert len(opportunities) == 1
    top = opportunities[0]
    assert top["site_name"] == "Austin Right-Sized Powered Industrial Land"
    assert top["metro"] == "Austin"
    assert top["city"] == "Austin"
    assert top["confidence_score"] is not None
    assert top["viability_score"] >= 70
    assert top["listing_source_id"] == "myelisting"
    assert top["source_url"] == "https://example.test/listings/austin-right-sized-powered-industrial-land"
    assert top["acreage_band"] == "1.4 acres (1-2 acre sweet spot)"
    assert top["price_per_acre"] == 235714.29
    assert top["broker_name"] is None
    assert top["nearest_substation_name"] == "Austin Substation"
    assert top["nearest_peering_facility_name"] == "Austin Carrier Hotel"
    assert top["nearest_highway_name"] == "I 35"
    assert top["nearest_water_name"] == "Colorado River at Austin"
    assert top["social_score"] >= 50
    assert top["political_score"] >= 45
    assert top["approval_score"] >= 50
    assert top["social_category"] in {
        "community-aligned",
        "community-manageable",
        "community-sensitive",
        "community-fragile",
    }
    assert top["political_category"] in {
        "permit-forward",
        "policy-manageable",
        "hearing-sensitive",
        "politically fragile",
    }
    assert top["approval_stage"]
    assert top["approval_headwinds"]
    assert "Approval path currently reads" in top["approval_summary"]


def test_select_balanced_opportunities_guarantees_major_metro_floor() -> None:
    ranked: list[ScoredCandidate] = []
    metros = (
        "Dallas-Fort Worth",
        "Houston",
        "Austin",
        "San Antonio",
        "El Paso",
        "Rio Grande Valley",
        "Brazos Valley",
    )

    score = 100
    for metro in metros:
        if metro == "Dallas-Fort Worth":
            listing_count = 35
        elif metro in {"Houston", "San Antonio"}:
            listing_count = 30
        else:
            listing_count = 18
        for index in range(listing_count):
            ranked.append(
                ScoredCandidate(
                    opportunity={
                        "metro": metro,
                        "site_name": f"{metro} site {index + 1}",
                    },
                    confidence_score=82,
                    viability_score=score,
                    market_weight=1.0,
                )
            )
            score -= 1

    selected = _select_balanced_opportunities(
        ranked,
        limit=136,
        per_metro_cap=30,
        major_metro_minimums=_PRIMARY_FEATURED_METRO_MINIMUMS,
        metro_caps=_PRIMARY_FEATURED_METRO_CAPS,
    )

    assert 60 <= len(selected) <= 150
    counts: dict[str, int] = {}
    for item in selected:
        counts[item["metro"]] = counts.get(item["metro"], 0) + 1

    for metro_name, minimum in _PRIMARY_FEATURED_METRO_MINIMUMS.items():
        assert counts.get(metro_name, 0) >= minimum
    assert counts.get("Dallas-Fort Worth", 0) <= 30
    assert counts.get("Austin", 0) <= 20
    assert counts.get("Houston", 0) <= 25
    assert counts.get("San Antonio", 0) <= 25


def _seed_live_candidate_context(session: Session) -> None:
    session.add(MetroCatalog(metro_id="TX", display_name="Texas Statewide", state_code="TX"))
    session.add(
        SourceCatalog(
            source_id="LISTING",
            display_name="Market Listings Scraper",
            owner_name="Data Governance",
            refresh_cadence="daily",
            block_refresh=False,
            metro_coverage="TX",
            target_table_name="market_listing",
            is_active=True,
        )
    )
    for source_id in ("IF-001", "IF-007", "IF-009", "IF-021", "IF-023", "IF-025", "IF-026"):
        session.add(
            SourceCatalog(
                source_id=source_id,
                display_name=source_id,
                owner_name="Data Governance",
                refresh_cadence="daily",
                block_refresh=False,
                metro_coverage="TX",
                target_table_name="source_evidence",
                is_active=True,
            )
        )
    session.add(
        ListingSourceCatalog(
            listing_source_id="myelisting",
            display_name="MyEListing",
            acquisition_method="html_scrape",
            base_url="https://myelisting.com",
            allows_scraping=True,
            is_active=True,
        )
    )
    session.flush()

    session.add(
        MarketListing(
            source_id="LISTING",
            listing_source_id="myelisting",
            metro_id="TX",
            source_listing_key="austin-right-sized-powered-industrial-land",
            listing_title="Austin Right-Sized Powered Industrial Land",
            asset_type="commercial land",
            listing_status="sale",
            asking_price=Decimal("330000"),
            acreage=Decimal("1.4"),
            city="Austin",
            state_code="TX",
            latitude=Decimal("30.2840"),
            longitude=Decimal("-97.7010"),
            source_url="https://example.test/listings/austin-right-sized-powered-industrial-land",
            lineage_key="listing:austin-right-sized-powered-industrial-land",
            is_active=True,
        )
    )
    session.add(
        MarketListing(
            source_id="LISTING",
            listing_source_id="myelisting",
            metro_id="TX",
            source_listing_key="austin-powered-industrial-land",
            listing_title="Austin Large Tract Powered Industrial Land",
            asset_type="commercial land",
            listing_status="sale",
            asking_price=Decimal("6250000"),
            acreage=Decimal("45.5"),
            city="Austin",
            state_code="TX",
            latitude=Decimal("30.2850"),
            longitude=Decimal("-97.7000"),
            source_url="https://example.test/listings/austin-powered-industrial-land",
            lineage_key="listing:austin-powered-industrial-land",
            is_active=True,
        )
    )
    session.add(
        MarketListing(
            source_id="LISTING",
            listing_source_id="myelisting",
            metro_id="TX",
            source_listing_key="austin-retail-pad",
            listing_title="Austin Retail Pad",
            asset_type="retail space",
            listing_status="sale",
            asking_price=Decimal("2250000"),
            acreage=Decimal("3.0"),
            city="Austin",
            state_code="TX",
            latitude=Decimal("30.2900"),
            longitude=Decimal("-97.7100"),
            source_url="https://example.test/listings/austin-retail-pad",
            lineage_key="listing:austin-retail-pad",
            is_active=True,
        )
    )
    session.add(
        MarketListing(
            source_id="LISTING",
            listing_source_id="myelisting",
            metro_id="TX",
            source_listing_key="far-austin-powered-industrial-land",
            listing_title="Far Austin Powered Industrial Land",
            asset_type="commercial land",
            listing_status="sale",
            asking_price=Decimal("375000"),
            acreage=Decimal("2.5"),
            city="Georgetown",
            state_code="TX",
            latitude=Decimal("30.9000"),
            longitude=Decimal("-97.7000"),
            source_url="https://example.test/listings/far-austin-powered-industrial-land",
            lineage_key="listing:far-austin-powered-industrial-land",
            is_active=True,
        )
    )

    _add_evidence(session, "IF-001", "substation:aus-1", "latitude", "30.2800")
    _add_evidence(session, "IF-001", "substation:aus-1", "longitude", "-97.6900")
    _add_evidence(session, "IF-001", "substation:aus-1", "facility_name", "Austin Substation")
    _add_evidence(session, "IF-001", "substation:aus-1", "max_voltage_kv", "345")

    _add_evidence(session, "IF-009", "peeringdb:fac:aus-1", "latitude", "30.2680")
    _add_evidence(session, "IF-009", "peeringdb:fac:aus-1", "longitude", "-97.7420")
    _add_evidence(session, "IF-009", "peeringdb:fac:aus-1", "facility_name", "Austin Carrier Hotel")
    _add_evidence(session, "IF-009", "peeringdb:fac:aus-1", "carrier_count", "4")
    _add_evidence(session, "IF-009", "peeringdb:fac:aus-1", "ix_count", "1")
    _add_evidence(session, "IF-009", "peeringdb:fac:aus-1", "net_count", "18")

    _add_evidence(session, "IF-021", "water-site:aus-1", "latitude", "30.2700")
    _add_evidence(session, "IF-021", "water-site:aus-1", "longitude", "-97.7350")
    _add_evidence(session, "IF-021", "water-site:aus-1", "site_name", "Colorado River at Austin")

    _add_evidence(session, "IF-025", "power-plant:aus-1", "latitude", "30.3000")
    _add_evidence(session, "IF-025", "power-plant:aus-1", "longitude", "-97.6800")
    _add_evidence(session, "IF-025", "power-plant:aus-1", "plant_name", "Austin Gas Plant")
    _add_evidence(session, "IF-025", "power-plant:aus-1", "installed_capacity_mw", "650")

    _add_evidence(session, "IF-026", "osm-way:aus-i35", "latitude", "30.2860")
    _add_evidence(session, "IF-026", "osm-way:aus-i35", "longitude", "-97.7050")
    _add_evidence(session, "IF-026", "osm-way:aus-i35", "ref", "I 35")
    _add_evidence(session, "IF-026", "osm-way:aus-i35", "highway_type", "motorway")

    _add_evidence(session, "IF-007", "nri-county:48453", "county", "Travis")
    _add_evidence(session, "IF-007", "nri-county:48453", "expected_annual_loss_percentile", "35")
    _add_evidence(session, "IF-007", "nri-county:48453", "community_resilience_percentile", "72")

    session.commit()


def _add_evidence(
    session: Session,
    source_id: str,
    record_key: str,
    attribute_name: str,
    attribute_value: str,
) -> None:
    session.add(
        SourceEvidence(
            source_id=source_id,
            metro_id="TX",
            record_key=record_key,
            attribute_name=attribute_name,
            attribute_value=attribute_value,
            lineage_key=f"{record_key}:{attribute_name}",
            is_active=True,
        )
    )
