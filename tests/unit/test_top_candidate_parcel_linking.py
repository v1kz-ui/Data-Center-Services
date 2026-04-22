from __future__ import annotations

from decimal import Decimal

from sqlalchemy.orm import Session

from app.db.models.catalogs import SourceCatalog
from app.db.models.market import ListingSourceCatalog, MarketListing
from app.db.models.source_data import SourceEvidence
from app.db.models.territory import CountyCatalog, MetroCatalog, ParcelRepPoint, RawParcel
from app.services.top_candidate_parcel_linking import (
    link_top_live_candidates_to_parcels,
)


def test_link_top_live_candidates_to_parcels_matches_ranked_listing_inside_parcel(
    db_session: Session,
) -> None:
    _seed_live_link_context(db_session)

    report = link_top_live_candidates_to_parcels(
        db_session,
        limit=10,
        write_changes=True,
    )
    db_session.commit()

    listing = db_session.query(MarketListing).filter_by(source_listing_key="austin-powered-land").one()

    assert report.candidate_count == 1
    assert report.matched_count == 1
    assert report.coverage_rate == 1.0
    assert listing.parcel_id == "AUS:48453:P100"
    assert listing.county_fips == "48453"
    assert report.records[0].match_strategy in {"polygon_cover", "buffered_cover"}


def _seed_live_link_context(session: Session) -> None:
    session.add_all(
        [
            MetroCatalog(metro_id="TX", display_name="Texas Statewide", state_code="TX"),
            MetroCatalog(metro_id="AUS", display_name="Austin", state_code="TX"),
            CountyCatalog(
                county_fips="48453",
                metro_id="AUS",
                display_name="Travis",
                state_code="TX",
            ),
        ]
    )
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
            source_listing_key="austin-powered-land",
            listing_title="Austin Powered Land",
            asset_type="commercial land",
            listing_status="sale",
            asking_price=Decimal("5500000"),
            acreage=Decimal("50"),
            city="Austin",
            state_code="TX",
            latitude=Decimal("30.2850"),
            longitude=Decimal("-97.7000"),
            source_url="https://example.test/listings/austin-powered-land",
            lineage_key="listing:austin-powered-land",
            is_active=True,
        )
    )
    session.add(
        RawParcel(
            parcel_id="AUS:48453:P100",
            county_fips="48453",
            metro_id="AUS",
            apn="P100",
            acreage=Decimal("49.50"),
            geometry_wkt="POLYGON ((-97.705 30.280, -97.705 30.290, -97.695 30.290, -97.695 30.280, -97.705 30.280))",
            lineage_key="parcel:AUS:48453:P100",
            is_active=True,
        )
    )
    session.add(
        ParcelRepPoint(
            parcel_id="AUS:48453:P100",
            rep_point_wkt="POINT (-97.700 30.285)",
            geometry_method="representative_point",
        )
    )

    _add_evidence(session, "IF-001", "substation:aus-1", "latitude", "30.2800")
    _add_evidence(session, "IF-001", "substation:aus-1", "longitude", "-97.6900")
    _add_evidence(session, "IF-001", "substation:aus-1", "facility_name", "Austin Substation")
    _add_evidence(session, "IF-001", "substation:aus-1", "max_voltage_kv", "345")
    _add_evidence(session, "IF-009", "peeringdb:aus-1", "latitude", "30.2680")
    _add_evidence(session, "IF-009", "peeringdb:aus-1", "longitude", "-97.7420")
    _add_evidence(session, "IF-009", "peeringdb:aus-1", "facility_name", "Austin Carrier Hotel")
    _add_evidence(session, "IF-009", "peeringdb:aus-1", "carrier_count", "4")
    _add_evidence(session, "IF-009", "peeringdb:aus-1", "ix_count", "1")
    _add_evidence(session, "IF-009", "peeringdb:aus-1", "net_count", "18")
    _add_evidence(session, "IF-021", "water-site:aus-1", "latitude", "30.2700")
    _add_evidence(session, "IF-021", "water-site:aus-1", "longitude", "-97.7350")
    _add_evidence(session, "IF-021", "water-site:aus-1", "site_name", "Colorado River")
    _add_evidence(session, "IF-025", "plant:aus-1", "latitude", "30.3000")
    _add_evidence(session, "IF-025", "plant:aus-1", "longitude", "-97.6800")
    _add_evidence(session, "IF-025", "plant:aus-1", "plant_name", "Austin Gas Plant")
    _add_evidence(session, "IF-025", "plant:aus-1", "installed_capacity_mw", "650")
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
