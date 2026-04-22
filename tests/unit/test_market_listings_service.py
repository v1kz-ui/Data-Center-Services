from __future__ import annotations

from decimal import Decimal

from sqlalchemy.orm import Session

from app.db.models.catalogs import SourceCatalog
from app.db.models.market import ListingSourceCatalog, MarketListing
from app.db.models.territory import CountyCatalog, MetroCatalog
from app.services.market_listings import search_market_listings


def test_search_market_listings_filters_active_results(db_session: Session) -> None:
    _seed_market_listing_catalogs(db_session)
    db_session.add_all(
        [
            MarketListing(
                source_id="LISTING",
                listing_source_id="public-broker",
                metro_id="DFW",
                county_fips="48113",
                source_listing_key="tx-001",
                listing_title="Dallas Infill Land Site",
                asset_type="land",
                listing_status="for_sale",
                asking_price=Decimal("4500000"),
                acreage=Decimal("12.50"),
                city="Dallas",
                state_code="TX",
                broker_name="North Texas Brokerage",
                source_url="https://example.test/listings/tx-001",
                lineage_key="listing:tx-001",
                is_active=True,
            ),
            MarketListing(
                source_id="LISTING",
                listing_source_id="public-broker",
                metro_id="DFW",
                county_fips="48439",
                source_listing_key="tx-002",
                listing_title="Fort Worth Powered Building",
                asset_type="building",
                listing_status="for_lease",
                asking_price=Decimal("7800000"),
                building_sqft=Decimal("98500"),
                city="Fort Worth",
                state_code="TX",
                broker_name="Westplex Advisors",
                source_url="https://example.test/listings/tx-002",
                lineage_key="listing:tx-002",
                is_active=True,
            ),
            MarketListing(
                source_id="LISTING",
                listing_source_id="public-broker",
                metro_id="DFW",
                county_fips="48113",
                source_listing_key="tx-003",
                listing_title="Inactive Listing",
                asset_type="land",
                listing_status="for_sale",
                asking_price=Decimal("1000000"),
                acreage=Decimal("5.00"),
                city="Dallas",
                state_code="TX",
                source_url="https://example.test/listings/tx-003",
                lineage_key="listing:tx-003",
                is_active=False,
            ),
        ]
    )
    db_session.commit()

    page = search_market_listings(
        db_session,
        metro_id="DFW",
        asset_type="land",
        q="Dallas",
    )

    assert page.total_count == 1
    assert len(page.items) == 1
    assert page.items[0].listing_source_name == "Public Broker Listings"
    assert page.items[0].source_listing_key == "tx-001"
    assert page.items[0].asking_price == Decimal("4500000")


def _seed_market_listing_catalogs(session: Session) -> None:
    session.add(MetroCatalog(metro_id="DFW", display_name="Dallas-Fort Worth", state_code="TX"))
    session.add_all(
        [
            CountyCatalog(
                county_fips="48113",
                metro_id="DFW",
                display_name="Dallas",
                state_code="TX",
            ),
            CountyCatalog(
                county_fips="48439",
                metro_id="DFW",
                display_name="Tarrant",
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
            target_table_name="market_listings",
            is_active=True,
        )
    )
    session.add(
        ListingSourceCatalog(
            listing_source_id="public-broker",
            display_name="Public Broker Listings",
            acquisition_method="html_scrape",
            base_url="https://example.test",
            allows_scraping=True,
            is_active=True,
        )
    )
    session.commit()
