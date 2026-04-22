from __future__ import annotations

from decimal import Decimal

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

from app.db.models.catalogs import SourceCatalog
from app.db.models.market import ListingSourceCatalog, MarketListing
from app.db.models.territory import CountyCatalog, MetroCatalog


def test_market_listing_search_endpoint_returns_active_results(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    session = session_factory()
    try:
        session.add(MetroCatalog(metro_id="DFW", display_name="Dallas-Fort Worth", state_code="TX"))
        session.add(
            CountyCatalog(
                county_fips="48113",
                metro_id="DFW",
                display_name="Dallas",
                state_code="TX",
            )
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
        session.add(
            MarketListing(
                source_id="LISTING",
                listing_source_id="public-broker",
                metro_id="DFW",
                county_fips="48113",
                source_listing_key="tx-001",
                listing_title="Dallas Industrial Tract",
                asset_type="land",
                listing_status="for_sale",
                asking_price=Decimal("5200000"),
                acreage=Decimal("14.75"),
                city="Dallas",
                state_code="TX",
                source_url="https://example.test/listings/tx-001",
                lineage_key="listing:tx-001",
                is_active=True,
            )
        )
        session.commit()
    finally:
        session.close()

    response = client.get(
        "/market-listings/search",
        params={"metro_id": "DFW", "asset_type": "land"},
    )
    payload = response.json()

    assert response.status_code == 200
    assert payload["total_count"] == 1
    assert payload["items"][0]["listing_source_id"] == "public-broker"
    assert payload["items"][0]["listing_source_name"] == "Public Broker Listings"
    assert payload["items"][0]["listing_title"] == "Dallas Industrial Tract"
