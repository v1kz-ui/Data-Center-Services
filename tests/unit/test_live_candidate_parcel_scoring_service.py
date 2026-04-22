from __future__ import annotations

from decimal import Decimal

from sqlalchemy.orm import Session

from app.db.models.market import MarketListing
from app.db.models.source_data import SourceEvidence
from app.db.models.territory import RawParcel
from app.services.live_candidate_parcel_scoring import (
    materialize_live_candidate_evidence,
    run_live_candidate_parcel_scoring,
)
from app.services.reference_seeds import load_reference_seed_bundle


def test_materialize_live_candidate_evidence_writes_factor_and_bonus_records(
    db_session: Session,
) -> None:
    _seed_context(db_session)

    linked_candidates, report = materialize_live_candidate_evidence(db_session, limit=10)
    evidence_rows = (
        db_session.query(SourceEvidence)
        .filter(SourceEvidence.source_id == "LIVE_SCORE")
        .order_by(SourceEvidence.attribute_name)
        .all()
    )

    assert len(linked_candidates) == 1
    assert report.unique_parcel_count == 1
    assert report.evidence_record_count == len(evidence_rows)
    assert {row.attribute_name for row in evidence_rows} >= {
        "f01_measured",
        "f10_measured",
        "b02_measured",
        "market_listing_id",
    }


def test_run_live_candidate_parcel_scoring_scores_linked_candidate_parcel(
    db_session: Session,
) -> None:
    _seed_context(db_session)

    report = run_live_candidate_parcel_scoring(
        db_session,
        limit=10,
        profile_name="texas_live_v1",
    )

    assert report.linked_listing_count == 1
    assert report.unique_parcel_count == 1
    assert report.evaluation_summaries[0]["evaluated_count"] == 1
    assert report.scoring_summaries[0]["scored_count"] == 1


def _seed_context(session: Session) -> None:
    load_reference_seed_bundle(session)
    session.commit()

    session.add(
        RawParcel(
            parcel_id="P-AUS-1",
            county_fips="48453",
            metro_id="AUS",
            acreage=Decimal("2.4"),
            geometry_wkt="POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
            lineage_key="parcel:P-AUS-1",
            is_active=True,
        )
    )
    session.add(
        MarketListing(
            source_id="LISTING",
            listing_source_id="myelisting",
            metro_id="TX",
            county_fips="48453",
            parcel_id="P-AUS-1",
            source_listing_key="austin-right-sized-powered-industrial-land",
            listing_title="Austin Right-Sized Powered Industrial Land",
            asset_type="commercial land",
            listing_status="sale",
            asking_price=Decimal("330000"),
            acreage=Decimal("2.4"),
            city="Austin",
            state_code="TX",
            latitude=Decimal("30.2840"),
            longitude=Decimal("-97.7010"),
            source_url="https://example.test/listings/austin-right-sized-powered-industrial-land",
            lineage_key="listing:austin-right-sized-powered-industrial-land",
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
