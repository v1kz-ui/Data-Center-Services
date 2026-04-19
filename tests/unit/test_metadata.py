from app.db.models import MANAGED_TABLES


def test_foundational_tables_are_registered() -> None:
    expected = {
        "bonus_catalog",
        "county_catalog",
        "factor_catalog",
        "metro_catalog",
        "parcel_evaluations",
        "parcel_exclusion_events",
        "parcel_rep_point",
        "operator_action_event",
        "raw_parcels",
        "raw_zoning",
        "score_batch",
        "score_bonus_detail",
        "score_factor_detail",
        "score_factor_input",
        "score_run",
        "scoring_profile",
        "scoring_profile_factor",
        "source_catalog",
        "source_evidence",
        "source_interface",
        "source_record_rejection",
        "source_snapshot",
    }
    assert expected.issubset(set(MANAGED_TABLES))
