import csv
from pathlib import Path


def test_source_interface_seed_covers_all_approved_sources() -> None:
    seed_path = Path("db/seeds/source_interface.csv")
    with seed_path.open(encoding="utf-8") as seed_file:
        rows = list(csv.DictReader(seed_file))

    source_ids = {row["source_id"] for row in rows}

    assert source_ids == {"PARCEL", "ZONING", "FLOOD", "UTILITY", "MARKET"}


def test_source_catalog_seed_aligns_targets_with_phase3_canonical_tables() -> None:
    seed_path = Path("db/seeds/source_catalog.csv")
    with seed_path.open(encoding="utf-8") as seed_file:
        rows = {row["source_id"]: row["target_table_name"] for row in csv.DictReader(seed_file)}

    assert rows["PARCEL"] == "raw_parcels"
    assert rows["ZONING"] == "raw_zoning"
    assert rows["FLOOD"] == "source_evidence"
    assert rows["UTILITY"] == "source_evidence"
    assert rows["MARKET"] == "source_evidence"
