import csv
from pathlib import Path


def test_source_interface_seed_covers_all_approved_sources() -> None:
    seed_path = Path("db/seeds/source_interface.csv")
    with seed_path.open(encoding="utf-8") as seed_file:
        rows = list(csv.DictReader(seed_file))

    source_ids = {row["source_id"] for row in rows}

    assert "PARCEL" in source_ids
    assert "ZONING" in source_ids
    assert "LISTING" in source_ids
    assert {f"IF-{number:03d}" for number in range(1, 52)}.issubset(source_ids)
    assert "FLOOD" not in source_ids
    assert "UTILITY" not in source_ids
    assert "MARKET" not in source_ids


def test_source_catalog_seed_aligns_targets_with_phase3_canonical_tables() -> None:
    seed_path = Path("db/seeds/source_catalog.csv")
    with seed_path.open(encoding="utf-8") as seed_file:
        rows = {row["source_id"]: row["target_table_name"] for row in csv.DictReader(seed_file)}

    assert rows["PARCEL"] == "raw_parcels"
    assert rows["ZONING"] == "raw_zoning"
    assert rows["IF-001"] == "source_evidence"
    assert rows["IF-014"] == "source_evidence"
    assert rows["IF-029"] == "raw_parcels"
    assert rows["IF-044"] == "raw_zoning"
    assert rows["LISTING"] == "market_listings"
