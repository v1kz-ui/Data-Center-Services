import csv
from pathlib import Path


def test_engineering_metro_seed_baseline_is_texas_only() -> None:
    metro_path = Path("db/seeds/metro_catalog.csv")
    with metro_path.open(encoding="utf-8") as metro_file:
        rows = list(csv.DictReader(metro_file))

    metro_ids = {row["metro_id"] for row in rows}

    assert metro_ids == {"TX", "DFW", "HOU", "SAT", "AUS", "ELP", "LRD", "MFE", "CRP", "MAF"}
    assert "BRO" not in metro_ids
    assert all(row["state_code"] == "TX" for row in rows)


def test_county_seed_rows_map_to_known_metros() -> None:
    metro_path = Path("db/seeds/metro_catalog.csv")
    county_path = Path("db/seeds/county_catalog.csv")

    with metro_path.open(encoding="utf-8") as metro_file:
        metro_rows = list(csv.DictReader(metro_file))
    with county_path.open(encoding="utf-8") as county_file:
        county_rows = list(csv.DictReader(county_file))

    metro_ids = {row["metro_id"] for row in metro_rows}
    county_metro_ids = {row["metro_id"] for row in county_rows}
    county_fips = {row["county_fips"] for row in county_rows}

    assert county_metro_ids.issubset(metro_ids)
    assert {
        "48085",
        "48113",
        "48121",
        "48439",
        "48201",
        "48029",
        "48453",
        "48491",
        "48141",
        "48479",
        "48215",
        "48355",
        "48329",
    }.issubset(county_fips)
