import csv
from pathlib import Path


def test_default_scoring_profile_budget_sums_to_100() -> None:
    seed_path = Path("db/seeds/scoring_profile_factor.csv")
    with seed_path.open(encoding="utf-8") as seed_file:
        rows = list(csv.DictReader(seed_file))

    total = sum(int(row["max_points"]) for row in rows if row["profile_name"] == "default_v1")
    assert total == 100
