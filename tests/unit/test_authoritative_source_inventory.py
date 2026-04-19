import json
from pathlib import Path


def test_authoritative_source_inventory_counts_and_if_codes() -> None:
    inventory_path = Path("configs/authoritative_source_inventory.json")
    payload = json.loads(inventory_path.read_text(encoding="utf-8"))

    sources = payload["sources"]
    if_codes = {source["if_code"] for source in sources}

    assert len(sources) == 51
    assert len(if_codes) == 51
    assert if_codes == {f"IF-{number:03d}" for number in range(1, 52)}


def test_authoritative_source_inventory_phase_totals_are_cumulative() -> None:
    inventory_path = Path("configs/authoritative_source_inventory.json")
    payload = json.loads(inventory_path.read_text(encoding="utf-8"))

    sources = payload["sources"]
    phase_totals = {item["phase"]: item["source_count"] for item in payload["phase_totals"]}

    assert len([source for source in sources if source["phase"] <= 1]) == phase_totals[1] == 34
    assert len([source for source in sources if source["phase"] <= 2]) == phase_totals[2] == 42
    assert len([source for source in sources if source["phase"] <= 3]) == phase_totals[3] == 51


def test_authoritative_source_inventory_contains_houston_flag_and_key_phase_sources() -> None:
    inventory_path = Path("configs/authoritative_source_inventory.json")
    payload = json.loads(inventory_path.read_text(encoding="utf-8"))

    sources_by_if_code = {source["if_code"]: source for source in payload["sources"]}
    houston_flag = payload["config_flags"][0]

    assert len(payload["config_flags"]) == 1
    assert houston_flag["flag_key"] == "verify_deed_restrictions"
    assert houston_flag["metro"] == "HOU"
    assert sources_by_if_code["IF-029"]["target_partition"] == "raw_parcels_dcad"
    assert sources_by_if_code["IF-044"]["target_table"] == "raw_zoning"
    assert sources_by_if_code["IF-051"]["city"] == "Corpus Christi"
