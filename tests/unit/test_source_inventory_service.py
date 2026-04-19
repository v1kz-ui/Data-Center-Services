from app.services.source_inventory import (
    build_source_inventory_coverage,
    build_source_inventory_summary,
    load_authoritative_source_inventory,
)
from ingestion.connectors import load_connector_registry


def test_source_inventory_summary_supports_phase_and_category_filters() -> None:
    inventory = load_authoritative_source_inventory("configs/authoritative_source_inventory.json")

    summary = build_source_inventory_summary(inventory, phase=1, category="city_zoning")

    assert summary.total_sources == 51
    assert summary.filtered_source_count == 2
    assert summary.filtered_config_flag_count == 0
    assert [item.if_code for item in summary.sources] == ["IF-044", "IF-045"]
    assert summary.category_counts[0].category == "city_zoning"
    assert summary.category_counts[0].source_count == 2


def test_source_inventory_coverage_reports_current_connector_alignment() -> None:
    inventory = load_authoritative_source_inventory("configs/authoritative_source_inventory.json")
    registry = load_connector_registry("configs/source_connectors.json")

    coverage = build_source_inventory_coverage(inventory, registry, phase=1)

    assert coverage.total_sources == 34
    assert coverage.covered_sources == 2
    assert coverage.uncovered_sources == 32
    assert coverage.coverage_percent == round((2 / 34) * 100, 2)
    assert coverage.covered_if_codes == ["IF-044", "IF-045"]
    assert "dfw_dallas_arcgis_zoning_live" not in coverage.unmapped_connector_keys
    assert "dfw_fort_worth_arcgis_zoning_live" not in coverage.unmapped_connector_keys
    assert "dfw_parcel_pilot" in coverage.unmapped_connector_keys
    dallas_zoning_item = next(item for item in coverage.items if item.if_code == "IF-044")
    fort_worth_zoning_item = next(item for item in coverage.items if item.if_code == "IF-045")
    assert dallas_zoning_item.implemented is True
    assert dallas_zoning_item.connector_keys == ["dfw_dallas_arcgis_zoning_live"]
    assert dallas_zoning_item.enabled_connector_keys == []
    assert fort_worth_zoning_item.implemented is True
    assert fort_worth_zoning_item.connector_keys == ["dfw_fort_worth_arcgis_zoning_live"]
    assert fort_worth_zoning_item.enabled_connector_keys == []
    assert coverage.orphaned_connector_if_codes == []
