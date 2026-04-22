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
    assert coverage.covered_sources == 34
    assert coverage.uncovered_sources == 0
    assert coverage.coverage_percent == 100.0
    assert coverage.live_ready_sources == 16
    assert coverage.not_live_ready_sources == 18
    assert coverage.live_ready_percent == 47.06
    assert len(coverage.covered_if_codes) == 34
    assert "dfw_dallas_arcgis_zoning_live" not in coverage.unmapped_connector_keys
    assert "dfw_fort_worth_arcgis_zoning_live" not in coverage.unmapped_connector_keys
    assert "dfw_parcel_pilot" in coverage.unmapped_connector_keys
    acs_item = next(item for item in coverage.items if item.if_code == "IF-010")
    dallas_zoning_item = next(item for item in coverage.items if item.if_code == "IF-044")
    fort_worth_zoning_item = next(item for item in coverage.items if item.if_code == "IF-045")
    hifld_substations_item = next(item for item in coverage.items if item.if_code == "IF-001")
    hifld_transmission_item = next(item for item in coverage.items if item.if_code == "IF-002")
    hifld_gas_pipelines_item = next(item for item in coverage.items if item.if_code == "IF-003")
    hifld_gas_compressors_item = next(item for item in coverage.items if item.if_code == "IF-004")
    hifld_territories_item = next(item for item in coverage.items if item.if_code == "IF-005")
    nfhl_item = next(item for item in coverage.items if item.if_code == "IF-006")
    nri_item = next(item for item in coverage.items if item.if_code == "IF-007")
    tceq_item = next(item for item in coverage.items if item.if_code == "IF-019")
    twdb_item = next(item for item in coverage.items if item.if_code == "IF-020")
    census_tracts_item = next(item for item in coverage.items if item.if_code == "IF-011")
    superfund_item = next(item for item in coverage.items if item.if_code == "IF-023")
    critical_habitat_item = next(item for item in coverage.items if item.if_code == "IF-024")
    peeringdb_item = next(item for item in coverage.items if item.if_code == "IF-009")
    nwis_item = next(item for item in coverage.items if item.if_code == "IF-021")
    overpass_item = next(item for item in coverage.items if item.if_code == "IF-026")
    assert dallas_zoning_item.implemented is True
    assert dallas_zoning_item.live_ready is False
    assert dallas_zoning_item.connector_keys == ["dfw_dallas_arcgis_zoning_live"]
    assert dallas_zoning_item.enabled_connector_keys == []
    assert fort_worth_zoning_item.implemented is True
    assert fort_worth_zoning_item.live_ready is False
    assert fort_worth_zoning_item.connector_keys == ["dfw_fort_worth_arcgis_zoning_live"]
    assert fort_worth_zoning_item.enabled_connector_keys == []
    assert hifld_substations_item.implemented is True
    assert hifld_substations_item.live_ready is True
    assert hifld_substations_item.enabled_connector_keys == ["tx_hifld_substations_live"]
    assert hifld_transmission_item.live_ready is True
    assert hifld_transmission_item.enabled_connector_keys == ["tx_hifld_transmission_lines_live"]
    assert hifld_gas_pipelines_item.live_ready is True
    assert hifld_gas_pipelines_item.enabled_connector_keys == ["tx_hifld_gas_pipelines_live"]
    assert hifld_gas_compressors_item.live_ready is True
    assert hifld_gas_compressors_item.enabled_connector_keys == ["tx_hifld_gas_compressors_live"]
    assert hifld_territories_item.live_ready is True
    assert hifld_territories_item.enabled_connector_keys == ["tx_hifld_service_territories_live"]
    assert nfhl_item.live_ready is True
    assert nfhl_item.enabled_connector_keys == ["tx_fema_nfhl_flood_zones_live"]
    assert nri_item.live_ready is True
    assert nri_item.enabled_connector_keys == ["tx_fema_nri_counties_live"]
    assert tceq_item.live_ready is True
    assert tceq_item.enabled_connector_keys == ["tx_tceq_water_rights_live"]
    assert twdb_item.live_ready is True
    assert twdb_item.enabled_connector_keys == ["tx_twdb_groundwater_live"]
    assert acs_item.live_ready is True
    assert acs_item.enabled_connector_keys == ["tx_census_acs_live"]
    assert census_tracts_item.live_ready is True
    assert census_tracts_item.enabled_connector_keys == ["tx_census_tracts_live"]
    assert superfund_item.live_ready is True
    assert superfund_item.enabled_connector_keys == ["tx_epa_superfund_sites_live"]
    assert critical_habitat_item.live_ready is True
    assert critical_habitat_item.enabled_connector_keys == ["tx_usfws_critical_habitat_live"]
    assert peeringdb_item.live_ready is True
    assert peeringdb_item.enabled_connector_keys == ["tx_peeringdb_facilities_live"]
    assert nwis_item.live_ready is True
    assert nwis_item.enabled_connector_keys == ["tx_usgs_nwis_iv_live"]
    assert overpass_item.live_ready is True
    assert overpass_item.enabled_connector_keys == ["dfw_overpass_highways_live"]
    assert coverage.phase_coverage[0].live_ready_sources == 16
    assert coverage.phase_coverage[0].not_live_ready_sources == 18
    assert coverage.orphaned_connector_if_codes == []
