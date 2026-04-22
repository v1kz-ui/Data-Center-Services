from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
INVENTORY_PATH = REPO_ROOT / "configs" / "authoritative_source_inventory.json"
CONNECTOR_CONFIG_PATH = REPO_ROOT / "configs" / "source_connectors.json"
SEED_DIR = REPO_ROOT / "db" / "seeds"
SCHEMA_VERSION = "2026.04"

METRO_ROWS = [
    {"metro_id": "TX", "display_name": "Texas Statewide", "state_code": "TX", "is_active": "true"},
    {"metro_id": "DFW", "display_name": "Dallas-Fort Worth", "state_code": "TX", "is_active": "true"},
    {"metro_id": "HOU", "display_name": "Houston", "state_code": "TX", "is_active": "true"},
    {"metro_id": "SAT", "display_name": "San Antonio", "state_code": "TX", "is_active": "true"},
    {"metro_id": "AUS", "display_name": "Austin", "state_code": "TX", "is_active": "true"},
    {"metro_id": "ELP", "display_name": "El Paso", "state_code": "TX", "is_active": "true"},
    {"metro_id": "LRD", "display_name": "Laredo", "state_code": "TX", "is_active": "true"},
    {"metro_id": "MFE", "display_name": "McAllen", "state_code": "TX", "is_active": "true"},
    {"metro_id": "CRP", "display_name": "Corpus Christi", "state_code": "TX", "is_active": "true"},
    {"metro_id": "MAF", "display_name": "Midland", "state_code": "TX", "is_active": "true"},
]

COUNTY_ROWS = [
    {"county_fips": "48085", "metro_id": "DFW", "display_name": "Collin", "state_code": "TX", "is_active": "true"},
    {"county_fips": "48113", "metro_id": "DFW", "display_name": "Dallas", "state_code": "TX", "is_active": "true"},
    {"county_fips": "48121", "metro_id": "DFW", "display_name": "Denton", "state_code": "TX", "is_active": "true"},
    {"county_fips": "48257", "metro_id": "DFW", "display_name": "Kaufman", "state_code": "TX", "is_active": "true"},
    {"county_fips": "48397", "metro_id": "DFW", "display_name": "Rockwall", "state_code": "TX", "is_active": "true"},
    {"county_fips": "48439", "metro_id": "DFW", "display_name": "Tarrant", "state_code": "TX", "is_active": "true"},
    {"county_fips": "48157", "metro_id": "HOU", "display_name": "Fort Bend", "state_code": "TX", "is_active": "true"},
    {"county_fips": "48201", "metro_id": "HOU", "display_name": "Harris", "state_code": "TX", "is_active": "true"},
    {"county_fips": "48339", "metro_id": "HOU", "display_name": "Montgomery", "state_code": "TX", "is_active": "true"},
    {"county_fips": "48029", "metro_id": "SAT", "display_name": "Bexar", "state_code": "TX", "is_active": "true"},
    {"county_fips": "48453", "metro_id": "AUS", "display_name": "Travis", "state_code": "TX", "is_active": "true"},
    {"county_fips": "48491", "metro_id": "AUS", "display_name": "Williamson", "state_code": "TX", "is_active": "true"},
    {"county_fips": "48141", "metro_id": "ELP", "display_name": "El Paso", "state_code": "TX", "is_active": "true"},
    {"county_fips": "48479", "metro_id": "LRD", "display_name": "Webb", "state_code": "TX", "is_active": "true"},
    {"county_fips": "48215", "metro_id": "MFE", "display_name": "Hidalgo", "state_code": "TX", "is_active": "true"},
    {"county_fips": "48355", "metro_id": "CRP", "display_name": "Nueces", "state_code": "TX", "is_active": "true"},
    {"county_fips": "48329", "metro_id": "MAF", "display_name": "Midland", "state_code": "TX", "is_active": "true"},
]

LEGACY_SOURCE_ROWS = [
    {
        "source_id": "PARCEL",
        "display_name": "Canonical Parcel Pipeline",
        "owner_name": "Data Governance",
        "refresh_cadence": "daily",
        "block_refresh": "true",
        "metro_coverage": "DFW",
        "target_table_name": "raw_parcels",
        "is_active": "true",
    },
    {
        "source_id": "ZONING",
        "display_name": "Canonical Zoning Pipeline",
        "owner_name": "Data Governance",
        "refresh_cadence": "weekly",
        "block_refresh": "true",
        "metro_coverage": "DFW",
        "target_table_name": "raw_zoning",
        "is_active": "true",
    },
    {
        "source_id": "LISTING",
        "display_name": "Market Listings Scraper",
        "owner_name": "Data Governance",
        "refresh_cadence": "daily",
        "block_refresh": "false",
        "metro_coverage": "TX",
        "target_table_name": "market_listings",
        "is_active": "true",
    },
]

LEGACY_SOURCE_NOTES = {
    "PARCEL": "Requires parcel_id, county_fips, acreage, geometry_wkt, lineage_key.",
    "ZONING": "Requires parcel_id, county_fips, zoning_code, lineage_key.",
}

LISTING_SOURCE_ROWS = [
    {
        "listing_source_id": "myelisting",
        "display_name": "MyEListing Texas Complete Feed",
        "acquisition_method": "html_scrape",
        "base_url": "https://myelisting.com/sitemap.xml",
        "terms_url": "https://myelisting.com/terms/",
        "allows_scraping": "true",
        "compliance_notes": (
            "Customer-approved public listing ingestion. Respect robots.txt, pace requests, "
            "and retain source attribution links."
        ),
        "is_active": "true",
    },
    {
        "listing_source_id": "myelisting_lease",
        "display_name": "MyEListing Texas Lease Feed (legacy split)",
        "acquisition_method": "html_scrape",
        "base_url": "https://myelisting.com/properties/for-lease/texas/all-property-types/",
        "terms_url": "https://myelisting.com/terms/",
        "allows_scraping": "true",
        "compliance_notes": (
            "Legacy split lease feed retained for lineage history. Prefer the complete "
            "MyEListing feed for active sale and lease coverage."
        ),
        "is_active": "false",
    },
    {
        "listing_source_id": "acrevalue",
        "display_name": "AcreValue Texas Land Listings",
        "acquisition_method": "public_json",
        "base_url": "https://www.acrevalue.com/land-for-sale-Texas/",
        "terms_url": "https://www.acrevalue.com/terms/",
        "allows_scraping": "true",
        "compliance_notes": (
            "Customer-approved public land listing ingestion from AcreValue rendered pages "
            "and public listing JSON. Respect robots.txt, pace requests, and retain source "
            "attribution links."
        ),
        "is_active": "true",
    },
]


def main() -> None:
    inventory_payload = json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))
    connector_payload = json.loads(CONNECTOR_CONFIG_PATH.read_text(encoding="utf-8"))

    inventory_sources = list(inventory_payload["sources"])
    explicit_definitions = list(connector_payload.get("definitions") or [])
    explicit_if_codes = {
        if_code
        for definition in explicit_definitions
        for if_code in (definition.get("inventory_if_codes") or [])
    }

    source_rows = _build_source_catalog_rows(inventory_sources)
    interface_rows = _build_source_interface_rows(
        inventory_sources=inventory_sources,
        explicit_definitions=explicit_definitions,
        explicit_if_codes=explicit_if_codes,
    )

    _write_csv(
        SEED_DIR / "metro_catalog.csv",
        fieldnames=["metro_id", "display_name", "state_code", "is_active"],
        rows=METRO_ROWS,
    )
    _write_csv(
        SEED_DIR / "county_catalog.csv",
        fieldnames=["county_fips", "metro_id", "display_name", "state_code", "is_active"],
        rows=COUNTY_ROWS,
    )
    _write_csv(
        SEED_DIR / "source_catalog.csv",
        fieldnames=[
            "source_id",
            "display_name",
            "owner_name",
            "refresh_cadence",
            "block_refresh",
            "metro_coverage",
            "target_table_name",
            "is_active",
        ],
        rows=source_rows,
    )
    _write_csv(
        SEED_DIR / "listing_source_catalog.csv",
        fieldnames=[
            "listing_source_id",
            "display_name",
            "acquisition_method",
            "base_url",
            "terms_url",
            "allows_scraping",
            "compliance_notes",
            "is_active",
        ],
        rows=LISTING_SOURCE_ROWS,
    )
    _write_csv(
        SEED_DIR / "source_interface.csv",
        fieldnames=[
            "source_id",
            "interface_name",
            "schema_version",
            "load_mode",
            "validation_notes",
        ],
        rows=interface_rows,
    )

    print(
        json.dumps(
            {
                "metro_rows": len(METRO_ROWS),
                "county_rows": len(COUNTY_ROWS),
                "source_rows": len(source_rows),
                "listing_source_rows": len(LISTING_SOURCE_ROWS),
                "source_interface_rows": len(interface_rows),
            },
            indent=2,
        )
    )


def _build_source_catalog_rows(inventory_sources: list[dict[str, Any]]) -> list[dict[str, str]]:
    rows = list(LEGACY_SOURCE_ROWS)
    for source in inventory_sources:
        target_table = str(source.get("target_table") or "").strip().lower()
        rows.append(
            {
                "source_id": str(source["if_code"]).strip(),
                "display_name": str(source["name"]).strip(),
                "owner_name": _derive_owner_name(str(source["category"]).strip()),
                "refresh_cadence": _derive_refresh_cadence(source),
                "block_refresh": "true" if target_table in {"raw_parcels", "raw_zoning"} else "false",
                "metro_coverage": _infer_inventory_metro_id(source),
                "target_table_name": _canonical_target_table_name(target_table),
                "is_active": "true",
            }
        )
    return sorted(rows, key=lambda row: row["source_id"])


def _build_source_interface_rows(
    *,
    inventory_sources: list[dict[str, Any]],
    explicit_definitions: list[dict[str, Any]],
    explicit_if_codes: set[str],
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen_keys: set[tuple[str, str]] = set()

    for definition in explicit_definitions:
        source_id = str(definition["source_id"]).strip().upper()
        interface_name = str(definition["interface_name"]).strip()
        key = (source_id, interface_name)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        load_strategy = str(definition.get("load_strategy") or _default_load_strategy(source_id)).strip().lower()
        rows.append(
            {
                "source_id": source_id,
                "interface_name": interface_name,
                "schema_version": SCHEMA_VERSION,
                "load_mode": _load_mode_for_strategy(load_strategy),
                "validation_notes": _validation_notes_for_strategy(load_strategy, source_id),
            }
        )

    for source in inventory_sources:
        if_code = str(source["if_code"]).strip().upper()
        if if_code in explicit_if_codes:
            continue
        adapter_type = _infer_inventory_adapter_type(source)
        interface_name = _build_inventory_interface_name(if_code, adapter_type)
        key = (if_code, interface_name)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        load_strategy = _infer_inventory_load_strategy(str(source.get("target_table") or "source_evidence"))
        rows.append(
            {
                "source_id": if_code,
                "interface_name": interface_name,
                "schema_version": SCHEMA_VERSION,
                "load_mode": _load_mode_for_strategy(load_strategy),
                "validation_notes": _validation_notes_for_strategy(load_strategy, if_code),
            }
        )

    return sorted(rows, key=lambda row: (row["source_id"], row["interface_name"]))


def _derive_owner_name(category: str) -> str:
    normalized = category.strip().lower()
    if normalized == "federal_state":
        return "Federal / State Program"
    if normalized == "county_cad":
        return "County Appraisal District"
    if normalized == "city_zoning":
        return "City GIS / Planning"
    return "Data Governance"


def _derive_refresh_cadence(source: dict[str, Any]) -> str:
    target_table = str(source.get("target_table") or "").strip().lower()
    protocol = str(source.get("protocol") or "").strip().lower()
    if target_table == "raw_parcels":
        return "daily"
    if target_table == "raw_zoning":
        return "weekly"
    if "rest api" in protocol or "arcgis" in protocol or "json" in protocol:
        return "daily"
    if "manual" in protocol or "pdf" in protocol or "csv" in protocol or "download" in protocol or "shapefile" in protocol:
        return "monthly"
    return "weekly"


def _canonical_target_table_name(target_table: str) -> str:
    if target_table in {"raw_parcels", "raw_zoning", "market_listings"}:
        return target_table
    return "source_evidence"


def _infer_inventory_adapter_type(source_payload: dict[str, Any]) -> str:
    protocol = str(source_payload.get("protocol") or "").strip().lower()
    if "manual" in protocol or "pdf" in protocol:
        return "manual"
    if "shapefile" in protocol:
        return "http_zip_shapefile"
    if "csv" in protocol or "txt" in protocol or "download" in protocol:
        if "rest api" in protocol or "api" in protocol:
            return "http_json"
        return "http_csv"
    if "arcgis" in protocol or "featureserver" in protocol or "mapserver" in protocol:
        return "arcgis_feature_service"
    if "rest api" in protocol or "json" in protocol:
        return "http_json"
    return "manual"


def _infer_inventory_metro_id(source_payload: dict[str, Any]) -> str:
    if_code = str(source_payload["if_code"]).strip().upper()
    city = str(source_payload.get("city") or "").strip().lower()
    county = str(source_payload.get("county") or "").strip().lower()
    name = str(source_payload.get("name") or "").strip().lower()

    if if_code in {"IF-029", "IF-030", "IF-031", "IF-032", "IF-044", "IF-045"}:
        return "DFW"
    if if_code in {"IF-033", "IF-034", "IF-035"} or "houston" in name:
        return "HOU"
    if if_code == "IF-036" or "san antonio" in city or "bexar" in county:
        return "SAT"
    if if_code in {"IF-037", "IF-038", "IF-047"} or "austin" in city or "travis" in county:
        return "AUS"
    if if_code in {"IF-039", "IF-048"} or "el paso" in city or "el paso" in county:
        return "ELP"
    if if_code in {"IF-040", "IF-049"} or "laredo" in city or "webb" in county:
        return "LRD"
    if if_code in {"IF-041", "IF-050"} or "mcallen" in city or "hidalgo" in county:
        return "MFE"
    if if_code in {"IF-042", "IF-051"} or "corpus christi" in city or "nueces" in county:
        return "CRP"
    if if_code == "IF-043" or "midland" in city or "midland" in county:
        return "MAF"
    return "TX"


def _infer_inventory_load_strategy(target_table: str) -> str:
    normalized = target_table.strip().lower()
    if normalized == "raw_parcels":
        return "parcel"
    if normalized == "raw_zoning":
        return "zoning"
    return "evidence"


def _build_inventory_interface_name(if_code: str, adapter_type: str) -> str:
    return f"{if_code.lower()}-{adapter_type.replace('_', '-')}-v1"


def _default_load_strategy(source_id: str) -> str:
    normalized = source_id.strip().upper()
    if normalized == "PARCEL":
        return "parcel"
    if normalized == "ZONING":
        return "zoning"
    if normalized == "LISTING":
        return "market_listing"
    return "evidence"


def _load_mode_for_strategy(load_strategy: str) -> str:
    if load_strategy in {"parcel", "zoning", "market_listing"}:
        return "full"
    return "incremental"


def _validation_notes_for_strategy(load_strategy: str, source_id: str) -> str:
    normalized_source_id = source_id.strip().upper()
    if normalized_source_id in LEGACY_SOURCE_NOTES:
        return LEGACY_SOURCE_NOTES[normalized_source_id]
    if load_strategy == "parcel":
        return "Requires parcel_id, county_fips, acreage, geometry_wkt, lineage_key."
    if load_strategy == "zoning":
        return "Requires parcel_id, county_fips, zoning_code, lineage_key."
    if load_strategy == "market_listing":
        return (
            "Requires source_listing_key, listing_title, source_url, lineage_key. "
            "Optional parcel linkage and market metadata are allowed."
        )
    return "Requires record_key, attribute_name, attribute_value, lineage_key."


def _write_csv(path: Path, *, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
