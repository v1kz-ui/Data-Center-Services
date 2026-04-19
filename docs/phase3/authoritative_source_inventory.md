# Authoritative Source Inventory

This artifact captures the authoritative v1.4 source list supplied by the project owner and
supersedes any provisional connector inventory notes.

## Canonical Machine-Readable File

- `configs/authoritative_source_inventory.json`

## Baseline Counts

- Phase 1: 34 sources
- Phase 2: 42 sources plus 1 Houston configuration flag
- Phase 3: 51 sources plus 1 Houston configuration flag

## Current Repo Alignment

- The ingestion framework already supports parcel, zoning, and evidence ingestion paths.
- The connector framework now supports fixture-backed connectors, generic HTTP JSON, and
  ArcGIS FeatureServer pagination and field transforms.
- Dallas parcel, Dallas zoning, and Fort Worth zoning live blueprints are already checked
  into `configs/source_connectors.json` as disabled connectors.
- The authoritative inventory is the build queue for the remaining connector waves.

## Immediate Build Order

- Phase 1 county parcel connectors: `IF-029` through `IF-032`
- Phase 1 zoning connectors:
  - `IF-044` Dallas is blueprinted as a disabled live ArcGIS connector
  - `IF-045` Fort Worth is blueprinted as a disabled live ArcGIS connector
- Phase 1 highest-value federal/state connectors for parcel scoring and screening:
  `IF-001`, `IF-006`, `IF-008`, `IF-009`, `IF-010`, `IF-011`, `IF-021`, `IF-023`, `IF-025`, `IF-026`

## Notes

- Houston remains a special-case rules configuration and not a zoning source integration.
- County CAD sources should land into `raw_parcels` using county-specific partitions or
  lineage keys that preserve county provenance.
- City zoning sources should land into canonical `raw_zoning`, parcel-keyed where possible,
  with overlay expansion when only polygon layers are available.
