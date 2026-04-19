# Source Interfaces

Phase 3 adds the first controlled source-interface seed for the approved source set.

## Seed File

- `db/seeds/source_interface.csv`

## Interface Baseline

- `PARCEL`
  - interface: `parcel_csv_v1`
  - load mode: `full`
  - required fields: `parcel_id`, `county_fips`, `acreage`, `geometry_wkt`, `lineage_key`
- `ZONING`
  - interface: `zoning_csv_v1`
  - load mode: `full`
  - required fields: `parcel_id`, `county_fips`, `zoning_code`, `lineage_key`
- `FLOOD`, `UTILITY`, `MARKET`
  - evidence-style interfaces
  - load mode: `incremental`
  - required fields: `record_key`, `attribute_name`, `attribute_value`, `lineage_key`

## Source Catalog Alignment

The Phase 3 package also aligns `source_catalog.csv` target table names with the canonical
ingestion outputs:

- `PARCEL` -> `raw_parcels`
- `ZONING` -> `raw_zoning`
- `FLOOD` -> `source_evidence`
- `UTILITY` -> `source_evidence`
- `MARKET` -> `source_evidence`
