# Configuration Assets

This folder contains example and environment-specific configuration files.

Do not store secrets here. Store only:

- example config shapes,
- non-secret defaults,
- environment wiring documentation.

## Included Inventories

- `source_connectors.json`
  - active and blueprint connector definitions used by the ingestion framework, including
    verified Dallas and Fort Worth zoning ArcGIS blueprints for the Phase 1 DFW scope
- `authoritative_source_inventory.json`
  - owner-approved v1.4 master inventory of 51 external sources plus the Houston no-zoning flag
