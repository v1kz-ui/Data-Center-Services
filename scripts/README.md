# Scripts

This folder is reserved for local automation, developer bootstrap helpers, and repeatable operational scripts.

Prefer scripts that:

- are idempotent,
- log clearly,
- do not hide failures,
- match the agent-run delivery model.

Current operational helpers:

- `sync_authoritative_reference_seeds.py`: regenerates the authoritative Texas
  source, listing-source, interface, metro, and county seed files from
  inventory and connector config
- `seed_reference_data.py`: loads the controlled reference seed bundle into a
  prepared database
- `run_live_candidate_parcel_scoring.py`: derives normalized parcel-scoring
  evidence from linked live candidates and executes the metro scoring batch
- `report_land_listing_hub_coverage.py`: audits active Texas land-for-sale
  listings within a configurable radius of Census-defined major population hubs
- `build_uat_manifest.py`: builds the Phase 7 UAT manifest from seeds and the
  scenario pack
