# Seed Data

These seed files establish the first controlled reference datasets for local and lower-environment builds.

- `source_catalog.csv`: approved initial source inventory skeleton
- `source_interface.csv`: interface contracts and validation expectations by source
- `metro_catalog.csv`: approved engineering metro baseline
- `county_catalog.csv`: approved county-to-metro baseline
- `factor_catalog.csv`: factor IDs `F01-F10`
- `bonus_catalog.csv`: bonus IDs `B01-B05`
- `scoring_profile.csv`: scoring profile seed metadata
- `scoring_profile_factor.csv`: factor-budget allocations for the active profile

These are starter reference files and should be loaded only through controlled seed logic or migrations.

For the Phase 7 UAT package, use `scripts/seed_reference_data.py` to apply this
bundle into a prepared lower environment database and pair it with
`infra/uat/phase7_uat_scenarios.json`.
