# Phase 1 Decision Register

| ID | Status | Decision | Rationale | Linked Artifact |
| --- | --- | --- | --- | --- |
| `D-001` | accepted | Use one monorepo with API, workers, db, tests, configs, and infra folders | keeps early build coordination simple and traceable | `ADR-002` |
| `D-002` | accepted | Use Python `3.12+`, FastAPI, PostgreSQL/PostGIS, SQLAlchemy, Alembic, pytest, Ruff, and mypy | matches build needs and local execution reality | `ADR-001` |
| `D-003` | accepted | Publish only through activated batches | prevents mixed-batch user exposure | `SAD` 8.3 |
| `D-004` | accepted | Remove `candidate_parcels_v` from the approved design | run-scoped evaluation is explicit and auditable | `SAD` 8.1 |
| `D-005` | accepted | Engineering pilot metro baseline is `DFW`, `AUS`, `PHX`, `LAS`; Brownsville is excluded | unblocks implementation planning while preserving explicit exclusion rules | `scope_baseline.md` |
| `D-006` | accepted | Blocking source baseline is `PARCEL`, `ZONING`, and `FLOOD` | aligns gating logic with the current seed catalog | `scope_baseline.md` |
| `D-007` | open | Final business-approved metro roster must be reconfirmed before Phase 2 expands source work | current roster is sufficient for engineering start, but sponsor confirmation is still needed | `raid_log.md` |

