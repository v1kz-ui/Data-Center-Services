# Phase 1 Traceability Matrix

## Purpose

This matrix links the Phase 1 requirement baseline to design references, implementation anchors, test evidence, and the planned owner agent.

## Matrix

| Requirement IDs | Capability | Design References | Implementation Anchor | Test / Evidence Anchor | Owner Agent | Status |
| --- | --- | --- | --- | --- | --- | --- |
| `FR-001` to `FR-006` | source catalog and freshness gate baseline | `SAD` 4.1, 4.2; `SDD` 3.1, 5.1, 5.2 | `db/seeds/source_catalog.csv`, future `workers/ingestion` modules | `test_cases.md` freshness cases, `test_scripts.md` freshness scripts | `Ingestion Agent` | planned |
| `FR-007` to `FR-014` | parcel evaluation lifecycle | `SAD` 4.3; `SDD` 3.2, 5.3, 6.2 | future `workers/evaluation` modules | evaluation cases in `test_cases.md` | `GIS Agent` | planned |
| `FR-015` to `FR-023` | scoring, bonus, confidence, provenance | `SAD` 4.4; `SDD` 3.2, 5.4, 7 | `db/seeds/factor_catalog.csv`, `db/seeds/bonus_catalog.csv`, future `workers/scoring` modules | scoring and oracle tests in `test_cases.md` | `Scoring Agent` | planned |
| `FR-024` to `FR-028` | batch and publication semantics | `SAD` 4.5, 4.6, 7; `SDD` 5.5, 6.1, 8.2 | `apps/api/src/app/db/models/batching.py`, `workers/orchestrator/src/orchestrator/service.py`, `apps/api/src/app/api/routes/orchestration.py` | `tests/unit/test_orchestrator_service.py`, `tests/unit/test_orchestration_api.py` | `API Agent` | in progress |
| `FR-029` to `FR-033` | reader and operator interfaces | `SAD` 4.6, 4.7; `SDD` 8.1, 8.2 | `apps/api/src/app/main.py`, route packages, future admin/read APIs | `tests/unit/test_app_health.py`, future API suites | `API Agent` | in progress |
| `FR-034` to `FR-036` | security and admin controls | `SAD` 9.1; `SDD` 8.2, 10 | future auth/RBAC implementation | security scripts in `test_scripts.md` | `Ops-Release Agent` | planned |
| `DR-001` to `DR-006` | canonical data and audit storage | `SAD` 6.2; `SDD` 3.1, 3.2 | `db/migrations/versions/20260413_0001_initial_foundation.py` and future migrations | schema tests and migration rehearsal | `Schema Agent` | in progress |
| `NFR-001` to `NFR-012` | nonfunctional reliability, auditability, and testability | `SAD` 9, 10; `SDD` 7, 10, 11 | `pyproject.toml`, `.github/workflows/ci.yml`, operational docs | `pytest`, `ruff`, release evidence pack | `QA Agent` / `Ops-Release Agent` | in progress |

## Implemented Phase 1 Anchors

- build and package contract: `pyproject.toml`
- schema and reference-seed baseline: `db/migrations` and `db/seeds`
- API scaffold and health checks: `apps/api/src/app`
- orchestration preview and persisted batch endpoints: `apps/api/src/app/api/routes/orchestration.py`
- smoke and orchestration tests: `tests/unit`

## Traceability Rules

- Any behavior change must update at least one row in this matrix.
- New requirements must be assigned an owner agent before implementation begins.
- A requirement cannot be declared complete without at least one linked test or certification artifact.

