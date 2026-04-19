# Dense Data Center Locator

This repository contains the build-ready implementation scaffold for the KIO Site Finder clean rewrite program. The delivery model, agent operating system, and implementation controls live under [docs/phase1](C:/Dev/Anti-Gravity/Dense%20Data%20Center%20Locator/docs/phase1/phase1_sprint_plan.md).

## Default Build Stack

- Python 3.12+
- FastAPI for APIs
- PostgreSQL 16 with PostGIS
- SQLAlchemy 2 and Alembic for persistence and migrations
- Pydantic v2 for settings and API models
- pytest for unit, integration, and end-to-end testing
- Ruff and mypy for linting and static checks

## Repository Layout

- `apps/api`: user-facing and admin API service
- `workers/orchestrator`: batch and run orchestration logic
- `workers/ingestion`: source ingestion and freshness logic
- `workers/evaluation`: parcel evaluation and exclusion logic
- `workers/scoring`: factor, bonus, confidence, and provenance logic
- `db/migrations`: versioned database migrations
- `db/seeds`: reference and seed data
- `tests`: unit, integration, and e2e test suites
- `infra`: environment and deployment assets
- `configs`: example settings and runtime config
- `scripts`: local automation and developer scripts

## Build-Ready Entry Points

- Program summary: [docs/phase1/phase1_sprint_plan.md](C:/Dev/Anti-Gravity/Dense%20Data%20Center%20Locator/docs/phase1/phase1_sprint_plan.md)
- Master delivery plan: [docs/phase1/master_delivery_plan.md](C:/Dev/Anti-Gravity/Dense%20Data%20Center%20Locator/docs/phase1/master_delivery_plan.md)
- Delivery controls: [docs/phase1/delivery_controls_pack.md](C:/Dev/Anti-Gravity/Dense%20Data%20Center%20Locator/docs/phase1/delivery_controls_pack.md)
- Agent operating playbook: [docs/phase1/agent_execution_playbook.md](C:/Dev/Anti-Gravity/Dense%20Data%20Center%20Locator/docs/phase1/agent_execution_playbook.md)
- Rewrite charter: [docs/phase1/rewrite_charter.md](C:/Dev/Anti-Gravity/Dense%20Data%20Center%20Locator/docs/phase1/rewrite_charter.md)
- Scope baseline: [docs/phase1/scope_baseline.md](C:/Dev/Anti-Gravity/Dense%20Data%20Center%20Locator/docs/phase1/scope_baseline.md)
- Traceability matrix: [docs/phase1/traceability_matrix.md](C:/Dev/Anti-Gravity/Dense%20Data%20Center%20Locator/docs/phase1/traceability_matrix.md)
- Build readiness checklist: [docs/phase1/build_readiness_checklist.md](C:/Dev/Anti-Gravity/Dense%20Data%20Center%20Locator/docs/phase1/build_readiness_checklist.md)
- Phase 1 exit report: [docs/phase1/phase1_exit_report.md](C:/Dev/Anti-Gravity/Dense%20Data%20Center%20Locator/docs/phase1/phase1_exit_report.md)
- Bootstrap guide: [docs/phase1/implementation_bootstrap.md](C:/Dev/Anti-Gravity/Dense%20Data%20Center%20Locator/docs/phase1/implementation_bootstrap.md)

## First Coding Objective

The first implementation window should deliver:

1. local environment bootstrap,
2. baseline database migrations,
3. source catalog and scoring reference tables,
4. API health service,
5. orchestration skeleton,
6. pytest and CI smoke coverage.
