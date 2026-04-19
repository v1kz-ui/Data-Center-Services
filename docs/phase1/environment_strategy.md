# Environment Strategy

## Environment Set

| Environment | Purpose | Data Expectations | Change Control |
| --- | --- | --- | --- |
| `local` | developer build and unit validation | synthetic or lightweight local data | developer controlled |
| `integration` | shared service integration and migration rehearsal | seeded reference data plus controlled fixtures | team controlled |
| `staging` | end-to-end verification and UAT prep | production-like masked or approved test data | release controlled |
| `production` | live operational use | approved authoritative data only | formal release gate required |

## Configuration Rules

- Application settings are sourced from environment variables and `.env` for local use.
- Secrets are never committed to the repo.
- Config shape is documented through `.env.example` and `configs/app.example.yaml`.
- Database connection details, active batch strategy, and runtime logging are environment-scoped.

## Runtime Topology

- API runtime for operator and read endpoints
- orchestration runtime for batch lifecycle control
- worker runtimes for ingestion, evaluation, and scoring
- PostgreSQL/PostGIS as the authoritative datastore

## Promotion Rules

1. Changes are developed and validated locally.
2. Shared integration validation covers migrations, API behavior, and batch orchestration.
3. Staging validates cross-service flow and release evidence.
4. Production promotion requires approved release governance and prior regression evidence.

## Operational Baseline

- Local uses Python `3.12+`.
- CI runs Python `3.12`.
- Production target remains Python `3.12+` until a dedicated Python `3.13` upgrade ADR is approved.

