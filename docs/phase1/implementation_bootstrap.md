# Implementation Bootstrap Guide

## 1. Default Technical Decision

The default implementation stack for this program is:

- Python 3.12+
- FastAPI
- PostgreSQL 16 + PostGIS
- SQLAlchemy 2
- Alembic
- pytest
- Ruff
- mypy

This stack is chosen because it fits:

- geospatial evaluation workflows,
- data-heavy ingestion and scoring services,
- API-first admin and search interfaces,
- migration-driven database control,
- strong testability for batch and rule-driven logic.

## 2. First Build Sequence

1. Create the application settings module and environment loader.
2. Create the shared database engine/session module.
3. Create baseline Alembic configuration and the first migration package.
4. Create a minimal FastAPI app with `/health` and `/version` endpoints.
5. Create orchestrator, ingestion, evaluation, and scoring worker package skeletons.
6. Add pytest smoke tests for importability, settings, and API health.
7. Add the initial source catalog and factor/bonus catalog migrations.

## 3. Minimum Code Packages To Create

- `apps/api/src/app`
- `apps/api/src/app/api`
- `apps/api/src/app/core`
- `apps/api/src/app/db`
- `workers/orchestrator/src/orchestrator`
- `workers/ingestion/src/ingestion`
- `workers/evaluation/src/evaluation`
- `workers/scoring/src/scoring`

## 4. Minimum Database Deliverables

- baseline schema version table
- `source_catalog`
- `source_interface`
- `scoring_profile`
- `scoring_profile_factor`
- `factor_catalog`
- `bonus_catalog`
- `score_batch`
- `score_run`

## 5. Minimum Test Deliverables

- API health smoke test
- settings load smoke test
- migration boot smoke test
- schema object existence checks
- factor-budget sum validation test

## 6. Exit Condition For Bootstrap

Bootstrap is complete when a new agent can clone the repo, create the local environment, apply migrations, start the API, and run smoke tests without having to invent structure or conventions.
