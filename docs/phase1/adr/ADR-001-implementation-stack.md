# ADR-001: Default Implementation Stack

## Status

Accepted

## Decision

Use Python 3.12+, FastAPI, PostgreSQL 16 with PostGIS, SQLAlchemy 2, Alembic, pytest, Ruff, and mypy as the default implementation stack for the KIO Site Finder clean rewrite.

## Rationale

- strong fit for data-heavy and geospatial workflows,
- strong fit for API plus worker service patterns,
- migration-first database control,
- straightforward testing of scoring and evaluation logic,
- simple local bootstrap compared with heavier distributed stacks.

## Consequences

- agents should build Python-first service packages,
- database control is migration-driven,
- geospatial logic should prefer PostGIS-backed execution where possible.
