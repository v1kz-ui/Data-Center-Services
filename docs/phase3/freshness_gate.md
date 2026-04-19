# Freshness Gate

## Objective

The freshness gate determines whether a metro is safe to enter evaluation and scoring.

## Inputs

- metro ID,
- active source catalog entries covering that metro,
- latest source snapshots for each source and metro,
- source cadence rules from `refresh_cadence`,
- source criticality from `block_refresh`.

## Decision Rules

For each source in scope:

1. If no snapshot exists, the status is `MISSING_SOURCE`.
2. If the latest snapshot is not `success`, the status is `SOURCE_LOAD_ERROR`.
3. If the latest successful snapshot is older than the allowed cadence window, the status is
   `STALE_SOURCE`.
4. Otherwise the status is `FRESH`.

## Blocking Semantics

- Sources with `block_refresh = TRUE` must be fresh and successful.
- Sources with `block_refresh = FALSE` may be stale or failed without blocking the metro,
  but their warning state is still returned to operators.

## Operator Endpoints

- `GET /admin/sources/freshness/{metro_id}`
- `GET /admin/sources/health?metro_id=...`
- `POST /admin/sources/{source_id}/metros/{metro_id}/loads`

These endpoints provide the Phase 3 operator view required before Phase 4 evaluation work.
