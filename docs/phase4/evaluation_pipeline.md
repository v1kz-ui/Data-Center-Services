# Phase 4 Evaluation Pipeline

## Purpose

Phase 4 converts raw, metro-scoped parcels into audited evaluation outcomes before
any scoring logic is allowed to run.

## Runtime Flow

1. The operator selects an existing `score_run`.
2. The evaluation service resolves the run to its `metro_id` and all approved
   `county_fips` rows from `county_catalog`.
3. The service executes the Phase 3 freshness gate for the metro before any parcel
   evaluation begins.
4. If freshness fails, the run is marked `failed`, prior evaluation artifacts for
   the run are cleared, and the failure reason is returned to the operator.
5. If freshness passes, the service clears any prior `parcel_evaluations` and
   `parcel_exclusion_events` for the run so the evaluation stage can be replayed
   deterministically.
6. The service loads all active `raw_parcels`, `raw_zoning`, and `source_evidence`
   rows for the metro and its counties.
7. Each parcel is evaluated exactly once and written to `parcel_evaluations`.

## Status Model

The evaluation runtime currently writes these Phase 4 statuses:

- `prefiltered_band`
- `prefiltered_size`
- `excluded`
- `pending_scoring`

The schema still reserves `pending_exclusion_check` and `scored` for later phases,
but Phase 4 resolves every parcel directly into a terminal pre-scoring outcome.

## Decision Order

Evaluation follows a consistent order so reruns stay deterministic:

1. Representative-point band check
2. Minimum acreage check
3. Zoning exclusions
4. Land-use exclusions
5. Evidence-driven exclusions
6. Promotion to `pending_scoring`

This ordering means a parcel that fails the band or acreage gate does not emit
exclusion events, while a parcel that reaches the exclusion stage may emit one or
more auditable rule hits.

## Exclusion Audit Trail

For every excluded parcel, the service writes:

- one `parcel_evaluations` row with status `excluded`,
- a human-readable `status_reason`, and
- one or more `parcel_exclusion_events` rows carrying the rule code, reason, and
  rule version used for the run.

This provides run-scoped auditability without depending on transient views.

## Operator Endpoints

Phase 4 exposes three admin routes:

- `GET /admin/runs/{run_id}/evaluation/scope`
- `POST /admin/runs/{run_id}/evaluation`
- `GET /admin/runs/{run_id}/evaluation`

These routes let operators inspect scope, execute the evaluation stage, and review
run-level counts before handing survivors to scoring.
