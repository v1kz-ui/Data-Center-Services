# Phase 5 Score Explanations

## Run-Scoped Detail Output

Phase 5 now exposes a run-scoped parcel explanation view for operator and QA use:

- `GET /admin/runs/{run_id}/scoring/parcels/{parcel_id}`

This endpoint returns the parcel's:

- overall scoring status,
- `viability_score`,
- `confidence_score`,
- ordered factor-detail rows,
- ordered bonus-detail rows, and
- evidence-quality counts derived from recorded provenance.

## Explanation Shape

Each factor detail includes:

- `factor_id`
- `points_awarded`
- `rationale`
- the recorded provenance inputs used to explain the factor decision

Each bonus detail includes:

- `bonus_id`
- `applied`
- `points_awarded`
- `rationale`

## Why This Matters

This gives Phase 5 a deterministic explanation surface before the later active-batch
parcel-detail API is introduced in Phase 6. Operators can now validate that a score
matches its factor math, bonus math, and provenance inputs without querying raw
tables directly.
