# Phase 4 Rerun and Reconciliation

## Replay Model

Evaluation reruns are supported at the `score_run` level and are designed to be
idempotent.

Before replaying a run, the service:

- blocks the replay if scoring outputs already exist for the run,
- resets failed runs back to `running` when replay is explicitly allowed,
- deletes prior `parcel_exclusion_events` rows for the run, and
- deletes prior `parcel_evaluations` rows for the run.

This keeps Phase 4 replay safe while avoiding duplicate evaluation or exclusion
artifacts.

## Why Replay Can Be Blocked

Replay is rejected when `score_factor_detail` or `score_bonus_detail` rows already
exist for the run. Once scoring has consumed evaluation outcomes, replaying Phase 4
would desynchronize scoring evidence from evaluation truth.

## Reconciliation Checks

Operators should use the Phase 4 summary endpoint to confirm:

- the evaluated parcel count matches the scoped parcel count,
- no parcel was skipped,
- exclusion counts align with audit-event volume,
- reruns produce stable counts when the underlying source data has not changed, and
- failed freshness gates leave zero evaluation rows behind for the affected run.

## Hand-off to Phase 5

Phase 5 should treat `pending_scoring` as the only eligible input set for factor
and bonus evaluation. A run should not be marked complete until scoring has
consumed every `pending_scoring` parcel and no parcel remains in a pending state.

Phase 4 prepares that invariant by ensuring the pre-scoring population is explicit,
run-scoped, and replay-safe.
