# Phase 6 Failure and Recovery

## Scope

This Phase 6 slice implements `P6-S02` foundations for failed-run recovery and
activation-precondition checks.

## Implemented Behavior

### Failed-run retry

- Operators can retry a failed run through the orchestration API.
- Retry currently performs an in-place recovery by:
  - setting the run back to `running`,
  - clearing `failure_reason`,
  - clearing `completed_at`,
  - refreshing `started_at`,
  - clearing `profile_name` so a future scoring pass resolves profile state
    again if needed.
- Parent batch state is recalculated immediately so a previously failed batch
  returns to `building` while the retry is in progress.

### Activation-precondition report

- The activation-check report is a read-only validation surface.
- It does not activate the batch.
- It returns `activation_ready = true` only when no validation issues are
  present.

### Current validation rules

- batch status is `completed`,
- persisted run count matches `expected_metros`,
- `completed_metros` matches the expected metro count,
- all runs are `completed`,
- completed runs have no lingering `failure_reason`,
- no pending parcel states remain,
- factor cardinality matches `10 x scored parcel count`,
- bonus cardinality matches `5 x scored parcel count`.

## API Surface

- `POST /orchestration/runs/{run_id}/retry`
- `GET /orchestration/batches/{batch_id}/activation-check`

## Deferred Work

- activation execution and audit history,
- rollback to the previously active batch,
- operator notifications on failed activation,
- integration of activation checks into final publication flow.
