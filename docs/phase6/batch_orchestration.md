# Phase 6 Batch Orchestration

## Scope

This Phase 6 slice implements `P6-S01` batch orchestrator core behavior on top
of the Phase 4 evaluation runtime and Phase 5 scoring runtime.

## Implemented Behavior

### Batch creation

- `create_batch()` normalizes metro IDs, deduplicates the requested scope, and
  persists one `score_batch` plus one `score_run` per metro.
- Newly created runs are scheduled in `running` state with `started_at`
  captured for operator visibility.

### Batch aggregation

- `reconcile_batch()` recalculates `score_batch.status` and
  `score_batch.completed_metros` from current run states.
- A batch is:
  - `building` while any run remains in progress,
  - `failed` when any run has failed, and
  - `completed` only when all required metro runs are completed.
- `activation_ready` is exposed as an orchestration read-model flag when all
  runs are completed successfully.

### Run status inspection

- `get_run()` exposes run-level operator metadata:
  - `batch_id`,
  - `metro_id`,
  - `profile_name`,
  - `status`,
  - `failure_reason`,
  - `started_at`,
  - `completed_at`.

### Failure and recovery

- `retry_run()` reopens a failed run for operator-directed recovery.
- Retry is blocked when:
  - the run is not currently failed, or
  - the run belongs to an already active batch.
- A successful retry resets the run to `running`, clears prior failure metadata,
  refreshes `started_at`, and reconciles the parent batch back to `building`.

### Activation prechecks

- `get_activation_check()` validates whether a completed batch is safe to
  publish once activation endpoints arrive in later Phase 6 slices.
- Current checks cover:
  - batch status must be `completed`,
  - persisted run count must match `expected_metros`,
  - `completed_metros` must match the expected metro count,
  - every run must be `completed` with no lingering failure reason,
  - no `pending_exclusion_check` or `pending_scoring` parcel states may remain,
  - scored-parcel factor and bonus cardinality must still satisfy Phase 5
    invariants.

### Lifecycle wiring

- Evaluation failure now reconciles the parent batch to `failed`.
- Scoring completion now reconciles the parent batch to `completed` when the
  last metro finishes successfully.
- Scoring freshness failure now reconciles the parent batch to `failed`.

## API Surface

### Operator endpoints

- `POST /orchestration/batches`
- `GET /orchestration/batches`
- `GET /orchestration/batches/{batch_id}`
- `POST /orchestration/batches/{batch_id}/reconcile`
- `GET /orchestration/batches/{batch_id}/activation-check`
- `GET /orchestration/runs/{run_id}`
- `POST /orchestration/runs/{run_id}/retry`

These are operator-facing orchestration endpoints. They do not yet expose
activated-batch publication or user-facing parcel reads.

## Deferred To Later Phase 6 Sprints

- activation-precondition enforcement and previous-active-batch retention,
- active-batch pointer logic,
- user-facing parcel search and parcel detail APIs,
- export/reporting endpoints, and
- cross-metro read-model publication.
