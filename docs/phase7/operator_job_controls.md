# Operator Job Controls

This slice adds the first operator-control surface for orchestration batches and
runs.

## Delivered Controls

- `POST /orchestration/runs/{run_id}/retry`
  Accepts an optional operator action payload and resets a failed run to
  `running` when the parent batch is not active.
- `POST /orchestration/runs/{run_id}/cancel`
  Accepts an optional operator action payload and marks a running run as failed
  with `MANUAL_CANCELLED` when the parent batch is not active.
- `POST /orchestration/batches/{batch_id}/rerun`
  Accepts an optional operator action payload and creates a replacement batch
  with the same metro pack when the source batch is no longer building.
- `GET /orchestration/actions`
  Returns the most recent operator action events and supports `batch_id`,
  `run_id`, and `limit` filters.

## Audit Model

- Operator actions are persisted in `operator_action_event`.
- Each event records the action type, target identifiers, actor, optional
  operator reason, JSON payload, and timestamps.
- Retry payloads preserve the previous failure reason.
- Cancel payloads preserve the terminal failure code.
- Batch rerun payloads preserve the replacement batch identifier.

## Guardrails

- Retry is blocked unless the run is currently `failed`.
- Cancel is blocked unless the run is currently `running`.
- Retry and cancel are both blocked for runs that belong to the active batch.
- Batch rerun is blocked while the batch remains `building`.

## Validation Intent

- service tests cover state transitions and persisted audit records,
- API tests cover successful actions, conflict paths, and filtered history, and
- metadata validation confirms the new audit table is registered with the
  managed schema set.
