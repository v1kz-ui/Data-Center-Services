# Phase 7 Monitoring Dashboard

## Scope

This Phase 7 slice implements the `P7-S01` admin monitoring dashboard backend
surface.

## Implemented Behavior

### Monitoring overview API

- `GET /admin/monitoring/overview`
- Optional query parameters:
  - `metro_id`
  - `recent_failed_limit`

### Returned dashboard domains

- source health snapshots,
- metro freshness report when `metro_id` is supplied,
- batch status counts,
- run status counts,
- latest batch summary,
- recent failed runs,
- derived operator alerts.

## Current alert signals

- failed latest source snapshot,
- quarantined latest source snapshot,
- freshness failure for required or optional sources,
- latest batch failed,
- failed metro runs.

## Intended Operator Use

This slice is designed to support a future admin dashboard without locking the
team into a specific UI framework yet. The API gives operators one summary view
for:

- stale or broken source conditions,
- current batch progression,
- metro run failures,
- recent system instability that may require retry or recovery actions.

## Deferred Work

- dashboard frontend implementation,
- structured alert routing and notifications,
- trace/log deep links,
- RBAC protection for admin-only operations,
- audit capture of dashboard actions.
