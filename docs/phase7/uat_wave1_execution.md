# UAT Wave 1 Execution

## Scope

This slice adds persistent UAT cycle management so the Phase 7 operator,
security, audit, and observability surfaces can be rehearsed in a structured
environment instead of tracked manually.

## Delivered Capabilities

- create a named UAT cycle against the scripted scenario pack,
- materialize one execution row per UAT scenario at cycle creation time,
- record scenario outcomes with operator evidence and execution notes,
- track cycle progress across `planned`, `in_progress`, `completed`,
  `approved`, and `rework_required`,
- log UAT defects against a cycle with scenario linkage, ownership, and
  external references,
- update defect status and resolution notes so approval blockers can be
  cleared within the API, and
- finalize a cycle only when all scenarios are terminal and no open
  high-severity defects remain.

## API Surface

- `POST /admin/uat/cycles`
- `GET /admin/uat/cycles`
- `GET /admin/uat/cycles/{cycle_id}`
- `POST /admin/uat/cycles/{cycle_id}/scenarios/{scenario_id}/results`
- `POST /admin/uat/cycles/{cycle_id}/defects`
- `PATCH /admin/uat/cycles/{cycle_id}/defects/{defect_id}`
- `POST /admin/uat/cycles/{cycle_id}/finalize`

## Operational Notes

- UAT cycle creation uses the configured scenario pack and fails fast if the
  file is missing.
- Approval is intentionally blocked while any `critical` or `high` defect
  remains `open`.
- Approved cycles become immutable so evidence, scenario outcomes, and defect
  logs remain audit-stable after sign-off.

## Validation Targets

- prove cycle creation expands the scenario pack into executable rows,
- prove execution-result updates move the cycle into progress,
- prove high-severity defects block approval until resolved, and
- prove approved cycles reject additional mutations.
