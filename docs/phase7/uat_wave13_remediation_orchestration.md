# UAT Wave 13 Remediation Orchestration

## Scope

Wave 13 adds an admin-ready orchestration run for follow-up notification
dispatch so archive remediation can move from digest generation into a
repeatable preview-or-execute workflow.

## Delivered Capabilities

- preview or execution of recipient-grouped follow-up notification dispatch
  runs,
- automated `notification_sent` journaling against the latest export for each
  actionable archive,
- duplicate-send protection that skips archives already waiting on a
  notification acknowledgement,
- explicit skip reporting for actionable archives that do not yet have an
  export to journal against, and
- admin API coverage for dispatch execution, validation, and RBAC enforcement.

## Validation Targets

- dry-run dispatch should preview eligible archive notifications without
  mutating delivery-event history,
- live execution should record `notification_sent` events and transition digest
  state to notification-acknowledgement pending,
- reruns should skip archives already awaiting acknowledgement instead of
  duplicating send events, and
- admin-only access control should cover the orchestration endpoint.
