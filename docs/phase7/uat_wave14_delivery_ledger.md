# UAT Wave 14 Delivery Ledger And Escalation

## Scope

Wave 14 adds a delivery-ledger export for release-archive follow-up so support
teams can review archive handoff status, notification state, and stale reply
escalations from one admin report.

## Delivered Capabilities

- delivery-ledger exports that summarize archive, export, handoff, and
  notification state in one response,
- stale notification-reply escalation windows based on the most recent
  `notification_sent` event,
- recommended-action guidance for escalations, missing exports, retry-due work,
  and pending external-handoff confirmation, and
- admin API coverage for ledger generation, validation, and RBAC enforcement.

## Validation Targets

- delivery-ledger exports should surface stale notification replies as
  escalated items once the configured reply window expires,
- acknowledged notification history should remain visible without being marked
  stale,
- actionable archives with no export should be reported with a
  `create_archive_export` recommendation, and
- admin-only access control should cover the ledger route.
