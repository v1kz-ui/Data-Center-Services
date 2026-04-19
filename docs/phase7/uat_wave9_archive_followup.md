# UAT Wave 9 Export Handoff and Retention Remediation

## Scope

Wave 9 turns release-archive operations into a follow-up workflow by adding
export handoff progression plus retention-review actions that can reschedule
review dates or request downstream re-export work.

## Delivered Capabilities

- admin export lifecycle updates for delivered, acknowledged, follow-up
  required, and re-export scheduled states,
- delivery-confirmation metadata with who acknowledged the handoff and when,
- retry scheduling metadata for downstream export follow-up,
- persisted retention-action records for review completion, retention
  extensions, and re-export requests, and
- admin API routes to create and list retention actions per archive.

## Validation Targets

- export lifecycle updates should persist delivery confirmation and retry
  scheduling metadata,
- re-export scheduling should require a future retry timestamp,
- retention actions should reject exports that belong to a different archive,
  and
- retention remediation should move overdue archives out of the default review
  queue when a future review date is recorded.
