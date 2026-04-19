# UAT Wave 10 Follow-Up Operations

## Scope

Wave 10 turns archive follow-up into an operational control surface by adding
 a dashboard for release-archive attention items plus bulk retention-review
 handling for admin remediation workflows.

## Delivered Capabilities

- follow-up dashboard for overdue reviews, due-soon reviews, acknowledgement
  gaps, and export retry follow-up,
- attention-item rollups that surface archives needing action without forcing
  operators to inspect each archive one by one,
- bulk retention-review handling for review-completed and retention-extended
  actions across multiple archives, and
- per-archive bulk results so partial success and validation failures remain
  audit-friendly.

## Validation Targets

- the dashboard should count overdue, due-soon, and export follow-up archives
  consistently,
- bulk retention handling should remediate multiple archives in one request,
- bulk requests should reject unsupported re-export actions, and
- admin-only access control should cover the dashboard and bulk action routes.
