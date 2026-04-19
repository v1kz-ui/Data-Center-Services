# Audit Exports

This slice adds an audit-ready evidence package endpoint for internal review,
support, and launch-readiness validation.

## Endpoint

- `GET /admin/audit/packages/runs/{run_id}`
  - admin-only
  - optional `parcel_id` query parameter to append parcel-level evidence

## Package Contents

- export metadata
  - package version
  - export scope
  - export timestamp
  - authenticated exporter identity
- run and batch state
  - run status, timestamps, failure reason, and metro
  - full batch context and sibling run state
- freshness evidence
  - current metro freshness evaluation
  - latest source snapshot identifiers, status, checksum, row counts, and
    rejection counts
- operator action history
  - retry, cancel, rerun, and related action metadata captured for the batch
- parcel evidence package when `parcel_id` is supplied
  - canonical parcel and zoning lineage context
  - parcel scoring detail with factor, bonus, confidence, and provenance
  - active source evidence rows for the parcel

## Intended Use

- validate one parcel end to end before launch,
- capture evidence for release gating and audit review,
- give support a single exportable package when triaging scoring disputes, and
- confirm source freshness and operator action history without direct database
  access.
