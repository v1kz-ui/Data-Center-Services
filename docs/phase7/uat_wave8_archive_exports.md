# UAT Wave 8 Retention Operations and Archive Exports

## Scope

Wave 8 turns release archives into an operational queue by adding retention
review visibility and persisted export records for downstream audit or support
handoff systems.

## Delivered Capabilities

- admin retention-review queue with overdue, due-soon, active, and superseded
  archive classification,
- review-window filtering that highlights archives approaching or past their
  retention review date,
- persisted archive export records for downstream systems such as audit vaults
  or support handoff repositories,
- checksum-backed export payloads that preserve the archive detail, evidence
  index, and destination metadata for re-export or audit review, and
- admin API routes to create, list, and inspect release-archive exports.

## Validation Targets

- retention queues should classify overdue and due-soon archives consistently,
- superseded archives should remain hidden by default and become visible when
  explicitly requested,
- export creation should persist a stable payload, checksum, and destination
  metadata, and
- duplicate export names for the same archive should be rejected with a
  conflict response.
