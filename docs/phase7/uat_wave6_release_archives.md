# Phase 7 UAT Wave 6 Release Archives

## Scope

This slice adds immutable release-archive bundles on top of the launch
reconciliation workflow. It packages the closeout recommendation, support
handoff context, and board-ready manifest payload into a sealed artifact that
can be referenced during release governance and support transition.

## Delivered Capabilities

- persisted release archives tied to a specific handoff snapshot,
- sealed manifest payloads that embed the launch closeout report and support
  handoff metadata,
- deterministic archive checksums for immutable board-ready evidence bundles,
- support handoff summary capture for operational ownership and runbook
  references,
- admin APIs to create, list, and retrieve release archives.

## API Surface

- `POST /admin/uat/handoff-snapshots/{snapshot_id}/release-archives`
- `GET /admin/uat/handoff-snapshots/{snapshot_id}/release-archives`
- `GET /admin/uat/release-archives/{archive_id}`

## Validation Targets

- archive creation should seal a manifest payload with a stable checksum,
- archives should preserve the reconciled launch outcome at seal time,
- duplicate archive names for the same snapshot should return `409`,
- operator access should be denied on admin-only archive routes.
