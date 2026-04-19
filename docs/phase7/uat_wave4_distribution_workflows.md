# Phase 7 UAT Wave 4 Distribution Workflows

## Scope

This slice extends the Phase 7 UAT handoff model with generated distribution
packets that can be attached to a handoff snapshot and delivered to named
stakeholders. It is focused on ready-to-send briefing content, recipient
tracking, and acknowledgement capture for launch-readiness review.

## Delivered Capabilities

- persistent UAT distribution packets generated from a handoff snapshot,
- generated subject lines and briefing bodies based on the stored sign-off
  payload and approval-readiness summary,
- packet readiness and lifecycle tracking across `draft`, `ready`,
  `distributed`, and `completed` states,
- recipient tracking with contact metadata, required-acknowledgement flags,
  delivery notes, acknowledgement notes, and actor attribution,
- admin APIs to create, list, retrieve, and advance distribution packets and
  recipients.

## API Surface

- `POST /admin/uat/handoff-snapshots/{snapshot_id}/distribution-packets`
- `GET /admin/uat/handoff-snapshots/{snapshot_id}/distribution-packets`
- `GET /admin/uat/distribution-packets/{packet_id}`
- `POST /admin/uat/distribution-packets/{packet_id}/recipients`
- `PATCH /admin/uat/distribution-packets/{packet_id}/recipients/{recipient_id}`

## Validation Targets

- packet creation should generate a draft packet when no recipients are seeded,
- adding recipients should make a packet ready to send,
- recipient delivery updates should move packets into `distributed`,
- acknowledgement completion should move packets into `completed`,
- duplicate packet names for the same snapshot should return `409`,
- operator access should be denied on admin-only distribution endpoints.
