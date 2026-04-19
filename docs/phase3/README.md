# Phase 3 Ingestion and Freshness Package

This folder contains the Phase 3 engineering package for source ingestion, quarantine
handling, freshness evaluation, and operator visibility.

## Included Artifacts

- `ingestion_framework.md`
- `source_interfaces.md`
- `authoritative_source_inventory.md`
- `freshness_gate.md`
- `migration_rehearsal.md`
- `phase3_exit_report.md`

## Engineering Outcome

The repo now includes shared ingestion services for parcel, zoning, and generic evidence
loads; quarantine and row-rejection persistence; operator-facing freshness and source-health
APIs; Phase 3 schema support; and automated tests that exercise the full build slice.
