# Phase 4 Parcel Evaluation Package

This folder contains the Phase 4 engineering package for metro-scoped parcel
evaluation, exclusion auditing, replay safety, and operator visibility.

## Included Artifacts

- `evaluation_pipeline.md`
- `rerun_and_reconciliation.md`
- `phase4_exit_report.md`

## Engineering Outcome

The repo now includes a run-scoped evaluation runtime that:

- resolves a score run to its approved metro and counties,
- evaluates every active parcel in scope without relying on deprecated candidate views,
- applies representative-point and acreage prefilters,
- records auditable exclusion events for zoning, land-use, and source-evidence rules,
- supports idempotent reruns when scoring outputs do not yet exist, and
- exposes operator APIs for scope inspection, execution, and post-run summaries.

Phase 4 intentionally stops at `pending_scoring`; final scoring and completion
enforcement remain Phase 5 responsibilities.
